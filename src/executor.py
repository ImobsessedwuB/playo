from __future__ import annotations

"""
Executor: menjalankan 6 tx per hari untuk pair USDCx <-> cBTC.

Jadwal tx per hari:
  TX 1-4: all-in seluruh saldo (sisakan gas fee CC)
  TX 5-6: fixed $2 per transaksi

Pair bolak-balik: USDCx -> cBTC -> USDCx -> cBTC -> USDCx -> cBTC
"""

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .storage import AccountCredential, DailyProgress, ProgressStore

log = logging.getLogger("cantex.executor")

DAILY_TX_LIMIT = 6
ALLIN_TX_COUNT = 4
FIXED_TX_COUNT = 2
FIXED_AMOUNT_USD = Decimal("2")

# Instrument IDs untuk USDCx dan cBTC di Cantex
USDCX_INSTRUMENT_ID = "USDCx"
USDCX_INSTRUMENT_ADMIN = "decentralized-usdc-interchain-rep::12208115f1e168dd7e792320be9c4ca720c751a02a3053c7606e1c1cd3dad9bf60ef"
CBTC_INSTRUMENT_ID = "cBTC"
CBTC_INSTRUMENT_ADMIN = ""  # diisi dari API saat runtime

# Pasangan swap berurutan (index % 2 == 0 => USDCx->cBTC, ganjil => cBTC->USDCx)
def _swap_pair(tx_index: int) -> tuple[str, str]:
    if tx_index % 2 == 0:
        return ("USDCx", "cBTC")
    return ("cBTC", "USDCx")


@dataclass
class TxResult:
    tx_index: int
    sell_symbol: str
    buy_symbol: str
    sell_amount: str
    received_amount: str
    fee_cc: float
    success: bool
    error: str | None = None
    event_id: str = ""
    timestamp: str = ""


@dataclass
class DayResult:
    account_name: str
    date: str
    tx_results: list[TxResult] = field(default_factory=list)
    total_tx: int = 0
    success_tx: int = 0
    failed_tx: int = 0

    @property
    def done(self) -> bool:
        return self.total_tx >= DAILY_TX_LIMIT


class AccountExecutor:
    """
    Menjalankan siklus swap harian untuk satu akun.
    Diinstansiasi per-account, dijalankan secara concurrent.
    """

    def __init__(
        self,
        credential: AccountCredential,
        *,
        cc_price: float = 0.0,
        fee_cc_at_trigger: float = 0.0,
        dry_run: bool = False,
    ) -> None:
        self.credential = credential
        self.cc_price = cc_price
        self.fee_cc_at_trigger = fee_cc_at_trigger
        self.dry_run = dry_run
        self.log = logging.getLogger(f"cantex.executor.{credential.name}")
        self._today = datetime.now(timezone.utc).date().isoformat()

    async def run_daily_cycle(self) -> DayResult:
        result = DayResult(account_name=self.credential.name, date=self._today)
        progress = ProgressStore.load(self.credential.name, self._today)

        if progress.completed_tx >= DAILY_TX_LIMIT:
            self.log.info(
                "%s sudah selesai %d tx hari ini, skip",
                self.credential.name, DAILY_TX_LIMIT,
            )
            result.total_tx = progress.completed_tx
            result.success_tx = progress.completed_tx
            return result

        start_index = progress.completed_tx
        self.log.info(
            "%s mulai dari tx %d/%d",
            self.credential.name, start_index + 1, DAILY_TX_LIMIT,
        )

        sdk = await self._build_sdk()
        if sdk is None:
            result.failed_tx = 1
            return result

        try:
            async with sdk:
                await sdk.authenticate(force=True)
                info = await sdk.get_account_info()
                instruments = await self._resolve_instruments(sdk)

                for tx_index in range(start_index, DAILY_TX_LIMIT):
                    sell_sym, buy_sym = _swap_pair(tx_index)
                    is_allin = tx_index < ALLIN_TX_COUNT

                    sell_amount = await self._compute_amount(
                        sdk=sdk,
                        info=info,
                        sell_symbol=sell_sym,
                        is_allin=is_allin,
                        instruments=instruments,
                    )
                    if sell_amount is None or sell_amount <= 0:
                        self.log.warning(
                            "TX %d/%d: saldo %s tidak cukup, skip",
                            tx_index + 1, DAILY_TX_LIMIT, sell_sym,
                        )
                        tx_res = TxResult(
                            tx_index=tx_index,
                            sell_symbol=sell_sym,
                            buy_symbol=buy_sym,
                            sell_amount="0",
                            received_amount="0",
                            fee_cc=self.fee_cc_at_trigger,
                            success=False,
                            error="Saldo tidak cukup",
                            timestamp=datetime.now(timezone.utc).isoformat(),
                        )
                        result.tx_results.append(tx_res)
                        result.failed_tx += 1
                        result.total_tx += 1
                        progress.completed_tx += 1
                        progress.tx_log.append(_tx_to_log(tx_res))
                        ProgressStore.save(self.credential.name, progress)
                        continue

                    tx_res = await self._execute_swap(
                        sdk=sdk,
                        tx_index=tx_index,
                        sell_sym=sell_sym,
                        buy_sym=buy_sym,
                        sell_amount=sell_amount,
                        instruments=instruments,
                    )
                    result.tx_results.append(tx_res)
                    if tx_res.success:
                        result.success_tx += 1
                    else:
                        result.failed_tx += 1
                    result.total_tx += 1

                    progress.completed_tx += 1
                    progress.tx_log.append(_tx_to_log(tx_res))
                    ProgressStore.save(self.credential.name, progress)

                    # refresh info setelah swap
                    try:
                        info = await sdk.get_account_info()
                    except Exception:
                        pass

                    # jeda kecil antar tx
                    if tx_index < DAILY_TX_LIMIT - 1:
                        await asyncio.sleep(2)

        except Exception as exc:
            self.log.error("%s error fatal: %s", self.credential.name, exc, exc_info=True)
            result.failed_tx += 1

        return result

    async def _build_sdk(self) -> Any | None:
        try:
            from cantex_sdk import (
                CantexSDK,
                OperatorKeySigner,
                IntentTradingKeySigner,
            )
            from .sdk_proxy import patch_sdk_proxy

            operator_signer = OperatorKeySigner(self.credential.operator_key)
            trading_signer = IntentTradingKeySigner(self.credential.trading_key)
            sdk = CantexSDK(
                operator_signer=operator_signer,
                intent_signer=trading_signer,
                base_url=os.environ.get("CANTEX_BASE_URL", "https://api.cantex.io"),
            )
            if self.credential.proxy:
                patch_sdk_proxy(sdk, self.credential.proxy)
            return sdk
        except ImportError:
            self.log.error("cantex_sdk tidak terinstall")
            return None
        except Exception as exc:
            self.log.error("Gagal build SDK: %s", exc)
            return None

    async def _resolve_instruments(self, sdk: Any) -> dict[str, Any]:
        """Ambil InstrumentId untuk USDCx dan cBTC dari API."""
        try:
            from cantex_sdk import InstrumentId
            pools_info = await sdk.get_pools_info()
            instruments: dict[str, Any] = {}
            for pool in pools_info.pools:
                for instr in (pool.base_instrument, pool.quote_instrument):
                    sym = getattr(instr, "symbol", None) or getattr(instr, "id", "")
                    if sym in ("USDCx", "cBTC", "CBTC"):
                        instruments[sym] = instr
            # fallback alias
            if "CBTC" in instruments and "cBTC" not in instruments:
                instruments["cBTC"] = instruments["CBTC"]
            if "cBTC" in instruments and "CBTC" not in instruments:
                instruments["CBTC"] = instruments["cBTC"]
            return instruments
        except Exception as exc:
            self.log.warning("Gagal resolve instruments: %s", exc)
            return {}

    async def _compute_amount(
        self,
        *,
        sdk: Any,
        info: Any,
        sell_symbol: str,
        is_allin: bool,
        instruments: dict,
    ) -> Decimal | None:
        """Hitung jumlah yang akan di-swap."""
        try:
            balances: dict[str, Decimal] = {}
            for token in info.balances:
                sym = getattr(token, "symbol", "") or getattr(token, "instrument", {}).get("symbol", "")
                amount = Decimal(str(getattr(token, "amount", "0")))
                balances[sym] = amount

            if sell_symbol not in balances:
                return None

            balance = balances[sell_symbol]

            if is_allin:
                # all-in: gunakan semua saldo, sisakan gas fee jika sell CC
                if sell_symbol == "CC":
                    cc_reserve = Decimal("5")  # reserve CC untuk gas
                    spendable = balance - cc_reserve
                    return spendable if spendable > 0 else None
                # untuk USDCx atau cBTC: gunakan semua
                return balance if balance > Decimal("0.000001") else None
            else:
                # fixed $2 USD
                if sell_symbol == "USDCx":
                    # USDCx ~1 USD
                    amount = FIXED_AMOUNT_USD
                elif sell_symbol == "cBTC" or sell_symbol == "CBTC":
                    # estimasi: $2 / harga cBTC, pakai cc_price sebagai proxy sementara
                    # idealnya dari price feed tapi sebagai fallback gunakan balance kecil
                    # jika cc_price tersedia & ada price feed cBTC gunakan itu
                    btc_price = await self._get_cbtc_price(sdk)
                    if btc_price and btc_price > 0:
                        amount = FIXED_AMOUNT_USD / Decimal(str(btc_price))
                    else:
                        amount = Decimal("0.00003")  # fallback ~$2 asumsi BTC ~66k
                else:
                    amount = FIXED_AMOUNT_USD
                # pastikan cukup saldo
                if amount > balance:
                    amount = balance
                return amount if amount > Decimal("0.000001") else None
        except Exception as exc:
            self.log.warning("Gagal hitung amount: %s", exc)
            return None

    async def _get_cbtc_price(self, sdk: Any) -> float | None:
        try:
            pools_info = await sdk.get_pools_info()
            for pool in pools_info.pools:
                syms = {
                    getattr(pool.base_instrument, "symbol", ""),
                    getattr(pool.quote_instrument, "symbol", ""),
                }
                if "CBTC" in syms or "cBTC" in syms:
                    price = getattr(pool, "price", None)
                    if price:
                        return float(price)
        except Exception:
            pass
        return None

    async def _execute_swap(
        self,
        *,
        sdk: Any,
        tx_index: int,
        sell_sym: str,
        buy_sym: str,
        sell_amount: Decimal,
        instruments: dict,
    ) -> TxResult:
        self.log.info(
            "TX %d/%d: %s -> %s | amount=%s | dry_run=%s",
            tx_index + 1, DAILY_TX_LIMIT, sell_sym, buy_sym,
            sell_amount, self.dry_run,
        )

        if self.dry_run:
            return TxResult(
                tx_index=tx_index,
                sell_symbol=sell_sym,
                buy_symbol=buy_sym,
                sell_amount=str(sell_amount),
                received_amount="0",
                fee_cc=self.fee_cc_at_trigger,
                success=True,
                event_id="DRY_RUN",
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

        try:
            sell_instr = instruments.get(sell_sym)
            buy_instr = instruments.get(buy_sym)
            if sell_instr is None or buy_instr is None:
                raise ValueError(f"Instrument tidak ditemukan: {sell_sym} atau {buy_sym}")

            event = await sdk.swap_and_confirm(
                sell_amount=sell_amount,
                sell_instrument=sell_instr,
                buy_instrument=buy_instr,
                timeout=90.0,
            )
            received = str(getattr(event, "output_amount", "?"))
            event_id = str(getattr(event, "event_id", ""))
            self.log.info(
                "TX %d/%d sukses: dapat %s %s | event_id=%s",
                tx_index + 1, DAILY_TX_LIMIT, received, buy_sym, event_id,
            )
            return TxResult(
                tx_index=tx_index,
                sell_symbol=sell_sym,
                buy_symbol=buy_sym,
                sell_amount=str(sell_amount),
                received_amount=received,
                fee_cc=self.fee_cc_at_trigger,
                success=True,
                event_id=event_id,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
        except Exception as exc:
            self.log.error(
                "TX %d/%d gagal: %s -> %s | %s",
                tx_index + 1, DAILY_TX_LIMIT, sell_sym, buy_sym, exc,
            )
            return TxResult(
                tx_index=tx_index,
                sell_symbol=sell_sym,
                buy_symbol=buy_sym,
                sell_amount=str(sell_amount),
                received_amount="0",
                fee_cc=self.fee_cc_at_trigger,
                success=False,
                error=str(exc),
                timestamp=datetime.now(timezone.utc).isoformat(),
            )


def _tx_to_log(tx: TxResult) -> dict:
    return {
        "tx_index": tx.tx_index,
        "pair": f"{tx.sell_symbol}->{tx.buy_symbol}",
        "sell_amount": tx.sell_amount,
        "received_amount": tx.received_amount,
        "fee_cc": tx.fee_cc,
        "success": tx.success,
        "error": tx.error,
        "event_id": tx.event_id,
        "timestamp": tx.timestamp,
    }
