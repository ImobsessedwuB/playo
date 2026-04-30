from __future__ import annotations

"""
Executor: menjalankan 6 tx per hari untuk pair USDCx <-> cBTC.

Jadwal tx per hari:
  TX 1-4: all-in seluruh saldo (sisakan CC untuk gas fee)
  TX 5-6: fixed $2 per transaksi

Catatan SDK:
  cantex_sdk adalah package private Cantex, tidak tersedia di PyPI.
  Letakkan file _sdk.py dari repo referensi di root project,
  bot akan menemukannya secara otomatis saat runtime.
"""

import asyncio
import importlib.util
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from .storage import AccountCredential, ProgressStore

log = logging.getLogger("cantex.executor")

DAILY_TX_LIMIT = 6
ALLIN_TX_COUNT = 4
FIXED_AMOUNT_USD = Decimal("2")


def _import_cantex_sdk() -> dict | None:
    """
    Coba import cantex_sdk dari beberapa lokasi:
    1. Installed package (jika sudah install wheel-nya)
    2. cantex_sdk.py di root project
    3. _sdk.py di root project (nama asli dari repo referensi)
    """
    # 1. installed package
    try:
        import cantex_sdk as m
        return {
            "CantexSDK": m.CantexSDK,
            "OperatorKeySigner": m.OperatorKeySigner,
            "IntentTradingKeySigner": m.IntentTradingKeySigner,
            "CantexAPIError": m.CantexAPIError,
        }
    except ImportError:
        pass

    # 2. file lokal di root project
    root = Path(__file__).resolve().parents[1]
    for filename in ("cantex_sdk.py", "_sdk.py"):
        candidate = root / filename
        if candidate.exists():
            try:
                spec = importlib.util.spec_from_file_location("cantex_sdk", candidate)
                m = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(m)
                sys.modules["cantex_sdk"] = m
                log.info("cantex_sdk dimuat dari %s", candidate)
                return {
                    "CantexSDK": m.CantexSDK,
                    "OperatorKeySigner": m.OperatorKeySigner,
                    "IntentTradingKeySigner": m.IntentTradingKeySigner,
                    "CantexAPIError": getattr(m, "CantexAPIError", Exception),
                }
            except Exception as exc:
                log.warning("Gagal load %s: %s", candidate, exc)

    return None


def _swap_pair(tx_index: int) -> tuple[str, str]:
    """TX genap: USDCx->cBTC, TX ganjil: cBTC->USDCx."""
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


# Exceptions yang kemungkinan disebabkan proxy mati / timeout
_PROXY_ERRORS = (
    "timed out",
    "timeout",
    "connection",
    "proxy",
    "network",
    "unreachable",
    "connect",
)

def _is_proxy_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(kw in msg for kw in _PROXY_ERRORS)



# Exceptions yang kemungkinan disebabkan proxy mati / timeout
_PROXY_ERRORS = (
    "timed out", "timeout", "connection",
    "proxy", "network", "unreachable", "connect",
)

def _is_proxy_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(kw in msg for kw in _PROXY_ERRORS)

class AccountExecutor:
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
        self._sdk_module: dict | None = None
        self._proxy_list: list[str] = credential.get_proxy_list()
        self._proxy_index: int = 0
        self._proxy_list: list[str] = credential.get_proxy_list()
        self._proxy_index: int = 0  # indeks proxy aktif saat ini

    async def run_daily_cycle(self) -> DayResult:
        result = DayResult(account_name=self.credential.name, date=self._today)
        progress = ProgressStore.load(self.credential.name, self._today)

        if progress.completed_tx >= DAILY_TX_LIMIT:
            self.log.info("%s sudah selesai %d tx hari ini, skip", self.credential.name, DAILY_TX_LIMIT)
            result.total_tx = progress.completed_tx
            result.success_tx = progress.completed_tx
            return result

        start_index = progress.completed_tx
        self.log.info("%s mulai dari tx %d/%d", self.credential.name, start_index + 1, DAILY_TX_LIMIT)

        self._sdk_module = _import_cantex_sdk()
        if self._sdk_module is None:
            self.log.error(
                "cantex_sdk tidak ditemukan. "
                "Letakkan file '_sdk.py' dari repo referensi di root project dengan nama 'cantex_sdk.py'."
            )
            result.failed_tx = 1
            return result

        sdk = await self._build_sdk()
        if sdk is None:
            result.failed_tx = 1
            return result

        max_proxy_retries = max(1, len(self._proxy_list))
        for attempt in range(max_proxy_retries):
            sdk = await self._build_sdk()
            if sdk is None:
                result.failed_tx += 1
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
                        if sell_amount is None or sell_amount <= Decimal("0"):
                            self.log.warning("TX %d/%d: saldo %s tidak cukup, skip", tx_index + 1, DAILY_TX_LIMIT, sell_sym)
                            tx_res = TxResult(
                                tx_index=tx_index, sell_symbol=sell_sym, buy_symbol=buy_sym,
                                sell_amount="0", received_amount="0", fee_cc=self.fee_cc_at_trigger,
                                success=False, error="Saldo tidak cukup",
                                timestamp=datetime.now(timezone.utc).isoformat(),
                            )
                        else:
                            tx_res = await self._execute_swap(
                                sdk=sdk, tx_index=tx_index, sell_sym=sell_sym,
                                buy_sym=buy_sym, sell_amount=sell_amount, instruments=instruments,
                            )

                        result.tx_results.append(tx_res)
                        result.success_tx += 1 if tx_res.success else 0
                        result.failed_tx += 0 if tx_res.success else 1
                        result.total_tx += 1

                        progress.completed_tx += 1
                        progress.tx_log.append(_tx_to_log(tx_res))
                        ProgressStore.save(self.credential.name, progress)

                        if tx_res.success:
                            try:
                                info = await sdk.get_account_info()
                            except Exception:
                                pass

                        if tx_index < DAILY_TX_LIMIT - 1:
                            await asyncio.sleep(2)

                break  # sukses, keluar dari proxy retry loop

            except Exception as exc:
                is_proxy_err = _is_proxy_error(exc)
                if is_proxy_err and len(self._proxy_list) > 1 and attempt < max_proxy_retries - 1:
                    bad_proxy = self._proxy_list[self._proxy_index]
                    self._proxy_index = (self._proxy_index + 1) % len(self._proxy_list)
                    new_proxy = self._proxy_list[self._proxy_index]
                    self.log.warning(
                        "%s proxy error (attempt %d/%d): %s — rotasi ke proxy %s",
                        self.credential.name, attempt + 1, max_proxy_retries,
                        bad_proxy.split("@")[-1], new_proxy.split("@")[-1],
                    )
                    # Reset start_index dari progress yang tersimpan (tx yang sudah selesai tidak diulang)
                    progress = ProgressStore.load(self.credential.name, self._today)
                    start_index = progress.completed_tx
                    continue
                else:
                    self.log.error("%s error fatal: %s", self.credential.name, exc, exc_info=True)
                    result.failed_tx += 1
                    break

        return result

    async def _build_sdk(self) -> Any | None:
        try:
            from .sdk_proxy import patch_sdk_proxy
            CantexSDK = self._sdk_module["CantexSDK"]
            OperatorKeySigner = self._sdk_module["OperatorKeySigner"]
            IntentTradingKeySigner = self._sdk_module["IntentTradingKeySigner"]

            # FIX: gunakan from_hex() bukan konstruktor langsung
            # Konstruktor menerima object key, bukan hex string
            operator_signer = OperatorKeySigner.from_hex(self.credential.operator_key)
            trading_signer = IntentTradingKeySigner.from_hex(self.credential.trading_key)

            sdk = CantexSDK(
                operator_signer=operator_signer,
                intent_signer=trading_signer,
                base_url=os.environ.get("CANTEX_BASE_URL", "https://api.cantex.io"),
            )
            # Gunakan proxy aktif dari daftar rotasi
            active_proxy = (
                self._proxy_list[self._proxy_index]
                if self._proxy_list else self.credential.proxy
            )
            if active_proxy:
                self.log.info(
                    "Menggunakan proxy: %s (index %d/%d)",
                    active_proxy.split("@")[-1],
                    self._proxy_index + 1,
                    max(1, len(self._proxy_list)),
                )
                patch_sdk_proxy(sdk, active_proxy)
            return sdk
        except Exception as exc:
            self.log.error("Gagal build SDK: %s", exc, exc_info=True)
            return None

    async def _resolve_instruments(self, sdk: Any) -> dict[str, Any]:
        """
        Resolve instrument objects dari pools info.
        SDK: get_pool_info() (bukan get_pools_info())
        Pool fields: token_a, token_b (InstrumentId dengan .id dan .admin)
        """
        try:
            # FIX: nama method yang benar adalah get_pool_info(), bukan get_pools_info()
            pools_info = await sdk.get_pool_info()
            instruments: dict[str, Any] = {}
            for pool in pools_info.pools:
                # FIX: field yang benar adalah token_a/token_b, bukan base_instrument/quote_instrument
                for instr in (pool.token_a, pool.token_b):
                    # FIX: InstrumentId punya .id dan .admin, tidak punya .symbol
                    sym = instr.id
                    if sym in ("USDCx", "cBTC", "CBTC"):
                        instruments[sym] = instr
            # normalisasi alias cBTC/CBTC
            if "CBTC" in instruments and "cBTC" not in instruments:
                instruments["cBTC"] = instruments["CBTC"]
            if "cBTC" in instruments and "CBTC" not in instruments:
                instruments["CBTC"] = instruments["cBTC"]
            self.log.info("Instruments resolved: %s", list(instruments.keys()))
            return instruments
        except Exception as exc:
            self.log.warning("Gagal resolve instruments: %s", exc, exc_info=True)
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
        try:
            balances: dict[str, Decimal] = {}

            # FIX: AccountInfo menggunakan .tokens bukan .balances
            # TokenBalance fields: .instrument_symbol, .unlocked_amount
            for token in info.tokens:
                sym = token.instrument_symbol  # FIX: bukan getattr(token, "symbol")
                amount = token.unlocked_amount  # FIX: bukan getattr(token, "amount", "0")
                if sym:
                    balances[sym] = amount

            self.log.debug("Balances: %s", {k: str(v) for k, v in balances.items()})
            balance = balances.get(sell_symbol, Decimal("0"))

            if is_allin:
                if sell_symbol == "CC":
                    spendable = balance - Decimal("5")  # reserve gas
                    return spendable if spendable > Decimal("0.000001") else None
                return balance if balance > Decimal("0.000001") else None
            else:
                if sell_symbol == "USDCx":
                    amount = FIXED_AMOUNT_USD
                elif sell_symbol in ("cBTC", "CBTC"):
                    btc_price = await self._get_cbtc_price(sdk, instruments)
                    amount = (FIXED_AMOUNT_USD / Decimal(str(btc_price))) if btc_price else Decimal("0.00003")
                else:
                    amount = FIXED_AMOUNT_USD
                amount = min(amount, balance)
                return amount if amount > Decimal("0.000001") else None
        except Exception as exc:
            self.log.warning("Gagal hitung amount: %s", exc, exc_info=True)
            return None

    async def _get_cbtc_price(self, sdk: Any, instruments: dict) -> float | None:
        """
        Ambil harga cBTC menggunakan swap quote API.
        Pool tidak punya field .price, jadi kita pakai get_swap_quote().
        """
        try:
            usdc_instr = instruments.get("USDCx")
            cbtc_instr = instruments.get("cBTC") or instruments.get("CBTC")
            if usdc_instr is None or cbtc_instr is None:
                self.log.warning("Instrument USDCx atau cBTC tidak ditemukan untuk price lookup")
                return None

            # FIX: Pool tidak punya .price — gunakan swap quote 1 USDCx->cBTC
            quote = await sdk.get_swap_quote(
                sell_amount=Decimal("1"),
                sell_instrument=usdc_instr,
                buy_instrument=cbtc_instr,
            )
            # 1 USDCx menghasilkan X cBTC → harga cBTC = 1/X USD
            cbtc_received = float(quote.returned.amount)
            if cbtc_received > 0:
                price = 1.0 / cbtc_received
                self.log.debug("cBTC price via quote: $%.2f", price)
                return price
        except Exception as exc:
            self.log.warning("Gagal get cBTC price: %s", exc)
        return None

    async def _execute_swap(self, *, sdk, tx_index, sell_sym, buy_sym, sell_amount, instruments) -> TxResult:
        self.log.info(
            "TX %d/%d: %s -> %s | amount=%s | dry_run=%s",
            tx_index + 1, DAILY_TX_LIMIT, sell_sym, buy_sym, sell_amount, self.dry_run,
        )

        if self.dry_run:
            return TxResult(
                tx_index=tx_index, sell_symbol=sell_sym, buy_symbol=buy_sym,
                sell_amount=str(sell_amount), received_amount="0",
                fee_cc=self.fee_cc_at_trigger, success=True,
                event_id="DRY_RUN", timestamp=datetime.now(timezone.utc).isoformat(),
            )

        try:
            sell_instr = instruments.get(sell_sym)
            buy_instr = instruments.get(buy_sym)
            if sell_instr is None or buy_instr is None:
                raise ValueError(
                    f"Instrument tidak ditemukan: {sell_sym} atau {buy_sym}. "
                    f"Tersedia: {list(instruments.keys())}"
                )

            event = await sdk.swap_and_confirm(
                sell_amount=sell_amount,
                sell_instrument=sell_instr,
                buy_instrument=buy_instr,
                timeout=90.0,
            )
            # FIX: SwapExecutedEvent menggunakan .output_amount (Decimal), bukan .output_amount string
            received = str(getattr(event, "output_amount", "?"))
            event_id = str(getattr(event, "event_id", ""))
            self.log.info(
                "TX %d/%d sukses: dapat %s %s | event_id=%s",
                tx_index + 1, DAILY_TX_LIMIT, received, buy_sym, event_id,
            )
            return TxResult(
                tx_index=tx_index, sell_symbol=sell_sym, buy_symbol=buy_sym,
                sell_amount=str(sell_amount), received_amount=received,
                fee_cc=self.fee_cc_at_trigger, success=True,
                event_id=event_id, timestamp=datetime.now(timezone.utc).isoformat(),
            )
        except Exception as exc:
            self.log.error(
                "TX %d/%d gagal: %s -> %s | %s",
                tx_index + 1, DAILY_TX_LIMIT, sell_sym, buy_sym, exc,
            )
            return TxResult(
                tx_index=tx_index, sell_symbol=sell_sym, buy_symbol=buy_sym,
                sell_amount=str(sell_amount), received_amount="0",
                fee_cc=self.fee_cc_at_trigger, success=False,
                error=str(exc), timestamp=datetime.now(timezone.utc).isoformat(),
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
