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




class FeeTooHighPause(Exception):
    """Dilempar ketika fee naik di tengah eksekusi — main loop harus pause dan nunggu sinyal lagi."""
    def __init__(self, fee_cc: float | None = None):
        self.fee_cc = fee_cc
        super().__init__(f"Fee naik ke {fee_cc} CC, pause eksekusi")


class AccountExecutor:
    def __init__(
        self,
        credential: AccountCredential,
        *,
        cc_price: float = 0.0,
        fee_cc_at_trigger: float = 0.0,
        dry_run: bool = False,
        fee_gate_fn=None,   # async () -> (ok: bool, fee_cc: float|None)
    ) -> None:
        self.credential = credential
        self.cc_price = cc_price
        self.fee_cc_at_trigger = fee_cc_at_trigger
        self.dry_run = dry_run
        self.fee_gate_fn = fee_gate_fn   # None = tidak ada pengecekan fee antar pair
        self.log = logging.getLogger(f"cantex.executor.{credential.name}")
        self._today = datetime.now(timezone.utc).date().isoformat()
        self._sdk_module: dict | None = None

    async def run_daily_cycle(self) -> DayResult:
        result = DayResult(account_name=self.credential.name, date=self._today)
        progress = ProgressStore.load(self.credential.name, self._today)

        if progress.success_tx >= DAILY_TX_LIMIT:
            self.log.info(
                "%s sudah %d/6 tx sukses hari ini, skip",
                self.credential.name, progress.success_tx,
            )
            result.total_tx = progress.completed_tx
            result.success_tx = progress.success_tx
            return result

        self.log.info(
            "%s lanjut dari %d/6 sukses (%d total attempt)",
            self.credential.name, progress.success_tx, progress.completed_tx,
        )

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

        try:
            async with sdk:
                await sdk.authenticate(force=True)
                info = await sdk.get_account_info()
                instruments = await self._resolve_instruments(sdk)

                # success_count = jumlah TX sukses sejauh ini (persisten dari progress)
                # ARAH SWAP ditentukan dari success_count % 2:
                #   genap (0,2,4) → USDCx→cBTC
                #   ganjil (1,3,5) → cBTC→USDCx
                # Kalau TX gagal → success_count tidak naik → arah tetap sama → otomatis retry
                # Bot berhenti hanya setelah 6 TX SUKSES, bukan 6 attempt
                success_count = progress.success_tx
                attempt = 0   # counter attempt dalam sesi ini (untuk log)

                while success_count < DAILY_TX_LIMIT:
                    attempt += 1

                    # Fee gate: cek di awal setiap round-trip (success genap = belum punya cBTC)
                    is_roundtrip_start = (success_count % 2 == 0)
                    if is_roundtrip_start and self.fee_gate_fn is not None:
                        ok, current_fee = await self.fee_gate_fn()
                        if not ok:
                            self.log.info(
                                "sukses=%d/6, attempt=%d: fee naik (%.6f CC) — pause, tunggu sinyal berikutnya",
                                success_count, attempt, current_fee or 0,
                            )
                            raise FeeTooHighPause(current_fee)
                        if current_fee is not None:
                            self.fee_cc_at_trigger = current_fee

                    # Arah berdasarkan berapa TX yang sudah sukses
                    sell_sym, buy_sym = _swap_pair(success_count)
                    is_allin = success_count < ALLIN_TX_COUNT

                    self.log.info(
                        "sukses=%d/6 attempt=%d: %s→%s",
                        success_count, attempt, sell_sym, buy_sym,
                    )

                    sell_amount = await self._compute_amount(
                        sdk=sdk, info=info,
                        sell_symbol=sell_sym,
                        is_allin=is_allin,
                        instruments=instruments,
                    )
                    if sell_amount is None or sell_amount <= Decimal("0"):
                        self.log.warning(
                            "sukses=%d/6: saldo %s tidak cukup, skip attempt ini",
                            success_count, sell_sym,
                        )
                        tx_res = TxResult(
                            tx_index=len(progress.tx_log),
                            sell_symbol=sell_sym, buy_symbol=buy_sym,
                            sell_amount="0", received_amount="0",
                            fee_cc=self.fee_cc_at_trigger,
                            success=False, error="Saldo tidak cukup",
                            timestamp=datetime.now(timezone.utc).isoformat(),
                        )
                    else:
                        tx_res = await self._execute_swap(
                            sdk=sdk,
                            tx_index=len(progress.tx_log),
                            sell_sym=sell_sym, buy_sym=buy_sym,
                            sell_amount=sell_amount,
                            instruments=instruments,
                        )

                    # Hanya naikan success_count kalau TX berhasil
                    if tx_res.success:
                        success_count += 1
                        progress.success_tx = success_count

                    result.tx_results.append(tx_res)
                    result.success_tx  = success_count
                    result.failed_tx  += 0 if tx_res.success else 1
                    result.total_tx   += 1

                    progress.completed_tx += 1
                    progress.tx_log.append(_tx_to_log(tx_res))
                    ProgressStore.save(self.credential.name, progress)

                    if tx_res.success:
                        try:
                            info = await sdk.get_account_info()
                        except Exception:
                            pass

                    await asyncio.sleep(2)

        except FeeTooHighPause:
            raise   # biarkan naik ke main loop untuk ditangani
        except Exception as exc:
            self.log.error("%s error fatal: %s", self.credential.name, exc, exc_info=True)
            result.failed_tx += 1

        return result

    async def _build_sdk(self) -> Any | None:
        try:
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

            # FIX: timeout 60s (SDK default), 90s terlalu lama
            event = await sdk.swap_and_confirm(
                sell_amount=sell_amount,
                sell_instrument=sell_instr,
                buy_instrument=buy_instr,
                timeout=60.0,
            )
            received = str(getattr(event, "output_amount", "?"))
            event_id = str(getattr(event, "event_id", ""))
            # FIX: pakai nama aset dari event SDK, bukan hardcode buy_sym
            # (SDK bisa return "Amulet" atau nama lain tergantung pool)
            received_sym = str(getattr(getattr(event, "output_instrument", None), "id", buy_sym))
            self.log.info(
                "TX %d/%d sukses: dapat %s %s | event_id=%s",
                tx_index + 1, DAILY_TX_LIMIT, received, received_sym, event_id,
            )
            return TxResult(
                tx_index=tx_index, sell_symbol=sell_sym, buy_symbol=received_sym,
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
