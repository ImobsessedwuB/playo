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
        return self.success_tx >= DAILY_TX_LIMIT




class FeeTooHighPause(Exception):
    """Dilempar ketika fee naik di tengah eksekusi — main loop harus pause dan nunggu sinyal lagi."""
    def __init__(self, fee_cc: float | None = None):
        self.fee_cc = fee_cc
        super().__init__(f"Fee naik ke {fee_cc} CC, pause eksekusi")


# Interval keepalive ping (detik) — cek apakah token masih valid
_KEEPALIVE_INTERVAL = 180   # 3 menit

# Jeda antar TX dalam satu sesi eksekusi
_TX_SLEEP = 0.5             # Optimasi #3: turun dari 2s ke 0.5s


class AccountExecutor:
    """
    Satu instance per akun, bisa dipakai ulang lintas sinyal.
    Optimasi:
      #1 Pre-auth   — SDK di-auth saat warmup, keepalive setiap 3 menit
      #2 Cache info — account_info di-cache, refresh background tiap 60 detik
      #3 Fast sleep — jeda antar TX dikurangi dari 2s ke 0.5s
    """

    def __init__(
        self,
        credential: AccountCredential,
        *,
        cc_price: float = 0.0,
        fee_cc_at_trigger: float = 0.0,
        dry_run: bool = False,
        fee_gate_fn=None,
    ) -> None:
        self.credential = credential
        self.cc_price = cc_price
        self.fee_cc_at_trigger = fee_cc_at_trigger
        self.dry_run = dry_run
        self.fee_gate_fn = fee_gate_fn
        self.log = logging.getLogger(f"cantex.executor.{credential.name}")
        self._today = datetime.now(timezone.utc).date().isoformat()
        self._sdk_module: dict | None = None

        # Pre-auth state
        self._sdk: Any | None = None              # SDK instance yang di-reuse
        self._sdk_ready: bool = False             # True = sudah auth dan siap pakai
        self._keepalive_task: asyncio.Task | None = None
        self._warmup_lock = asyncio.Lock()

        # Cached account info (#2)
        self._cached_info: Any | None = None
        self._cached_info_ts: float = 0.0
        self._info_refresh_task: asyncio.Task | None = None

        # Cached instruments (#4) — pool jarang berubah, resolve sekali saja
        self._cached_instruments: dict | None = None

    # ── Warmup (pre-auth) ─────────────────────────────────────────────────

    async def warmup(self) -> bool:
        """
        Pre-auth: buka SDK dan authenticate sebelum sinyal datang.
        Aman dipanggil paralel dari banyak akun sekaligus.
        Return True kalau berhasil.
        """
        async with self._warmup_lock:
            if self._sdk_ready:
                return True
            self._sdk_module = _import_cantex_sdk()
            if self._sdk_module is None:
                self.log.error("cantex_sdk tidak ditemukan")
                return False
            sdk = await self._build_sdk()
            if sdk is None:
                return False
            try:
                await sdk.__aenter__()
                await sdk.authenticate(force=True)
                self._sdk = sdk
                self._sdk_ready = True
                # Cache instruments saat warmup
                try:
                    self._cached_instruments = await self._resolve_instruments(sdk)
                except Exception as exc:
                    self.log.warning("Warmup: gagal cache instruments: %s", exc)
                self.log.info("Warmup selesai — SDK siap")
                # Mulai keepalive background task
                self._keepalive_task = asyncio.create_task(
                    self._keepalive_loop(), name=f"keepalive-{self.credential.name}"
                )
                # Mulai background info refresh
                self._info_refresh_task = asyncio.create_task(
                    self._info_refresh_loop(), name=f"info-refresh-{self.credential.name}"
                )
                return True
            except Exception as exc:
                self.log.warning("Warmup gagal: %s", exc)
                try:
                    await sdk.__aexit__(None, None, None)
                except Exception:
                    pass
                return False

    async def _keepalive_loop(self) -> None:
        """
        Background keepalive: ping /v1/account/info tiap _KEEPALIVE_INTERVAL detik.
        Kalau gagal → re-auth otomatis.
        Berjalan terus sampai teardown() dipanggil.
        """
        while self._sdk_ready and self._sdk is not None:
            await asyncio.sleep(_KEEPALIVE_INTERVAL)
            if not self._sdk_ready or self._sdk is None:
                break
            try:
                await self._sdk.get_account_info()
                self.log.debug("Keepalive OK")
            except Exception as exc:
                self.log.warning("Keepalive gagal (%s), re-auth...", exc)
                try:
                    await self._sdk.authenticate(force=True)
                    self.log.info("Re-auth berhasil")
                except Exception as exc2:
                    self.log.error("Re-auth gagal: %s", exc2)
                    self._sdk_ready = False

    async def _info_refresh_loop(self) -> None:
        """
        Background cache refresh: perbarui account_info tiap 60 detik.
        Sehingga saat sinyal datang, info sudah tersedia tanpa API call.
        """
        while self._sdk_ready and self._sdk is not None:
            await asyncio.sleep(60)
            if not self._sdk_ready or self._sdk is None:
                break
            try:
                self._cached_info = await self._sdk.get_account_info()
                self._cached_info_ts = __import__("time").monotonic()
                self.log.debug("Account info cache diperbarui")
            except Exception as exc:
                self.log.debug("Info cache refresh gagal: %s", exc)

    async def _get_account_info_cached(self, sdk: Any) -> Any:
        """
        Return cached account info kalau masih fresh (< 90 detik),
        otherwise fetch baru.
        """
        import time
        age = time.monotonic() - self._cached_info_ts
        if self._cached_info is not None and age < 90:
            self.log.debug("Pakai cached account info (age=%.0fs)", age)
            return self._cached_info
        info = await sdk.get_account_info()
        self._cached_info = info
        self._cached_info_ts = time.monotonic()
        return info

    async def teardown(self) -> None:
        """Tutup SDK dan stop semua background tasks."""
        self._sdk_ready = False
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
        if self._info_refresh_task and not self._info_refresh_task.done():
            self._info_refresh_task.cancel()
        if self._sdk is not None:
            try:
                await self._sdk.__aexit__(None, None, None)
            except Exception:
                pass
            self._sdk = None
        self.log.debug("SDK teardown selesai")

    async def run_daily_cycle(self) -> DayResult:
        """
        Jalankan sesi eksekusi untuk hari ini.
        Optimasi:
          #1 Pre-auth   — pakai self._sdk yang sudah warmup, tidak auth ulang
          #2 Info cache — account_info dari cache, bukan API call baru
          #3 Fast sleep — 0.5s antar TX, bukan 2s
          #4 Instrument cache — resolve instruments sekali, reuse
        """
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

        # Opt #1: pakai SDK yang sudah pre-auth, atau fallback build baru
        if self._sdk_ready and self._sdk is not None:
            sdk = self._sdk
            owns_context = False
            self.log.info("%s: pakai pre-auth SDK (skip auth delay)", self.credential.name)
        else:
            self._sdk_module = _import_cantex_sdk()
            if self._sdk_module is None:
                self.log.error("cantex_sdk tidak ditemukan.")
                result.failed_tx = 1
                return result
            sdk = await self._build_sdk()
            if sdk is None:
                result.failed_tx = 1
                return result
            owns_context = True

        try:
            if owns_context:
                await sdk.__aenter__()
                await sdk.authenticate(force=True)

            # Opt #2: pakai cached account info
            info = await self._get_account_info_cached(sdk)
            # Opt #4: pakai cached instruments, resolve ulang kalau belum ada
            if self._cached_instruments:
                instruments = self._cached_instruments
                self.log.debug("Pakai cached instruments")
            else:
                instruments = await self._resolve_instruments(sdk)
                self._cached_instruments = instruments

            success_count = progress.success_tx
            attempt = 0

            while success_count < DAILY_TX_LIMIT:
                attempt += 1

                # Fee gate: cek di awal setiap round-trip baru
                is_roundtrip_start = (success_count % 2 == 0)
                if is_roundtrip_start and self.fee_gate_fn is not None:
                    # Pass success_count agar fee gate bisa skip cek di pair pertama
                    ok, current_fee = await self.fee_gate_fn(success_count)
                    if not ok:
                        self.log.info(
                            "sukses=%d/6 attempt=%d: fee naik (%.6f CC) — pause",
                            success_count, attempt, current_fee or 0,
                        )
                        raise FeeTooHighPause(current_fee)
                    if current_fee is not None:
                        self.fee_cc_at_trigger = current_fee

                # Cek arah dari saldo aktual wallet (fix restart bot)
                sell_sym, buy_sym = await self._resolve_swap_direction(info, success_count)
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
                    # Saldo kosong = tidak ada yang bisa dilakukan sekarang.
                    # STOP loop — tidak boleh infinite retry.
                    # Tunggu sinyal berikutnya (fee mungkin sudah naik / saldo benar2 habis).
                    self.log.warning(
                        "sukses=%d/6: saldo %s kosong — pause, tunggu sinyal berikutnya",
                        success_count, sell_sym,
                    )
                    raise FeeTooHighPause(self.fee_cc_at_trigger)

                tx_res = await self._execute_swap(
                    sdk=sdk,
                    tx_index=success_count,        # 0-5, untuk display "TX 1/6"
                    sell_sym=sell_sym, buy_sym=buy_sym,
                    sell_amount=sell_amount,
                    instruments=instruments,
                )

                if tx_res.success:
                    success_count += 1
                    progress.success_tx = success_count
                    # Hanya simpan TX sukses ke persistent log
                    # tx_index = urutan sukses (0-5), bukan nomor attempt
                    progress.tx_log.append(_tx_to_log(tx_res))
                    # Update cache setelah TX sukses
                    try:
                        info = await sdk.get_account_info()
                        self._cached_info = info
                        self._cached_info_ts = __import__("time").monotonic()
                    except Exception:
                        pass
                else:
                    # TX gagal: hanya tambah counter, TIDAK masuk tx_log
                    progress.failed_attempts = getattr(progress, "failed_attempts", 0) + 1

                result.tx_results.append(tx_res)
                result.success_tx = success_count
                result.failed_tx += 0 if tx_res.success else 1
                result.total_tx += 1

                progress.completed_tx += 1
                ProgressStore.save(self.credential.name, progress)

                # Opt #3: 0.5s antar TX (turun dari 2s)
                await asyncio.sleep(_TX_SLEEP)

        except FeeTooHighPause:
            raise
        except Exception as exc:
            self.log.error("%s error fatal: %s", self.credential.name, exc, exc_info=True)
            result.failed_tx += 1
        finally:
            if owns_context and sdk is not None:
                try:
                    await sdk.__aexit__(None, None, None)
                except Exception:
                    pass

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

    async def _resolve_swap_direction(self, info: any, success_count: int) -> tuple[str, str]:
        """
        Tentukan arah swap dari saldo AKTUAL di wallet.
        - Kalau USDCx ada dan cBTC kosong → USDCx→cBTC
        - Kalau cBTC ada dan USDCx kosong  → cBTC→USDCx
        - Kalau keduanya ada               → ikut success_count (normal alternating)
        - Kalau keduanya kosong            → ikut success_count (akan trigger pause saat compute)
        Ini fix untuk restart bot: tidak perlu tebak-tebakan dari history.
        """
        # Threshold minimal yang cukup untuk actual trade
        # USDCx: minimal $1 (bisa mulai swap)
        # cBTC:  minimal 0.00001 BTC (~$1 at $100k) — jauh di atas dust level
        MIN_USDC = Decimal("1.0")
        MIN_CBTC = Decimal("0.00001")
        try:
            balances: dict[str, Decimal] = {}
            for token in info.tokens:
                sym = token.instrument_symbol
                if sym:
                    balances[sym] = token.unlocked_amount

            usdc_bal = balances.get("USDCx", Decimal("0"))
            cbtc_bal = balances.get("cBTC", balances.get("CBTC", Decimal("0")))

            has_usdc = usdc_bal >= MIN_USDC
            has_cbtc = cbtc_bal >= MIN_CBTC

            self.log.info(
                "Balance check: USDCx=%s (%s), cBTC=%s (%s)",
                usdc_bal, "ok" if has_usdc else "dust/kosong",
                cbtc_bal, "ok" if has_cbtc else "dust/kosong",
            )

            if has_cbtc and not has_usdc:
                self.log.info("Wallet pegang cBTC → arah: cBTC→USDCx")
                return ("cBTC", "USDCx")
            if has_usdc and not has_cbtc:
                self.log.info("Wallet pegang USDCx → arah: USDCx→cBTC")
                return ("USDCx", "cBTC")
            if has_usdc and has_cbtc:
                # Keduanya ada — ikut success_count
                self.log.info("Keduanya ada → ikut success_count %d", success_count)
        except Exception as exc:
            self.log.debug("Gagal resolve swap direction dari balance: %s", exc)

        # Fallback: ikut success_count seperti biasa
        return _swap_pair(success_count)

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
