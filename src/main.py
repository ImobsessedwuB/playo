from __future__ import annotations

"""
Main orchestrator.

Railway #1 (FEE_WATCHER=true):
  - Fee watcher + signal server + Telegram bot
  - Menerima summary dari executor via HTTP /report

Railway #2-4 (FEE_WATCHER=false):
  - TIDAK perlu TELEGRAM_BOT_TOKEN
  - Poll sinyal dari Railway #1
  - Mini HTTP server untuk menerima credentials/clear_volume
  - Kirim summary ke Railway #1 via HTTP POST /report

Env vars penting:
  FEE_WATCHER       : true (Railway 1) / false (Railway 2-4)
  EXECUTOR_URLS     : comma-separated URLs executor (hanya di Railway 1)
                      contoh: https://r2.up.railway.app,https://r3.up.railway.app
  FEE_WATCHER_URL   : URL Railway 1 (diset di Railway 2-4)
  TELEGRAM_BOT_TOKEN: hanya di Railway 1
"""

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, timezone

from .executor import AccountExecutor, DayResult, FeeTooHighPause
from .fee_watcher import FeeResult, FeeWatcher
from .signal_broker import ExecutorServer, SignalClient, SignalServer
from .storage import BotStateStore, CredentialStore, FeeWatcherStore, ProgressStore

log = logging.getLogger("cantex.main")

DAILY_TX_LIMIT = 6


def _configure_logging() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        stream=sys.stdout,
    )
    for noisy in ("telegram", "httpx", "httpcore", "apscheduler", "aiohttp"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


class CantexOrchestrator:
    def __init__(self) -> None:
        self._is_fee_watcher = os.environ.get("FEE_WATCHER", "false").lower() == "true"
        self._telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self._dry_run = os.environ.get("DRY_RUN", "false").lower() == "true"
        self._stop = asyncio.Event()
        self._panel = None
        self._fee_watcher_instance: FeeWatcher | None = None
        self._signal_server: SignalServer | None = None
        self._signal_client: SignalClient | None = None
        self._execution_lock = asyncio.Lock()
        self._executor_pool: dict = {}

    async def run(self) -> None:
        mode = "fee_watcher" if self._is_fee_watcher else "executor"
        log.info("Cantex bot starting | mode=%s | dry_run=%s", mode, self._dry_run)

        if self._is_fee_watcher:
            await self._run_fee_watcher_mode()
        else:
            await self._run_executor_mode()

    # ------------------------------------------------------------------
    # Fee watcher mode (Railway #1)
    # ------------------------------------------------------------------

    async def _run_fee_watcher_mode(self) -> None:
        if self._telegram_token:
            from .telegram_panel import TelegramPanel
            self._panel = TelegramPanel(
                self._telegram_token,
                on_config_updated=self._on_config_updated,
            )
            await self._panel.start()
            await self._panel.send_to_admin("Fee watcher aktif, monitoring gas fee...")
        else:
            log.warning("TELEGRAM_BOT_TOKEN tidak diset di Railway #1")

        port = int(os.environ.get("PORT", "8080"))
        self._signal_server = SignalServer(port=port)
        self._signal_server.set_report_callback(self._on_executor_report)
        await self._signal_server.start()

        cfg = FeeWatcherStore.load()
        self._fee_watcher_instance = FeeWatcher(cfg)
        self._fee_watcher_instance.add_trigger_callback(self._on_fee_trigger_watcher)
        await self._fee_watcher_instance.start()
        # Wire setelah FeeWatcher dibuat
        self._signal_server._fee_watcher_ref = self._fee_watcher_instance

        # Jadwalkan summary harian jam 07.00 WIB = 00.00 UTC
        asyncio.create_task(self._daily_summary_loop(), name="daily-summary")

        log.info("Fee watcher mode aktif")
        await self._stop.wait()

    async def _on_executor_report(self, text: str) -> None:
        log.info("Report dari executor (diabaikan, pakai daily summary): %.80s...", text)

    async def _on_fee_trigger_watcher(self, result: FeeResult) -> None:
        # Cek bot state SEBELUM push signal
        if not BotStateStore.is_running():
            log.info("Bot tidak aktif (Telegram belum START), skip signal dan eksekusi")
            return

        if self._signal_server:
            self._signal_server.push_signal(
                fee_cc=result.fee_cc,
                fee_usd=result.fee_usd,
                fee_native=result.fee_native,
            )

        async with self._execution_lock:
            today = datetime.now(timezone.utc).date().isoformat()
            accounts = CredentialStore.load_all()
            if accounts and not self._all_accounts_done_today(accounts, today):
                await self._run_accounts_parallel(fee_cc=result.fee_cc, today=today)

    async def _daily_summary_loop(self) -> None:
        """Kirim summary harian ke semua admin tiap jam 00.00 UTC (07.00 WIB)."""
        from datetime import timedelta
        while not self._stop.is_set():
            now = datetime.now(timezone.utc)
            # Hitung waktu tunggu ke 00.00 UTC berikutnya
            next_run = now.replace(hour=0, minute=0, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            wait_secs = (next_run - now).total_seconds()
            log.info("Daily summary dijadwalkan dalam %.0f detik (%s UTC)", wait_secs, next_run.isoformat())
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=wait_secs)
                break  # stop diminta
            except asyncio.TimeoutError:
                pass

            # Kirim summary untuk hari kemarin (sudah lewat midnight)
            yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
            try:
                text = _build_daily_summary(yesterday)
                if self._panel:
                    await self._panel.broadcast(text)
                log.info("Daily summary terkirim untuk %s", yesterday)
            except Exception as exc:
                log.warning("Gagal kirim daily summary: %s", exc)

    async def _on_config_updated(self, what: str) -> None:
        log.info("Config diupdate: %s", what)
        if what == "fee_cookie" and self._fee_watcher_instance:
            cfg = FeeWatcherStore.load()
            self._fee_watcher_instance.config = cfg
        elif what in ("start_bot", "credentials"):
            log.info("Trigger warmup pre-auth executor pool")
            asyncio.create_task(self._warmup_executors())
        elif what == "stop_bot":
            log.info("Bot execution DISABLED via Telegram")
        elif what == "delete_volume":
            log.info("Volume data cleared via Telegram")

    # ------------------------------------------------------------------
    # Executor mode (Railway #2-4)
    # ------------------------------------------------------------------

    async def _run_executor_mode(self) -> None:
        watcher_url = os.environ.get("FEE_WATCHER_URL", "")
        if not watcher_url:
            log.warning("FEE_WATCHER_URL tidak diset, executor tidak bisa terima sinyal")

        port = int(os.environ.get("PORT", "8080"))
        executor_server = ExecutorServer(port=port)
        await executor_server.start()

        self._signal_client = SignalClient(watcher_url=watcher_url)
        log.info("Executor mode aktif, menunggu sinyal dari %s", watcher_url or "local file")

        # Warmup pre-auth saat executor pertama start
        await self._warmup_executors()

        while not self._stop.is_set():
            today = datetime.now(timezone.utc).date().isoformat()
            accounts = CredentialStore.load_all()

            if not accounts:
                log.info("Belum ada akun, tunggu 30 detik...")
                await asyncio.sleep(30)
                # Re-warmup saat akun baru masuk
                await self._warmup_executors()
                continue

            if not BotStateStore.is_running():
                log.info("Bot belum distart (via Telegram), menunggu...")
                await asyncio.sleep(30)
                continue

            if self._all_accounts_done_today(accounts, today):
                log.info("Semua akun selesai tx hari ini, tunggu esok...")
                await self._sleep_until_tomorrow()
                # Re-warmup setelah reset hari baru
                await self._warmup_executors()
                continue

            log.info("Menunggu sinyal fee trigger...")
            trigger = await self._signal_client.wait_for_trigger(timeout=3600)
            if trigger is None:
                log.info("Timeout menunggu sinyal, coba lagi...")
                continue

            fee_cc = float(trigger.get("fee_cc", 0))
            log.info("Sinyal diterima! Fee: %.6f CC, eksekusi swap...", fee_cc)

            accounts = CredentialStore.load_all()
            pending = [a for a in accounts if not self._account_done_today(a.name, today)]
            if not pending:
                continue

            signal_client = self._signal_client

            # fee_gate_fn: TIDAK cek fee sebelum pair pertama (success_count=0).
            # Sinyal sudah jadi validasi bahwa fee rendah.
            # Hanya cek sebelum pair ke-2 (success=2) dan ke-3 (success=4).
            async def _fee_gate_remote(success_count: int) -> tuple[bool, float | None]:
                if success_count == 0:
                    return True, fee_cc   # pair pertama: percaya sinyal
                return await signal_client.check_fee_ok()

            log.info("Eksekusi paralel %d akun | fee=%.6f CC | dry_run=%s", len(pending), fee_cc, self._dry_run)

            # Gunakan executor pool (pre-auth), fallback buat baru
            executors = []
            for cred in pending:
                ex = self._executor_pool.get(cred.name)
                if ex is None:
                    ex = AccountExecutor(cred, dry_run=self._dry_run)
                    self._executor_pool[cred.name] = ex
                ex.fee_cc_at_trigger = fee_cc
                ex.fee_gate_fn = _fee_gate_remote
                executors.append(ex)

            tasks = [asyncio.create_task(ex.run_daily_cycle()) for ex in executors]
            for t in asyncio.as_completed(tasks):
                try:
                    await t
                except FeeTooHighPause:
                    log.info("Fee naik saat eksekusi, pause — tunggu sinyal berikutnya")
                    for other in tasks:
                        other.cancel()
                    break
                except Exception as exc:
                    log.error("Akun error: %s", exc)

        log.info("Executor mode berhenti")

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    async def _warmup_executors(self) -> None:
        """
        Pre-auth semua akun di background setelah bot start.
        Dipanggil sekali saat bot pertama kali start / setelah credentials diupdate.
        Parallel untuk semua akun sekaligus.
        """
        accounts = CredentialStore.load_all()
        if not accounts:
            return
        log.info("Warmup pre-auth untuk %d akun...", len(accounts))
        # Teardown executor lama kalau ada
        for ex in self._executor_pool.values():
            await ex.teardown()
        self._executor_pool.clear()

        # Build executor pool baru
        for cred in accounts:
            ex = AccountExecutor(cred, dry_run=self._dry_run)
            self._executor_pool[cred.name] = ex

        # Warmup parallel semua akun
        results = await asyncio.gather(
            *(ex.warmup() for ex in self._executor_pool.values()),
            return_exceptions=True,
        )
        ok = sum(1 for r in results if r is True)
        log.info("Warmup selesai: %d/%d akun siap", ok, len(accounts))

    async def _run_accounts_parallel(self, *, fee_cc: float, today: str) -> list[DayResult]:
        accounts = CredentialStore.load_all()
        pending = [a for a in accounts if not self._account_done_today(a.name, today)]
        if not pending:
            return []

        log.info("Eksekusi paralel %d akun | fee=%.6f CC | dry_run=%s", len(pending), fee_cc, self._dry_run)

        fee_watcher = self._fee_watcher_instance
        _signal_fee_cc = fee_cc

        async def _fee_gate(success_count: int) -> tuple[bool, float | None]:
            # Pair pertama: percaya fee di sinyal — jangan hit API lagi
            if success_count == 0:
                return True, _signal_fee_cc
            if fee_watcher is None:
                return True, None
            # Pair ke-2 dan ke-3: cek fee terkini via last cached (bukan API call baru)
            last = fee_watcher.get_last_fee()
            if last is None:
                return True, None
            cfg = FeeWatcherStore.load()
            return last.fee_cc <= cfg.max_fee_cc, last.fee_cc

        # Pakai executor dari pool (pre-auth), atau buat baru kalau tidak ada
        executors = []
        for cred in pending:
            ex = self._executor_pool.get(cred.name)
            if ex is None:
                ex = AccountExecutor(cred, dry_run=self._dry_run)
                self._executor_pool[cred.name] = ex
            ex.fee_cc_at_trigger = fee_cc
            ex.fee_gate_fn = _fee_gate
            executors.append(ex)

        tasks = [asyncio.create_task(ex.run_daily_cycle()) for ex in executors]
        results = []
        for t in asyncio.as_completed(tasks):
            try:
                results.append(await t)
            except FeeTooHighPause:
                log.info("Fee naik saat eksekusi, sisa TX di-pause sampai sinyal berikutnya")
                for other in tasks:
                    other.cancel()
                break
            except Exception as exc:
                log.error("Akun error: %s", exc)
        return results

    def _all_accounts_done_today(self, accounts, today: str) -> bool:
        return all(self._account_done_today(a.name, today) for a in accounts)

    def _account_done_today(self, account_name: str, today: str) -> bool:
        # Cek dari success_tx — akun selesai hanya kalau 6 TX SUKSES
        return ProgressStore.load(account_name, today).success_tx >= DAILY_TX_LIMIT

    async def _sleep_until_tomorrow(self) -> None:
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        tomorrow = now.replace(hour=0, minute=5, second=0, microsecond=0)
        if tomorrow <= now:
            tomorrow += timedelta(days=1)
        sleep_seconds = (tomorrow - now).total_seconds()
        log.info("Tidur %.0f detik hingga %s UTC", sleep_seconds, tomorrow.isoformat())
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=sleep_seconds)
        except asyncio.TimeoutError:
            pass

    def request_stop(self) -> None:
        self._stop.set()


def _build_daily_summary(today: str) -> str:
    """
    Rangkuman harian — dikirim tiap jam 07.00 WIB (00.00 UTC).
    Mengambil data dari ProgressStore lokal (Railway 1).
    """
    progress_map = ProgressStore.load_all_today(today)
    if not progress_map:
        return f"SUMMARY {today}\n\n  Tidak ada data transaksi hari ini."

    # Kumpulkan semua fee dari semua tx yang sukses
    all_fees: list[float] = []
    tx_dist: dict[int, list[str]] = {}  # jumlah_sukses_tx -> [nama akun]
    total_accounts = len(progress_map)
    total_success_tx = 0
    total_failed_tx  = 0

    for acc_name, prog in progress_map.items():
        success_count = sum(1 for t in prog.tx_log if t.get("success"))
        tx_dist.setdefault(success_count, []).append(acc_name)
        for t in prog.tx_log:
            fee = t.get("fee_cc")
            if fee is not None:
                try:
                    all_fees.append(float(fee))
                except (ValueError, TypeError):
                    pass
            total_success_tx += 1 if t.get("success") else 0
            total_failed_tx  += 0 if t.get("success") else 1

    lines = [f"DAILY SUMMARY — {today}\n"]

    # Distribusi tx per akun
    lines.append("  TX Distribution:")
    for cnt in sorted(tx_dist.keys(), reverse=True):
        accs = ", ".join(tx_dist[cnt])
        lines.append(f"    {cnt}/6 tx  :  {len(tx_dist[cnt])} akun  ({accs})")

    lines.append("")
    lines.append(f"  {'Total akun':<16}:  {total_accounts}")
    lines.append(f"  {'Total TX':<16}:  {total_success_tx} sukses / {total_failed_tx} gagal")

    # Fee stats
    if all_fees:
        avg_fee = sum(all_fees) / len(all_fees)
        min_fee = min(all_fees)
        max_fee = max(all_fees)
        lines.append("")
        lines.append("  Gas Fee (saat trigger):")
        lines.append(f"    {'Average':<12}:  {avg_fee:.6f} CC")
        lines.append(f"    {'Min':<12}:  {min_fee:.6f} CC")
        lines.append(f"    {'Max':<12}:  {max_fee:.6f} CC")
        lines.append(f"    {'Samples':<12}:  {len(all_fees)} data point")
    else:
        lines.append("\n  Tidak ada data fee.")

    return "\n".join(lines)


async def main() -> None:
    _configure_logging()
    orch = CantexOrchestrator()
    loop = asyncio.get_event_loop()

    def _handle_sig(*_):
        log.info("Signal diterima, menghentikan bot...")
        orch.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_sig)
        except NotImplementedError:
            signal.signal(sig, _handle_sig)

    await orch.run()


if __name__ == "__main__":
    asyncio.run(main())
