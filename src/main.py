from __future__ import annotations

"""
Main orchestrator.

Railway #1 (FEE_WATCHER=true):
  - Fee watcher + signal server + Telegram bot
  - Menerima summary dari executor via HTTP /report

Railway #2-4 (FEE_WATCHER=false):
  - TIDAK perlu TELEGRAM_BOT_TOKEN
  - Poll sinyal dari Railway #1
  - Kirim summary ke Railway #1 via HTTP POST /report
"""

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, timezone

from .executor import AccountExecutor, DayResult
from .fee_watcher import FeeResult, FeeWatcher
from .signal_broker import SignalClient, SignalServer
from .storage import CredentialStore, FeeWatcherStore, ProgressStore

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
        # Telegram bot hanya di Railway #1
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

        log.info("Fee watcher mode aktif")
        await self._stop.wait()

    async def _on_executor_report(self, text: str) -> None:
        """Terima summary dari executor Railway, teruskan ke Telegram."""
        log.info("Report dari executor:\n%s", text)
        if self._panel:
            await self._panel.broadcast(text)

    async def _on_fee_trigger_watcher(self, result: FeeResult) -> None:
        if self._signal_server:
            self._signal_server.push_signal(
                fee_cc=result.fee_cc,
                fee_usd=result.fee_usd,
                fee_native=result.fee_native,
            )
        if self._panel:
            await self._panel.send_to_admin(
                f"Fee trigger: {result.fee_cc:.4f} CC — sinyal dikirim ke executor"
            )
        # jalankan juga akun lokal di Railway #1 jika ada
        async with self._execution_lock:
            today = datetime.now(timezone.utc).date().isoformat()
            accounts = CredentialStore.load_all()
            if accounts and not self._all_accounts_done_today(accounts, today):
                results = await self._run_accounts_parallel(fee_cc=result.fee_cc, today=today)
                summary = _build_summary(results, result.fee_cc)
                if self._panel:
                    await self._panel.broadcast(summary)

    async def _on_config_updated(self, what: str) -> None:
        log.info("Config diupdate: %s", what)
        if what == "fee_cookie" and self._fee_watcher_instance:
            cfg = FeeWatcherStore.load()
            self._fee_watcher_instance.config = cfg

    # ------------------------------------------------------------------
    # Executor mode (Railway #2-4)
    # ------------------------------------------------------------------

    async def _run_executor_mode(self) -> None:
        # Executor TIDAK butuh Telegram bot token
        watcher_url = os.environ.get("FEE_WATCHER_URL", "")
        if not watcher_url:
            log.warning("FEE_WATCHER_URL tidak diset, executor tidak bisa terima sinyal")

        self._signal_client = SignalClient(watcher_url=watcher_url)
        log.info("Executor mode aktif, menunggu sinyal dari %s", watcher_url or "local file")

        while not self._stop.is_set():
            today = datetime.now(timezone.utc).date().isoformat()
            accounts = CredentialStore.load_all()

            if not accounts:
                log.info("Belum ada akun, tunggu 30 detik...")
                await asyncio.sleep(30)
                continue

            if self._all_accounts_done_today(accounts, today):
                log.info("Semua akun selesai tx hari ini, tunggu esok...")
                await self._sleep_until_tomorrow()
                continue

            log.info("Menunggu sinyal fee trigger...")
            trigger = await self._signal_client.wait_for_trigger(timeout=3600)
            if trigger is None:
                log.info("Timeout menunggu sinyal, coba lagi...")
                continue

            fee_cc = float(trigger.get("fee_cc", 0))
            log.info("Sinyal diterima! Fee: %.4f CC, eksekusi swap...", fee_cc)

            results = await self._run_accounts_parallel(fee_cc=fee_cc, today=today)
            summary = _build_summary(results, fee_cc)

            # Kirim summary ke Railway #1 untuk diteruskan ke Telegram
            await self._signal_client.send_report(summary)

        log.info("Executor mode berhenti")

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    async def _run_accounts_parallel(self, *, fee_cc: float, today: str) -> list[DayResult]:
        accounts = CredentialStore.load_all()
        pending = [a for a in accounts if not self._account_done_today(a.name, today)]
        if not pending:
            return []

        log.info("Eksekusi paralel %d akun | fee=%.4f CC | dry_run=%s", len(pending), fee_cc, self._dry_run)
        executors = [
            AccountExecutor(cred, fee_cc_at_trigger=fee_cc, dry_run=self._dry_run)
            for cred in pending
        ]
        return await asyncio.gather(*(ex.run_daily_cycle() for ex in executors))

    def _all_accounts_done_today(self, accounts, today: str) -> bool:
        return all(self._account_done_today(a.name, today) for a in accounts)

    def _account_done_today(self, account_name: str, today: str) -> bool:
        return ProgressStore.load(account_name, today).completed_tx >= DAILY_TX_LIMIT

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


def _build_summary(results: list[DayResult], fee_cc: float) -> str:
    today = datetime.now(timezone.utc).date().isoformat()
    lines = [f"Eksekusi selesai - {today}", f"Fee saat eksekusi: {fee_cc:.4f} CC", ""]
    total_tx = total_ok = total_fail = 0
    for r in results:
        lines.append(f"{r.account_name}: {r.success_tx} sukses / {r.failed_tx} gagal")
        for tx in r.tx_results:
            status = "OK" if tx.success else "GAGAL"
            err = f" | {tx.error[:40]}" if tx.error else ""
            lines.append(
                f"  TX{tx.tx_index+1}: {tx.sell_symbol}->{tx.buy_symbol}"
                f" | jual={tx.sell_amount} | dapat={tx.received_amount}"
                f" | fee={tx.fee_cc:.4f} CC | {status}{err}"
            )
        total_tx += r.total_tx
        total_ok += r.success_tx
        total_fail += r.failed_tx
        lines.append("")
    lines.append(f"Total: {total_tx} tx | sukses: {total_ok} | gagal: {total_fail}")
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
