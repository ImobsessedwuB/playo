from __future__ import annotations

"""
Main orchestrator.

Mode Railway:
  FEE_WATCHER=true  -> fee watcher + signal server + Telegram bot
  FEE_WATCHER=false -> executor (menunggu sinyal) + Telegram bot

Semua instance memiliki Telegram bot untuk monitoring.
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
from .telegram_panel import TelegramPanel

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
        self._panel: TelegramPanel | None = None
        self._fee_watcher_instance: FeeWatcher | None = None
        self._signal_server: SignalServer | None = None
        self._signal_client: SignalClient | None = None
        self._execution_lock = asyncio.Lock()
        self._last_trigger_ts: float = 0.0

    async def run(self) -> None:
        log.info(
            "Cantex bot starting | mode=%s | dry_run=%s",
            "fee_watcher" if self._is_fee_watcher else "executor",
            self._dry_run,
        )

        # Telegram panel (semua instance punya)
        if self._telegram_token:
            self._panel = TelegramPanel(
                self._telegram_token,
                on_config_updated=self._on_config_updated,
            )
            await self._panel.start()
            await self._panel.send_to_admin(
                f"Bot started | mode={'fee_watcher' if self._is_fee_watcher else 'executor'}"
            )
        else:
            log.warning("TELEGRAM_BOT_TOKEN tidak diset, Telegram panel nonaktif")

        if self._is_fee_watcher:
            await self._run_fee_watcher_mode()
        else:
            await self._run_executor_mode()

    async def _run_fee_watcher_mode(self) -> None:
        """
        Mode fee watcher:
        1. Jalankan signal server (HTTP)
        2. Jalankan fee watcher - trigger eksekusi lokal juga
        """
        port = int(os.environ.get("PORT", "8080"))
        self._signal_server = SignalServer(port=port)
        await self._signal_server.start()

        cfg = FeeWatcherStore.load()
        self._fee_watcher_instance = FeeWatcher(cfg)
        self._fee_watcher_instance.add_trigger_callback(self._on_fee_trigger_watcher)
        await self._fee_watcher_instance.start()

        log.info("Fee watcher mode aktif, polling fee setiap 60 detik")
        await self._stop.wait()

    async def _run_executor_mode(self) -> None:
        """
        Mode executor:
        1. Poll sinyal dari fee watcher
        2. Jika sinyal diterima, jalankan swap untuk semua akun secara paralel
        3. Ulangi keesokan harinya
        """
        watcher_url = os.environ.get("FEE_WATCHER_URL", "")
        self._signal_client = SignalClient(watcher_url=watcher_url)

        log.info("Executor mode aktif, menunggu sinyal fee trigger")

        while not self._stop.is_set():
            today = datetime.now(timezone.utc).date().isoformat()
            accounts = CredentialStore.load_all()

            if not accounts:
                log.info("Belum ada akun, tunggu 30 detik...")
                await asyncio.sleep(30)
                continue

            # cek apakah semua akun sudah selesai tx hari ini
            all_done = self._all_accounts_done_today(accounts, today)
            if all_done:
                log.info("Semua akun sudah selesai tx hari ini, tunggu hari berikutnya...")
                await self._sleep_until_tomorrow()
                continue

            # tunggu sinyal fee trigger
            log.info("Menunggu sinyal fee trigger dari fee watcher...")
            if self._panel:
                await self._panel.send_to_admin("Menunggu fee turun ke batas yang ditentukan...")

            signal = await self._signal_client.wait_for_trigger(timeout=3600)
            if signal is None:
                log.info("Timeout menunggu sinyal, coba lagi...")
                continue

            fee_cc = signal.get("fee_cc", 0)
            log.info("Sinyal diterima! Fee: %.4f CC, eksekusi swap...", fee_cc)
            await self._run_all_accounts_parallel(fee_cc=fee_cc, today=today)

        log.info("Executor mode berhenti")

    async def _on_fee_trigger_watcher(self, result: FeeResult) -> None:
        """Callback dari fee watcher saat fee turun ke batas."""
        # Kirim sinyal ke executor Railway lain via HTTP
        if self._signal_server:
            self._signal_server.push_signal(
                fee_cc=result.fee_cc,
                fee_usd=result.fee_usd,
                fee_native=result.fee_native,
            )

        # Juga jalankan eksekusi lokal (jika ada akun di Railway yang sama)
        async with self._execution_lock:
            today = datetime.now(timezone.utc).date().isoformat()
            accounts = CredentialStore.load_all()
            if accounts and not self._all_accounts_done_today(accounts, today):
                await self._run_all_accounts_parallel(fee_cc=result.fee_cc, today=today)

    async def _run_all_accounts_parallel(self, *, fee_cc: float, today: str) -> None:
        accounts = CredentialStore.load_all()
        if not accounts:
            log.info("Tidak ada akun untuk dieksekusi")
            return

        cfg = FeeWatcherStore.load()

        # filter akun yang belum selesai hari ini
        pending_accounts = [
            a for a in accounts
            if not self._account_done_today(a.name, today)
        ]

        if not pending_accounts:
            log.info("Semua akun sudah selesai tx hari ini")
            return

        log.info(
            "Eksekusi paralel untuk %d akun | fee_cc=%.4f | dry_run=%s",
            len(pending_accounts), fee_cc, self._dry_run,
        )
        if self._panel:
            await self._panel.send_to_admin(
                f"Memulai eksekusi {len(pending_accounts)} akun | fee: {fee_cc:.4f} CC"
            )

        executors = [
            AccountExecutor(
                cred,
                cc_price=0.0,
                fee_cc_at_trigger=fee_cc,
                dry_run=self._dry_run,
            )
            for cred in pending_accounts
        ]

        results: list[DayResult] = await asyncio.gather(
            *(ex.run_daily_cycle() for ex in executors),
            return_exceptions=False,
        )

        await self._send_summary(results, fee_cc)

    async def _send_summary(self, results: list[DayResult], fee_cc: float) -> None:
        today = datetime.now(timezone.utc).date().isoformat()
        lines = [f"Eksekusi selesai - {today}", f"Fee saat eksekusi: {fee_cc:.4f} CC", ""]

        total_tx = 0
        total_success = 0
        total_failed = 0

        for r in results:
            lines.append(f"{r.account_name}:")
            lines.append(f"  Total TX: {r.total_tx} | Sukses: {r.success_tx} | Gagal: {r.failed_tx}")
            for tx in r.tx_results:
                status = "OK" if tx.success else "GAGAL"
                err_part = f" | {tx.error[:40]}" if tx.error else ""
                lines.append(
                    f"  TX{tx.tx_index+1}: {tx.sell_symbol}->{tx.buy_symbol} "
                    f"| jual={tx.sell_amount} | dapat={tx.received_amount} "
                    f"| fee={tx.fee_cc:.4f} CC | {status}{err_part}"
                )
            total_tx += r.total_tx
            total_success += r.success_tx
            total_failed += r.failed_tx
            lines.append("")

        lines.append(f"Total semua akun: {total_tx} tx | sukses: {total_success} | gagal: {total_failed}")
        summary = "\n".join(lines)

        log.info("Summary:\n%s", summary)

        # broadcast ke semua subscriber (admin dan non-admin)
        if self._panel:
            await self._panel.broadcast(summary)

    def _all_accounts_done_today(self, accounts, today: str) -> bool:
        return all(self._account_done_today(a.name, today) for a in accounts)

    def _account_done_today(self, account_name: str, today: str) -> bool:
        prog = ProgressStore.load(account_name, today)
        return prog.completed_tx >= DAILY_TX_LIMIT

    async def _sleep_until_tomorrow(self) -> None:
        """Tidur sampai jam 00:05 UTC keesokan harinya."""
        now = datetime.now(timezone.utc)
        tomorrow = now.replace(hour=0, minute=5, second=0, microsecond=0)
        from datetime import timedelta
        if tomorrow <= now:
            tomorrow += timedelta(days=1)
        sleep_seconds = (tomorrow - now).total_seconds()
        log.info("Tidur selama %.0f detik hingga %s UTC", sleep_seconds, tomorrow.isoformat())
        if self._panel:
            await self._panel.send_to_admin(
                f"Semua akun selesai hari ini. Lanjut besok pukul {tomorrow.strftime('%H:%M')} UTC."
            )
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=sleep_seconds)
        except asyncio.TimeoutError:
            pass

    async def _on_config_updated(self, what: str) -> None:
        log.info("Config diupdate: %s", what)
        if what == "fee_cookie" and self._fee_watcher_instance:
            cfg = FeeWatcherStore.load()
            self._fee_watcher_instance.config = cfg
            log.info("Fee watcher config diperbarui")

    def request_stop(self) -> None:
        self._stop.set()


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
