from __future__ import annotations

"""
Signal broker: Fee watcher Railway mengirim sinyal ke executor Railway
via HTTP endpoint. Juga menerima summary report dari executor.
"""

import asyncio
import json
import logging
import os
import time
from pathlib import Path

import aiohttp
from aiohttp import web

from .storage import DATA_DIR, AccountCredential, CredentialStore, clear_all_volume

log = logging.getLogger("cantex.signal")

SIGNAL_FILE = DATA_DIR / "fee_signal.json"
SIGNAL_MAX_AGE_SECONDS = 90


def _get_executor_urls() -> list[str]:
    raw = os.environ.get("EXECUTOR_URLS", "")
    return [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]


# ---------------------------------------------------------------------------
# Signal Server — Railway #1
# ---------------------------------------------------------------------------

class SignalServer:
    def __init__(self, port: int = 8080) -> None:
        self._port = port
        self._last_signal: dict | None = None
        self._on_report_callback = None
        self._fee_watcher_ref = None   # set dari main.py setelah FeeWatcher dibuat
        self._app = web.Application()
        self._app.router.add_get("/signal", self._handle_get_signal)
        self._app.router.add_post("/report", self._handle_post_report)
        self._app.router.add_get("/health", self._handle_health)
        self._app.router.add_get("/fee", self._handle_get_fee)

    def set_report_callback(self, cb) -> None:
        self._on_report_callback = cb

    async def start(self) -> None:
        runner = web.AppRunner(self._app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self._port)
        await site.start()
        log.info("Signal server listening on :%s", self._port)

    def push_signal(self, fee_cc: float, fee_usd: float, fee_native: float) -> None:
        signal = {"ts": time.time(), "fee_cc": fee_cc, "fee_usd": fee_usd,
                  "fee_native": fee_native, "trigger": True}
        self._last_signal = signal
        try:
            SIGNAL_FILE.parent.mkdir(parents=True, exist_ok=True)
            SIGNAL_FILE.write_text(json.dumps(signal), encoding="utf-8")
        except Exception as exc:
            log.debug("Gagal tulis signal file: %s", exc)

    async def _handle_get_signal(self, request: web.Request) -> web.Response:
        if self._last_signal is None:
            return web.json_response({"trigger": False})
        return web.json_response(self._last_signal)

    async def _handle_post_report(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
            text = data.get("text", "")
            if text and self._on_report_callback:
                asyncio.create_task(self._on_report_callback(text))
            return web.json_response({"ok": True})
        except Exception as exc:
            log.warning("Gagal proses report: %s", exc)
            return web.json_response({"ok": False}, status=400)

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "mode": "fee_watcher"})

    async def _handle_get_fee(self, request: web.Request) -> web.Response:
        """
        Kembalikan fee terkini beserta apakah masih di bawah limit.
        Executor Railway query ini sebelum tiap TX pair untuk fee-gating.
        """
        if self._fee_watcher_ref is None:
            return web.json_response({"ok": False, "error": "no fee watcher"}, status=503)
        last = self._fee_watcher_ref.get_last_fee()
        age  = self._fee_watcher_ref.get_last_fee_age()
        from .storage import FeeWatcherStore
        cfg  = FeeWatcherStore.load()
        if last is None or age > 35:    # lebih dari 2 poll cycles = stale (15s interval)
            return web.json_response({
                "ok": False, "stale": True,
                "age_seconds": round(age, 1),
                "fee_cc": None,
                "max_fee_cc": cfg.max_fee_cc,
            })
        return web.json_response({
            "ok": last.fee_cc <= cfg.max_fee_cc,
            "stale": False,
            "age_seconds": round(age, 1),
            "fee_cc": last.fee_cc,
            "fee_usd": last.fee_usd,
            "max_fee_cc": cfg.max_fee_cc,
        })


# ---------------------------------------------------------------------------
# Executor Server — Railway #2-4
# ---------------------------------------------------------------------------

class ExecutorServer:
    def __init__(self, port: int = 8080) -> None:
        self._port = port
        self._app = web.Application()
        self._app.router.add_get("/signal", self._handle_get_signal)
        self._app.router.add_post("/credentials", self._handle_credentials)
        self._app.router.add_post("/clear_volume", self._handle_clear_volume)
        self._app.router.add_post("/clear_credentials", self._handle_clear_credentials)
        self._app.router.add_post("/set_running", self._handle_set_running)
        self._app.router.add_get("/health", self._handle_health)
        self._app.router.add_get("/accounts", self._handle_accounts)
        self._app.router.add_get("/progress", self._handle_progress)

    async def start(self) -> None:
        runner = web.AppRunner(self._app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self._port)
        await site.start()
        log.info("Executor HTTP server listening on :%s", self._port)

    async def _handle_get_signal(self, request: web.Request) -> web.Response:
        try:
            if SIGNAL_FILE.exists():
                data = json.loads(SIGNAL_FILE.read_text(encoding="utf-8"))
                return web.json_response(data)
        except Exception:
            pass
        return web.json_response({"trigger": False})

    async def _handle_credentials(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
            accounts = data.get("accounts", [])
            if data.get("replace", False):
                CredentialStore.clear_all()
            for acc_data in accounts:
                cred = AccountCredential.from_dict(acc_data)
                CredentialStore.save(cred)
            log.info("Remote credentials saved: %d accounts", len(accounts))
            return web.json_response({"ok": True, "saved": len(accounts)})
        except Exception as exc:
            log.warning("Gagal simpan credentials remote: %s", exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=400)

    async def _handle_clear_volume(self, request: web.Request) -> web.Response:
        try:
            clear_all_volume()
            log.info("Volume cleared via remote command")
            return web.json_response({"ok": True})
        except Exception as exc:
            log.warning("Gagal clear volume remote: %s", exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=500)

    async def _handle_clear_credentials(self, request: web.Request) -> web.Response:
        try:
            CredentialStore.clear_all()
            log.info("Credentials cleared via remote command")
            return web.json_response({"ok": True})
        except Exception as exc:
            log.warning("Gagal clear credentials remote: %s", exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=500)

    async def _handle_set_running(self, request: web.Request) -> web.Response:
        """
        FIX UTAMA: Sinkronisasi bot_state.running dari Railway #1.
        Karena setiap Railway punya volume terpisah, Railway #1 harus
        broadcast state ini ke semua executor setiap kali Start/Stop ditekan.
        """
        try:
            data = await request.json()
            running = bool(data.get("running", False))
            from .storage import BotStateStore
            state = BotStateStore.load()
            state.running = running
            BotStateStore.save(state)
            log.info("Bot running state set to %s via remote broadcast", running)
            return web.json_response({"ok": True})
        except Exception as exc:
            log.warning("Gagal set running state remote: %s", exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=400)

    async def _handle_accounts(self, request: web.Request) -> web.Response:
        accounts = CredentialStore.load_all()
        return web.json_response({"count": len(accounts), "mode": "executor"})

    async def _handle_progress(self, request: web.Request) -> web.Response:
        try:
            from datetime import datetime, timezone
            from .storage import ProgressStore
            today = datetime.now(timezone.utc).date().isoformat()
            accounts = CredentialStore.load_all()
            progress = ProgressStore.load_all_today(today)
            result_accounts = []
            for acct in accounts:
                prog = progress.get(acct.name)
                result_accounts.append({
                    "name": acct.name,
                    "success_tx": prog.success_tx if prog else 0,
                    "completed_tx": prog.completed_tx if prog else 0,
                    "tx_log": prog.tx_log if prog else [],
                })
            return web.json_response({"today": today, "accounts": result_accounts})
        except Exception as exc:
            log.warning("Gagal ambil progress remote: %s", exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=500)

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "mode": "executor"})


# ---------------------------------------------------------------------------
# Signal Client — Railway #2-4
# ---------------------------------------------------------------------------

class SignalClient:
    def __init__(self, watcher_url: str | None = None) -> None:
        self._watcher_url = (watcher_url or os.environ.get("FEE_WATCHER_URL", "")).rstrip("/")
        self._last_consumed_ts: float = 0.0

    async def wait_for_trigger(self, timeout: float = 3600.0) -> dict | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            signal = await self._poll_signal()
            if signal and signal.get("trigger"):
                ts = float(signal.get("ts", 0))
                age = time.time() - ts
                if age <= SIGNAL_MAX_AGE_SECONDS and ts > self._last_consumed_ts:
                    self._last_consumed_ts = ts
                    return signal
            await asyncio.sleep(3)
        return None

    async def send_report(self, text: str) -> None:
        if not self._watcher_url:
            log.warning("FEE_WATCHER_URL tidak diset, report tidak terkirim")
            return
        try:
            url = self._watcher_url + "/report"
            async with aiohttp.ClientSession() as s:
                async with s.post(url, json={"text": text},
                                  timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        log.info("Report berhasil dikirim ke fee watcher")
                    else:
                        log.warning("Gagal kirim report, status: %s", resp.status)
        except Exception as exc:
            log.warning("Gagal kirim report ke fee watcher: %s", exc)

    async def _poll_signal(self) -> dict | None:
        if not self._watcher_url:
            return self._poll_file()
        try:
            url = self._watcher_url + "/signal"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        return await resp.json()
        except Exception as exc:
            log.debug("HTTP poll error: %s", exc)
        return None

    def _poll_file(self) -> dict | None:
        try:
            if SIGNAL_FILE.exists():
                return json.loads(SIGNAL_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
        return None

    async def check_fee_ok(self) -> tuple[bool, float | None]:
        """
        Cek fee saat ini di Railway #1.
        Return (ok, fee_cc).
        - ok=True  → fee masih oke, lanjut TX pair
        - ok=False → fee sudah naik, pause eksekusi
        BUG FIX: kalau Railway #1 tidak bisa dihubungi (network error),
        return True (allow) bukan False (block) — jangan blok eksekusi
        hanya karena koneksi sementara gagal.
        """
        if not self._watcher_url:
            return True, None
        try:
            url = self._watcher_url + "/fee"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return bool(data.get("ok", False)), data.get("fee_cc")
        except Exception as exc:
            log.warning("check_fee_ok gagal (%s) — allow through (jangan blok eksekusi)", exc)
            return True, None   # FIX: unreachable = allow, bukan block
        return True, None


# ---------------------------------------------------------------------------
# Remote management helpers
# ---------------------------------------------------------------------------

async def fetch_executor_account_count(url: str) -> int:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url + "/accounts",
                             timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return int(data.get("count", 0))
    except Exception:
        pass
    return 0


async def fetch_all_executor_account_counts() -> int:
    urls = _get_executor_urls()
    if not urls:
        return 0
    tasks = [fetch_executor_account_count(url) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return sum(r for r in results if isinstance(r, int))


async def push_credentials_to_url(url: str, accounts: list[AccountCredential], replace: bool = True) -> dict:
    try:
        payload = {"replace": replace, "accounts": [a.to_dict() for a in accounts]}
        async with aiohttp.ClientSession() as s:
            async with s.post(url + "/credentials", json=payload,
                              timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
                return {"ok": resp.status == 200, "status": resp.status, **data}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def clear_volume_at_url(url: str) -> dict:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url + "/clear_volume",
                              timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
                return {"ok": resp.status == 200, "status": resp.status, **data}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def broadcast_clear_volume() -> dict[str, dict]:
    results: dict[str, dict] = {}
    urls = _get_executor_urls()
    if not urls:
        return results
    tasks = {url: asyncio.create_task(clear_volume_at_url(url)) for url in urls}
    for url, task in tasks.items():
        try:
            results[url] = await task
        except Exception as exc:
            results[url] = {"ok": False, "error": str(exc)}
    return results


async def _set_running_at_url(url: str, running: bool) -> dict:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url + "/set_running", json={"running": running},
                              timeout=aiohttp.ClientTimeout(total=5)) as resp:
                return {"ok": resp.status == 200}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def broadcast_bot_state(running: bool) -> dict[str, dict]:
    """
    Broadcast bot running=True/False ke semua executor Railways.
    WAJIB dipanggil setiap kali admin klik Start/Stop Bot.
    Ini fix untuk bug: executor selalu baca is_running()=False dari volume-nya sendiri.
    """
    urls = _get_executor_urls()
    if not urls:
        return {}
    results: dict[str, dict] = {}
    tasks = [(url, asyncio.create_task(_set_running_at_url(url, running))) for url in urls]
    for url, task in tasks:
        try:
            results[url] = await task
        except Exception as exc:
            results[url] = {"ok": False, "error": str(exc)}
    log.info(
        "Broadcast bot_state running=%s ke %d executor(s): %s",
        running, len(urls),
        {u.split("//")[-1][:20]: r.get("ok") for u, r in results.items()},
    )
    return results


async def _clear_credentials_at_url(url: str) -> dict:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url + "/clear_credentials",
                              timeout=aiohttp.ClientTimeout(total=5)) as resp:
                return {"ok": resp.status == 200}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def broadcast_clear_credentials() -> dict[str, dict]:
    """Hapus hanya credentials di semua executor Railways."""
    urls = _get_executor_urls()
    if not urls:
        return {}
    results: dict[str, dict] = {}
    tasks = [(url, asyncio.create_task(_clear_credentials_at_url(url))) for url in urls]
    for url, task in tasks:
        try:
            results[url] = await task
        except Exception as exc:
            results[url] = {"ok": False, "error": str(exc)}
    return results


async def fetch_executor_progress(url: str) -> dict | None:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url + "/progress",
                             timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception:
        pass
    return None


async def fetch_all_executor_progress() -> list[tuple[int, dict | None]]:
    """Ambil progress dari semua executor Railways. Return [(railway_num, data|None)]."""
    urls = _get_executor_urls()
    if not urls:
        return []
    tasks = [fetch_executor_progress(url) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [
        (i + 2, r if isinstance(r, dict) else None)
        for i, r in enumerate(results)
    ]
