from __future__ import annotations

"""
Signal broker: Fee watcher Railway mengirim sinyal ke executor Railway
via HTTP endpoint. Juga menerima summary report dari executor.

Mode:
  FEE_WATCHER=true  -> fee watcher + HTTP server (port $PORT atau 8080)
  FEE_WATCHER=false -> executor, polling sinyal + mini HTTP server
                       untuk menerima credentials/clear_volume remote
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
    """
    Baca EXECUTOR_URLS dari env (comma-separated).
    Contoh: https://r2.up.railway.app,https://r3.up.railway.app,https://r4.up.railway.app
    """
    raw = os.environ.get("EXECUTOR_URLS", "")
    return [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]


# ---------------------------------------------------------------------------
# Signal Server — Railway #1 (fee watcher)
# ---------------------------------------------------------------------------

class SignalServer:
    """
    Berjalan di fee watcher Railway (#1).
    - /signal   : executor poll fee trigger
    - /report   : executor kirim summary
    - /health   : health check
    """

    def __init__(self, port: int = 8080) -> None:
        self._port = port
        self._last_signal: dict | None = None
        self._on_report_callback = None
        self._app = web.Application()
        self._app.router.add_get("/signal", self._handle_get_signal)
        self._app.router.add_post("/report", self._handle_post_report)
        self._app.router.add_get("/health", self._handle_health)

    def set_report_callback(self, cb) -> None:
        self._on_report_callback = cb

    async def start(self) -> None:
        runner = web.AppRunner(self._app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self._port)
        await site.start()
        log.info("Signal server listening on :%s", self._port)

    def push_signal(self, fee_cc: float, fee_usd: float, fee_native: float) -> None:
        signal = {
            "ts": time.time(),
            "fee_cc": fee_cc,
            "fee_usd": fee_usd,
            "fee_native": fee_native,
            "trigger": True,
        }
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


# ---------------------------------------------------------------------------
# Executor Server — Railway #2-4
# ---------------------------------------------------------------------------

class ExecutorServer:
    """
    Mini HTTP server untuk executor Railways (#2-4).
    Menerima credentials dan clear_volume command dari Railway #1.
    """

    def __init__(self, port: int = 8080) -> None:
        self._port = port
        self._app = web.Application()
        self._app.router.add_get("/signal", self._handle_get_signal)
        self._app.router.add_post("/credentials", self._handle_credentials)
        self._app.router.add_post("/clear_volume", self._handle_clear_volume)
        self._app.router.add_get("/health", self._handle_health)

    async def start(self) -> None:
        runner = web.AppRunner(self._app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self._port)
        await site.start()
        log.info("Executor HTTP server listening on :%s", self._port)

    async def _handle_get_signal(self, request: web.Request) -> web.Response:
        """Fallback: executor bisa juga terima push signal via HTTP."""
        try:
            if SIGNAL_FILE.exists():
                data = json.loads(SIGNAL_FILE.read_text(encoding="utf-8"))
                return web.json_response(data)
        except Exception:
            pass
        return web.json_response({"trigger": False})

    async def _handle_credentials(self, request: web.Request) -> web.Response:
        """Simpan credentials yang dikirim dari Railway #1."""
        try:
            data = await request.json()
            accounts = data.get("accounts", [])
            # Kalau replace=True, hapus semua dulu lalu simpan baru
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
        """Hapus semua volume data via remote command."""
        try:
            clear_all_volume()
            log.info("Volume cleared via remote command")
            return web.json_response({"ok": True})
        except Exception as exc:
            log.warning("Gagal clear volume remote: %s", exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=500)

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "mode": "executor"})


# ---------------------------------------------------------------------------
# Signal Client — Railway #2-4
# ---------------------------------------------------------------------------

class SignalClient:
    """
    Berjalan di executor Railway (#2-4).
    - Poll sinyal fee trigger dari Railway #1
    - Kirim summary report ke Railway #1
    """

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
                async with s.post(
                    url, json={"text": text},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
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


# ---------------------------------------------------------------------------
# Remote management helpers (digunakan dari Telegram panel)
# ---------------------------------------------------------------------------

async def push_credentials_to_url(url: str, accounts: list[AccountCredential], replace: bool = True) -> dict:
    """Kirim credentials ke executor Railway via HTTP."""
    try:
        payload = {
            "replace": replace,
            "accounts": [a.to_dict() for a in accounts],
        }
        async with aiohttp.ClientSession() as s:
            async with s.post(
                url + "/credentials",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()
                return {"ok": resp.status == 200, "status": resp.status, **data}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def clear_volume_at_url(url: str) -> dict:
    """Hapus volume data di executor Railway via HTTP."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                url + "/clear_volume",
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()
                return {"ok": resp.status == 200, "status": resp.status, **data}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def broadcast_clear_volume() -> dict[str, dict]:
    """Hapus volume di semua executor Railways. Return hasil per URL."""
    results: dict[str, dict] = {}
    urls = _get_executor_urls()
    if not urls:
        log.info("Tidak ada EXECUTOR_URLS dikonfigurasi, skip broadcast clear")
        return results
    tasks = {url: asyncio.create_task(clear_volume_at_url(url)) for url in urls}
    for url, task in tasks.items():
        try:
            results[url] = await task
        except Exception as exc:
            results[url] = {"ok": False, "error": str(exc)}
    return results
