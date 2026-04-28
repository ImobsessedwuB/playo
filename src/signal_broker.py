from __future__ import annotations

"""
Signal broker: Fee watcher Railway mengirim sinyal ke executor Railway
via HTTP endpoint sederhana atau shared volume file.

Mode:
  FEE_WATCHER=true  -> berjalan sebagai fee watcher + HTTP server (port $PORT atau 8080)
  FEE_WATCHER=false -> berjalan sebagai executor, polling sinyal dari watcher
"""

import asyncio
import json
import logging
import os
import time
from pathlib import Path

from aiohttp import web

from .storage import DATA_DIR

log = logging.getLogger("cantex.signal")

SIGNAL_FILE = DATA_DIR / "fee_signal.json"
SIGNAL_MAX_AGE_SECONDS = 90  # sinyal lebih dari 90 detik dianggap stale


class SignalServer:
    """
    Berjalan di fee watcher Railway.
    Menerima push dari FeeWatcher lalu menulis sinyal ke file & expose HTTP.
    """

    def __init__(self, port: int = 8080) -> None:
        self._port = port
        self._last_signal: dict | None = None
        self._app = web.Application()
        self._app.router.add_get("/signal", self._handle_get)
        self._app.router.add_post("/signal", self._handle_post)
        self._app.router.add_get("/health", self._handle_health)

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

    async def _handle_get(self, request: web.Request) -> web.Response:
        if self._last_signal is None:
            return web.json_response({"trigger": False})
        return web.json_response(self._last_signal)

    async def _handle_post(self, request: web.Request) -> web.Response:
        data = await request.json()
        self._last_signal = data
        return web.json_response({"ok": True})

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})


class SignalClient:
    """
    Berjalan di executor Railway.
    Poll sinyal dari fee watcher via HTTP atau local file.
    """

    def __init__(self, watcher_url: str | None = None) -> None:
        self._watcher_url = watcher_url or os.environ.get("FEE_WATCHER_URL", "")
        self._last_consumed_ts: float = 0.0

    async def wait_for_trigger(self, timeout: float = 300.0) -> dict | None:
        """
        Tunggu sinyal trigger yang sah (fee di bawah batas).
        Return dict sinyal atau None jika timeout.
        """
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

    async def get_current_signal(self) -> dict | None:
        return await self._poll_signal()

    async def _poll_signal(self) -> dict | None:
        if self._watcher_url:
            return await self._poll_http()
        return self._poll_file()

    async def _poll_http(self) -> dict | None:
        import aiohttp
        try:
            url = self._watcher_url.rstrip("/") + "/signal"
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
                data = json.loads(SIGNAL_FILE.read_text(encoding="utf-8"))
                return data
        except Exception:
            pass
        return None
