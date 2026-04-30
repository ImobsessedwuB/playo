from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Callable, Awaitable

import aiohttp
import websockets

from .storage import FeeWatcherConfig, FeeWatcherStore

log = logging.getLogger("cantex.fee_watcher")

CANTEX_WS_URL = "wss://api.cantex.io/v1/ws/public"
CANTEX_QUOTE_URL = "https://api.cantex.io/v2/pools/quote"


@dataclass
class FeeResult:
    fee_native: float
    fee_usd: float
    fee_cc: float
    cc_price: float


TriggerCallback = Callable[[FeeResult], Awaitable[None]]


class FeeWatcher:
    """
    Mengambil gas fee dari Cantex dan memicu callback jika fee di bawah batas.
    Berjalan di Railway instance yang di-set FEE_WATCHER=true.
    """

    def __init__(self, config: FeeWatcherConfig) -> None:
        self.config = config
        self._cc_price: float = 0.0
        self._last_fee: FeeResult | None = None
        self._last_fee_ts: float = 0.0
        self._cookie_expired_notified: bool = False
        self._stop = asyncio.Event()
        self._trigger_callbacks: list[TriggerCallback] = []

    def add_trigger_callback(self, cb: TriggerCallback) -> None:
        self._trigger_callbacks.append(cb)

    async def start(self) -> None:
        asyncio.create_task(self._ws_loop(), name="fee-watcher-ws")
        asyncio.create_task(self._poll_loop(), name="fee-watcher-poll")

    async def stop(self) -> None:
        self._stop.set()

    def get_last_fee(self) -> FeeResult | None:
        """Kembalikan fee terakhir yang di-check (tanpa API call baru)."""
        return self._last_fee

    def get_last_fee_age(self) -> float:
        """Berapa detik sejak fee terakhir di-check."""
        if self._last_fee_ts == 0:
            return float("inf")
        return __import__("time").time() - self._last_fee_ts

    async def get_fee_now(self) -> FeeResult | None:
        result = await self._check_fee()
        if result is not None:
            self._last_fee = result
            self._last_fee_ts = __import__("time").time()
        return result

    # ------------------------------------------------------------------

    async def _ws_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._run_ws()
            except Exception as exc:
                log.warning("WS error, reconnect dalam 3s: %s", exc)
                await asyncio.sleep(3)

    async def _run_ws(self) -> None:
        async with websockets.connect(
            CANTEX_WS_URL,
            additional_headers={"origin": "https://www.cantex.io"},
            ping_interval=20,
            ping_timeout=10,
        ) as ws:
            log.info("WS connected")
            await ws.send('{"op":"subscribe","channels":["market.CC-USDC.ticker"]}')
            async for raw in ws:
                if self._stop.is_set():
                    break
                try:
                    import json
                    parsed = json.loads(raw)
                    if parsed.get("op") == "ping":
                        await ws.send('{"op":"pong"}')
                        log.info("ping ok")
                        continue
                    channel = parsed.get("channel", "")
                    price = (parsed.get("data") or {}).get("price")
                    if price and channel == "market.CC-USDC.ticker":
                        new_price = float(price)
                        if new_price > 0:
                            self._cc_price = new_price
                            log.info("cc price updated: %.5f", new_price)
                except Exception as exc:
                    log.debug("WS parse error: %s", exc)

    async def _poll_loop(self) -> None:
        await asyncio.sleep(5)
        while not self._stop.is_set():
            if self._cc_price > 0:
                result = await self._check_fee()
                if result is not None:
                    self._last_fee = result          # <-- simpan untuk query eksternal
                    self._last_fee_ts = __import__("time").time()
                    log.info(
                        "gas fee  |  %.6f native  |  $%.4f usd  |  %.6f CC  (limit %.6f CC)",
                        result.fee_native, result.fee_usd, result.fee_cc,
                        self.config.max_fee_cc,
                    )
                    if result.fee_cc <= self.config.max_fee_cc:
                        log.info(
                            "Fee %.6f CC <= limit %.6f CC — trigger eksekusi",
                            result.fee_cc, self.config.max_fee_cc,
                        )
                        for cb in self._trigger_callbacks:
                            try:
                                await cb(result)
                            except Exception as exc:
                                log.warning("Trigger callback error: %s", exc)
                else:
                    log.debug("Fee check None (cookie expired atau API error)")
            else:
                log.debug("CC price belum ready, skip fee check")
            await asyncio.sleep(60)

    async def _check_fee(self) -> FeeResult | None:
        payload = {
            "sellInstrumentId": self.config.sell_instrument_id,
            "sellInstrumentAdmin": self.config.sell_instrument_admin,
            "sellAmount": self.config.sell_amount,
            "buyInstrumentId": self.config.buy_instrument_id,
            "buyInstrumentAdmin": self.config.buy_instrument_admin,
        }
        headers = {
            "content-type": "text/plain;charset=UTF-8",
            "origin": "https://www.cantex.io",
            "referer": "https://www.cantex.io/",
            "cookie": self.config.cantex_cookie,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    CANTEX_QUOTE_URL, json=payload, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 401:
                        if not self._cookie_expired_notified:
                            self._cookie_expired_notified = True
                            log.warning("Cookie Cantex expired (401) — perlu update cookie")
                        return None
                    if resp.status != 200:
                        log.warning("Quote API status %s", resp.status)
                        return None
                    self._cookie_expired_notified = False
                    data = await resp.json(content_type=None)
                    network_fee = float(data["fees"]["network_fee"]["amount"])
                    admin_fee = float(data["fees"]["amount_admin"])
                    liquidity_fee = float(data["fees"]["amount_liquidity"])
                    total_native = network_fee + admin_fee + liquidity_fee
                    trade_price = float(data["trade_price"])
                    fee_usd = total_native * trade_price
                    fee_cc = (fee_usd / self._cc_price) if self._cc_price else 0.0
                    return FeeResult(
                        fee_native=total_native,
                        fee_usd=fee_usd,
                        fee_cc=fee_cc,
                        cc_price=self._cc_price,
                    )
        except asyncio.TimeoutError:
            log.debug("Quote API timeout")
            return None
        except Exception as exc:
            log.debug("Quote API error: %s", exc)
            return None
