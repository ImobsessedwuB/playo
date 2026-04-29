from __future__ import annotations

"""
Patch CantexSDK untuk support HTTP proxy.
Format proxy: username:password@ip:port
"""

import logging
import re
from typing import Any

import aiohttp

log = logging.getLogger("cantex.proxy")


def parse_proxy(proxy_str: str) -> tuple[str, aiohttp.BasicAuth | None]:
    """
    Parse proxy string format: username:password@ip:port
    Return (proxy_url, auth)
    """
    proxy_str = proxy_str.strip()
    # cek apakah sudah ada scheme
    if not proxy_str.startswith("http"):
        proxy_str = "http://" + proxy_str

    match = re.match(
        r"(https?://)(?:([^:@]+):([^@]+)@)?(.+)",
        proxy_str,
    )
    if not match:
        raise ValueError(f"Format proxy tidak valid: {proxy_str}")

    scheme = match.group(1)
    username = match.group(2)
    password = match.group(3)
    host_port = match.group(4)

    proxy_url = f"{scheme}{host_port}"
    auth = aiohttp.BasicAuth(username, password) if username and password else None
    return proxy_url, auth


class _ProxiedSession(aiohttp.ClientSession):
    """
    Subclass ClientSession yang otomatis inject proxy + proxy_auth
    ke setiap request(). SDK memanggil session.request() secara langsung
    tanpa proxy param — ini cara paling bersih untuk menyuntikkan proxy
    tanpa menyentuh signature _request() milik SDK.
    """

    def __init__(self, proxy_url: str, proxy_auth: aiohttp.BasicAuth | None, **kwargs):
        super().__init__(**kwargs)
        self._proxy_url_val  = proxy_url
        self._proxy_auth_val = proxy_auth

    def request(self, method: str, url, **kwargs):
        kwargs.setdefault("proxy", self._proxy_url_val)
        if self._proxy_auth_val is not None:
            kwargs.setdefault("proxy_auth", self._proxy_auth_val)
        return super().request(method, url, **kwargs)


def patch_sdk_proxy(sdk: Any, proxy_str: str) -> None:
    """
    Patch _get_session di SDK agar setiap request melalui proxy.

    Fix: JANGAN patch sdk._request() — method itu tidak terima kwarg 'proxy'.
    Yang benar adalah override _get_session() agar return _ProxiedSession
    yang inject proxy di level session.request().
    """
    if not proxy_str:
        return
    try:
        proxy_url, proxy_auth = parse_proxy(proxy_str)
        log.debug("Proxy dikonfigurasi: %s", proxy_url)

        async def _get_session_with_proxy() -> aiohttp.ClientSession:
            if sdk._session is None or sdk._session.closed:
                connector = aiohttp.TCPConnector(limit=20)
                sdk._session = _ProxiedSession(
                    proxy_url,
                    proxy_auth,
                    timeout=sdk._timeout,
                    connector=connector,
                    headers={"User-Agent": "CantexSDK/1.0"},
                )
            return sdk._session

        sdk._get_session  = _get_session_with_proxy
        sdk._proxy_url    = proxy_url
        sdk._proxy_auth   = proxy_auth
        log.info("Proxy berhasil dipatch untuk SDK")

    except Exception as exc:
        log.warning("Gagal patch proxy: %s", exc)
