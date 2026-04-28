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


def patch_sdk_proxy(sdk: Any, proxy_str: str) -> None:
    """
    Patch _get_session di SDK agar menggunakan proxy connector.
    """
    if not proxy_str:
        return
    try:
        proxy_url, proxy_auth = parse_proxy(proxy_str)
        log.debug("Proxy dikonfigurasi: %s", proxy_url)

        original_get_session = sdk._get_session

        async def _get_session_with_proxy() -> aiohttp.ClientSession:
            if sdk._session is None or sdk._session.closed:
                connector = aiohttp.TCPConnector(limit=20)
                sdk._session = aiohttp.ClientSession(
                    timeout=sdk._timeout,
                    connector=connector,
                    headers={"User-Agent": "CantexSDK/1.0"},
                )
            return sdk._session

        # Patch request method untuk inject proxy
        original_request = sdk._request

        async def _request_with_proxy(method: str, path: str, **kwargs) -> Any:
            kwargs.setdefault("proxy", proxy_url)
            if proxy_auth:
                kwargs.setdefault("proxy_auth", proxy_auth)
            return await original_request(method, path, **kwargs)

        sdk._request = _request_with_proxy
        sdk._proxy_url = proxy_url
        sdk._proxy_auth = proxy_auth
        log.info("Proxy berhasil dipatch untuk SDK")

    except Exception as exc:
        log.warning("Gagal patch proxy: %s", exc)
