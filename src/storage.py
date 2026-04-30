from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

log = logging.getLogger("cantex.storage")

DATA_DIR = Path(os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "/data"))


def _data_path(filename: str) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR / filename


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Gagal baca %s: %s", path, exc)
        return {}


def _save_json(path: Path, data: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        tmp.replace(path)
    except Exception as exc:
        log.warning("Gagal simpan %s: %s", path, exc)


# ---------------------------------------------------------------------------
# Account credentials store
# ---------------------------------------------------------------------------

@dataclass
class AccountCredential:
    name: str
    operator_key: str
    trading_key: str
    proxy: str | None = None
    proxies: list = None

    def __post_init__(self):
        if self.proxies is None:
            self.proxies = []
        if self.proxy and self.proxy not in self.proxies:
            self.proxies = [self.proxy] + [p for p in self.proxies if p != self.proxy]
        if self.proxies and not self.proxy:
            self.proxy = self.proxies[0]

    def get_proxy_list(self) -> list:
        """Kembalikan list proxy yang valid untuk rotasi."""
        lst = [p for p in (self.proxies or []) if p]
        if not lst and self.proxy:
            lst = [self.proxy]
        return lst

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "operator_key": self.operator_key,
            "trading_key": self.trading_key,
            "proxy": self.proxy,
            "proxies": self.proxies or [],
        }

    @staticmethod
    def from_dict(d: dict) -> "AccountCredential":
        proxies = list(d.get("proxies") or [])
        proxy   = d.get("proxy")
        if proxy and proxy not in proxies:
            proxies = [proxy] + [p for p in proxies if p != proxy]
        return AccountCredential(
            name=d["name"],
            operator_key=d["operator_key"],
            trading_key=d["trading_key"],
            proxy=proxy or (proxies[0] if proxies else None),
            proxies=proxies,
        )


class CredentialStore:
    _path = _data_path("credentials.json")

    @classmethod
    def load_all(cls) -> list[AccountCredential]:
        raw = _load_json(cls._path)
        return [AccountCredential.from_dict(v) for v in raw.get("accounts", {}).values()]

    @classmethod
    def save(cls, cred: AccountCredential) -> None:
        raw = _load_json(cls._path)
        accounts = raw.get("accounts", {})
        accounts[cred.name] = cred.to_dict()
        raw["accounts"] = accounts
        _save_json(cls._path, raw)

    @classmethod
    def delete(cls, name: str) -> None:
        raw = _load_json(cls._path)
        accounts = raw.get("accounts", {})
        accounts.pop(name, None)
        raw["accounts"] = accounts
        _save_json(cls._path, raw)

    @classmethod
    def clear_all(cls) -> None:
        _save_json(cls._path, {"accounts": {}})


# ---------------------------------------------------------------------------
# Fee watcher config store
# ---------------------------------------------------------------------------

@dataclass
class FeeWatcherConfig:
    cantex_cookie: str = ""
    max_fee_cc: float = 0.27
    sell_instrument_id: str = "Amulet"
    sell_instrument_admin: str = "DSO::1220b1431ef217342db44d516bb9befde802be7d8899637d290895fa58880f19accc"
    sell_amount: str = "1.7761889243"
    buy_instrument_id: str = "USDCx"
    buy_instrument_admin: str = "decentralized-usdc-interchain-rep::12208115f1e168dd7e792320be9c4ca720c751a02a3053c7606e1c1cd3dad9bf60ef"

    def to_dict(self) -> dict:
        return {
            "cantex_cookie": self.cantex_cookie,
            "max_fee_cc": self.max_fee_cc,
            "sell_instrument_id": self.sell_instrument_id,
            "sell_instrument_admin": self.sell_instrument_admin,
            "sell_amount": self.sell_amount,
            "buy_instrument_id": self.buy_instrument_id,
            "buy_instrument_admin": self.buy_instrument_admin,
        }

    @staticmethod
    def from_dict(d: dict) -> "FeeWatcherConfig":
        return FeeWatcherConfig(
            cantex_cookie=d.get("cantex_cookie", ""),
            max_fee_cc=float(d.get("max_fee_cc", 0.27)),
            sell_instrument_id=d.get("sell_instrument_id", "Amulet"),
            sell_instrument_admin=d.get("sell_instrument_admin", "DSO::1220b1431ef217342db44d516bb9befde802be7d8899637d290895fa58880f19accc"),
            sell_amount=d.get("sell_amount", "1.7761889243"),
            buy_instrument_id=d.get("buy_instrument_id", "USDCx"),
            buy_instrument_admin=d.get("buy_instrument_admin", "decentralized-usdc-interchain-rep::12208115f1e168dd7e792320be9c4ca720c751a02a3053c7606e1c1cd3dad9bf60ef"),
        )


class FeeWatcherStore:
    _path = _data_path("fee_watcher.json")

    @classmethod
    def load(cls) -> FeeWatcherConfig:
        raw = _load_json(cls._path)
        if not raw:
            return FeeWatcherConfig()
        return FeeWatcherConfig.from_dict(raw)

    @classmethod
    def save(cls, cfg: FeeWatcherConfig) -> None:
        _save_json(cls._path, cfg.to_dict())


# ---------------------------------------------------------------------------
# Daily progress store
# ---------------------------------------------------------------------------

@dataclass
class DailyProgress:
    utc_date: str = ""
    completed_tx: int = 0
    tx_log: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "utc_date": self.utc_date,
            "completed_tx": self.completed_tx,
            "tx_log": self.tx_log,
        }

    @staticmethod
    def from_dict(d: dict) -> "DailyProgress":
        return DailyProgress(
            utc_date=d.get("utc_date", ""),
            completed_tx=int(d.get("completed_tx", 0)),
            tx_log=d.get("tx_log", []),
        )


class ProgressStore:
    _path = _data_path("progress.json")

    @classmethod
    def load(cls, account_name: str, today: str) -> DailyProgress:
        raw = _load_json(cls._path)
        account_data = raw.get("accounts", {}).get(account_name, {})
        progress = DailyProgress.from_dict(account_data)
        if progress.utc_date != today:
            return DailyProgress(utc_date=today)
        return progress

    @classmethod
    def save(cls, account_name: str, progress: DailyProgress) -> None:
        raw = _load_json(cls._path)
        accounts = raw.get("accounts", {})
        accounts[account_name] = progress.to_dict()
        raw["accounts"] = accounts
        _save_json(cls._path, raw)

    @classmethod
    def load_all_today(cls, today: str) -> dict[str, DailyProgress]:
        raw = _load_json(cls._path)
        result = {}
        for name, data in raw.get("accounts", {}).items():
            p = DailyProgress.from_dict(data)
            result[name] = p if p.utc_date == today else DailyProgress(utc_date=today)
        return result


# ---------------------------------------------------------------------------
# Bot running state store (shared between all admin instances)
# ---------------------------------------------------------------------------

@dataclass
class BotState:
    running: bool = False
    started_by: str = ""
    started_at: str = ""
    stopped_by: str = ""
    stopped_at: str = ""

    def to_dict(self) -> dict:
        return {
            "running": self.running,
            "started_by": self.started_by,
            "started_at": self.started_at,
            "stopped_by": self.stopped_by,
            "stopped_at": self.stopped_at,
        }

    @staticmethod
    def from_dict(d: dict) -> "BotState":
        return BotState(
            running=bool(d.get("running", False)),
            started_by=d.get("started_by", ""),
            started_at=d.get("started_at", ""),
            stopped_by=d.get("stopped_by", ""),
            stopped_at=d.get("stopped_at", ""),
        )


class BotStateStore:
    _path = _data_path("bot_state.json")

    @classmethod
    def load(cls) -> BotState:
        raw = _load_json(cls._path)
        if not raw:
            return BotState()
        return BotState.from_dict(raw)

    @classmethod
    def save(cls, state: BotState) -> None:
        _save_json(cls._path, state.to_dict())

    @classmethod
    def is_running(cls) -> bool:
        return cls.load().running


def clear_all_volume() -> None:
    """Hapus semua file data di volume (reset total)."""
    files = [
        "credentials.json",
        "fee_watcher.json",
        "progress.json",
        "bot_state.json",
        "fee_signal.json",
    ]
    for fname in files:
        p = DATA_DIR / fname
        try:
            if p.exists():
                p.unlink()
        except Exception as exc:
            log.warning("Gagal hapus %s: %s", fname, exc)
