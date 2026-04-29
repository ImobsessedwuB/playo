from __future__ import annotations

"""
Telegram bot control panel — Cantex Swap System
Admin IDs: 6469077855, 1118770958

Env vars:
  EXECUTOR_URLS   : comma-separated URLs untuk Railway #2, #3, #4
                    contoh: https://r2.up.railway.app,https://r3.up.railway.app,https://r4.up.railway.app
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from .signal_broker import (
    broadcast_clear_volume,
    push_credentials_to_url,
    _get_executor_urls,
)
from .storage import (
    AccountCredential,
    BotState,
    BotStateStore,
    CredentialStore,
    FeeWatcherConfig,
    FeeWatcherStore,
    ProgressStore,
    clear_all_volume,
)

log = logging.getLogger("cantex.telegram")

ADMIN_IDS: set[int] = {6469077855, 1118770958}

(
    STATE_IDLE,
    STATE_AWAIT_RAILWAY_SELECT,
    STATE_AWAIT_ACCOUNT_COUNT,
    STATE_AWAIT_CREDS,
    STATE_AWAIT_PROXY,
    STATE_AWAIT_FEE_COOKIE,
    STATE_AWAIT_MAX_FEE,
    STATE_AWAIT_DELETE_CONFIRM,
    STATE_AWAIT_VOLUME_CONFIRM,
) = range(9)


# ---------------------------------------------------------------------------
# Luxury UI helpers — tanpa garis ─────────
# ---------------------------------------------------------------------------

def _lux_title(text: str) -> str:
    return f"✦  {text.upper()}"


def _lux_field(key: str, val: str, w: int = 14) -> str:
    return f"  {key:<{w}}·  {val}"


def _lux_note(text: str) -> str:
    return f"  {text}"


def _is_admin(update: Update) -> bool:
    return update.effective_user is not None and update.effective_user.id in ADMIN_IDS


def _admin_name(update: Update) -> str:
    u = update.effective_user
    if u is None:
        return "Unknown"
    return u.full_name or u.username or str(u.id)


def _railway_label(idx: int) -> str:
    """idx 1=local, 2-4=executor."""
    if idx == 1:
        return "Railway 1  (local)"
    urls = _get_executor_urls()
    url_idx = idx - 2
    if url_idx < len(urls):
        return f"Railway {idx}  ({urls[url_idx].split('//')[1][:30]})"
    return f"Railway {idx}  (not configured)"


# ---------------------------------------------------------------------------
# Keyboards
# ---------------------------------------------------------------------------

def _main_menu_keyboard(
    has_accounts: bool,
    has_proxy: bool,
    bot_running: bool,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    rows.append([InlineKeyboardButton("📥  Input Credentials", callback_data="input_creds")])

    if has_accounts:
        label = "⏹  Stop Bot" if bot_running else "▶️  Start Bot"
        rows.append([InlineKeyboardButton(label, callback_data="stop_bot" if bot_running else "start_bot")])

    rows.append([InlineKeyboardButton("🌐  " + ("Change Proxy" if has_proxy else "Set Proxy"), callback_data="set_proxy")])
    rows.append([InlineKeyboardButton("🍪  Set Fee Cookie", callback_data="set_fee_cookie")])
    rows.append([InlineKeyboardButton("⚖️  Set Max Fee (CC)", callback_data="set_max_fee")])
    rows.append([
        InlineKeyboardButton("📊  Status", callback_data="status"),
        InlineKeyboardButton("📋  Summary", callback_data="summary"),
    ])

    if has_accounts:
        rows.append([InlineKeyboardButton("🗑  Delete All Accounts", callback_data="delete_accounts")])
    rows.append([InlineKeyboardButton("💣  Delete All Volume Data", callback_data="delete_volume")])

    return InlineKeyboardMarkup(rows)


def _railway_select_keyboard() -> InlineKeyboardMarkup:
    """Pilih Railway tujuan untuk input credentials."""
    urls = _get_executor_urls()
    rows: list[list[InlineKeyboardButton]] = []

    # Baris 1: Railway 1 & 2
    r1 = InlineKeyboardButton("Railway 1", callback_data="creds_r1")
    r2_label = "Railway 2" if len(urls) >= 1 else "Railway 2 ✗"
    r2 = InlineKeyboardButton(r2_label, callback_data="creds_r2")
    rows.append([r1, r2])

    # Baris 2: Railway 3 & 4
    r3_label = "Railway 3" if len(urls) >= 2 else "Railway 3 ✗"
    r4_label = "Railway 4" if len(urls) >= 3 else "Railway 4 ✗"
    r3 = InlineKeyboardButton(r3_label, callback_data="creds_r3")
    r4 = InlineKeyboardButton(r4_label, callback_data="creds_r4")
    rows.append([r3, r4])

    rows.append([InlineKeyboardButton("↩  Back", callback_data="back_menu")])
    return InlineKeyboardMarkup(rows)


# ---------------------------------------------------------------------------
# TelegramPanel
# ---------------------------------------------------------------------------

class TelegramPanel:
    def __init__(self, token: str, *, on_config_updated=None) -> None:
        self._token = token
        self._on_config_updated = on_config_updated
        self._app: Application | None = None
        self._pending_input: dict[int, dict] = {}
        self._subscriber_ids: set[int] = set(ADMIN_IDS)

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def start(self) -> None:
        log.info("Inisialisasi Telegram bot...")
        for lib in ("telegram", "httpx", "httpcore", "apscheduler"):
            logging.getLogger(lib).setLevel(logging.WARNING)

        try:
            self._app = Application.builder().token(self._token).build()
            app = self._app

            try:
                await app.bot.delete_webhook(drop_pending_updates=True)
            except Exception as exc:
                log.warning("delete_webhook: %s", exc)

            conv = ConversationHandler(
                entry_points=[
                    CommandHandler("start", self._cmd_start),
                    CommandHandler("menu", self._cmd_menu),
                    CallbackQueryHandler(self._handle_callback),
                ],
                states={
                    STATE_IDLE: [
                        CallbackQueryHandler(self._handle_callback),
                    ],
                    STATE_AWAIT_RAILWAY_SELECT: [
                        CallbackQueryHandler(self._recv_railway_select, pattern="^creds_r[1-4]$"),
                        CallbackQueryHandler(self._handle_callback),
                    ],
                    STATE_AWAIT_ACCOUNT_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_account_count)],
                    STATE_AWAIT_CREDS:         [MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_creds)],
                    STATE_AWAIT_PROXY:         [MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_proxy)],
                    STATE_AWAIT_FEE_COOKIE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_fee_cookie)],
                    STATE_AWAIT_MAX_FEE:       [MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_max_fee)],
                    STATE_AWAIT_DELETE_CONFIRM:[MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_delete_confirm)],
                    STATE_AWAIT_VOLUME_CONFIRM:[MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_volume_confirm)],
                },
                fallbacks=[
                    CommandHandler("cancel", self._cmd_cancel),
                    CommandHandler("menu", self._cmd_menu),
                    CommandHandler("start", self._cmd_start),
                ],
                per_chat=True,
                allow_reentry=True,
            )

            app.add_handler(conv)
            app.add_handler(CommandHandler("status", self._cmd_status))
            app.add_handler(CommandHandler("summary", self._cmd_summary))

            await app.initialize()
            await app.start()
            await app.updater.start_polling(
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES,
            )

            me = await app.bot.get_me()
            log.info("Telegram bot aktif: @%s (id=%s)", me.username, me.id)

        except Exception as exc:
            log.error("GAGAL START TELEGRAM BOT: %s", exc, exc_info=True)
            raise

    async def stop(self) -> None:
        if self._app:
            try:
                await self._app.updater.stop()
                await self._app.stop()
                await self._app.shutdown()
            except Exception as exc:
                log.warning("Error saat stop bot: %s", exc)

    async def broadcast(self, text: str) -> None:
        if not self._app:
            return
        for chat_id in list(self._subscriber_ids):
            try:
                await self._app.bot.send_message(chat_id=chat_id, text=text)
            except Exception as exc:
                log.debug("Gagal kirim ke %s: %s", chat_id, exc)

    async def send_to_admin(self, text: str) -> None:
        if not self._app:
            return
        for uid in ADMIN_IDS:
            try:
                await self._app.bot.send_message(chat_id=uid, text=text)
            except Exception as exc:
                log.warning("Gagal kirim ke admin %s: %s", uid, exc)

    # ── Commands ───────────────────────────────────────────────────────

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        self._subscriber_ids.add(update.effective_user.id)
        if _is_admin(update):
            await self._send_main_menu(update.message.reply_text)
        else:
            await update.message.reply_text(
                f"{_lux_title('Cantex Swap System')}\n\n"
                f"{_lux_field('Access Level', 'OBSERVER')}\n\n"
                "You will receive execution summaries\n"
                "automatically when trades complete."
            )
        return STATE_IDLE

    async def _cmd_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not _is_admin(update):
            return STATE_IDLE
        src = update.message or (update.callback_query and update.callback_query.message)
        if src:
            await self._send_main_menu(src.reply_text)
        return STATE_IDLE

    async def _cmd_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        self._pending_input.pop(update.effective_user.id, None)
        await update.message.reply_text(
            f"{_lux_title('Cancelled')}\n\n"
            "Operation cancelled.\n"
            "Use /menu to continue."
        )
        return STATE_IDLE

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not _is_admin(update):
            return STATE_IDLE
        await update.message.reply_text(await self._build_status_text())
        return STATE_IDLE

    async def _cmd_summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        await update.message.reply_text(await self._build_summary_text())
        return STATE_IDLE

    # ── Helpers ────────────────────────────────────────────────────────

    async def _send_main_menu(self, reply_fn) -> None:
        accounts = CredentialStore.load_all()
        has_proxy = any(a.proxy for a in accounts)
        state = BotStateStore.load()
        cfg = FeeWatcherStore.load()

        bot_status = "RUNNING" if state.running else "STOPPED"
        if state.running and state.started_by:
            bot_status += f"  ({state.started_by})"

        executor_urls = _get_executor_urls()
        railways_str = f"1 + {len(executor_urls)} executor" if executor_urls else "1 (local only)"

        text = (
            f"{_lux_title('Cantex Swap System')}\n\n"
            f"{_lux_field('Bot Status', bot_status)}\n"
            f"{_lux_field('Accounts', f'{len(accounts)} loaded')}\n"
            f"{_lux_field('Max Fee', f'{cfg.max_fee_cc} CC')}\n"
            f"{_lux_field('Fee Cookie', 'configured' if cfg.cantex_cookie else 'not set')}\n"
            f"{_lux_field('Railways', railways_str)}\n\n"
            "  Select an action"
        )
        await reply_fn(
            text,
            reply_markup=_main_menu_keyboard(bool(accounts), has_proxy, state.running),
        )

    async def _notify_others(self, acting_uid: int, message: str) -> None:
        if not self._app:
            return
        for uid in ADMIN_IDS:
            if uid == acting_uid:
                continue
            try:
                await self._app.bot.send_message(
                    chat_id=uid,
                    text=f"{_lux_title('Admin Notification')}\n\n{message}",
                )
            except Exception as exc:
                log.debug("Gagal notify admin %s: %s", uid, exc)

    # ── Callback dispatcher ────────────────────────────────────────────

    async def _handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        await query.answer()

        if not _is_admin(update):
            await query.message.reply_text(
                f"{_lux_title('Access Denied')}\n\nYou do not have admin access."
            )
            return STATE_IDLE

        data = query.data
        uid  = update.effective_user.id
        name = _admin_name(update)

        # ── Back to menu ──
        if data == "back_menu":
            await self._send_main_menu(query.message.reply_text)
            return STATE_IDLE

        # ── Railway selector buttons (creds_r1..creds_r4) ──
        # Bisa masuk sini kalau state hilang (bot restart) tapi user klik tombol lama.
        # query.answer() sudah dipanggil di atas, jadi langsung proses.
        if data in ("creds_r1", "creds_r2", "creds_r3", "creds_r4"):
            railway_num = int(data[-1])  # 1,2,3,4
            if railway_num >= 2:
                urls = _get_executor_urls()
                url_idx = railway_num - 2
                if url_idx >= len(urls):
                    await query.message.reply_text(
                        f"{_lux_title('Railway Not Configured')}\n\n"
                        f"  Railway {railway_num} URL not found in EXECUTOR_URLS.\n\n"
                        "  Add the URL to EXECUTOR_URLS env variable\n"
                        "  in Railway #1 settings, then retry.\n\n"
                        "  Format: https://r2.railway.app,https://r3.railway.app"
                    )
                    return STATE_AWAIT_RAILWAY_SELECT
            self._pending_input[uid] = {
                "action": "creds",
                "railway": railway_num,
                "total": None,
                "current": 1,
                "collected": [],
            }
            label = _railway_label(railway_num)
            await query.message.reply_text(
                f"{_lux_title(f'Railway {railway_num} Credentials')}\n\n"
                f"{_lux_field('Target', label)}\n\n"
                "  How many accounts do you want to configure?\n"
                "  Enter a number between 1 and 50.\n\n"
                "  /cancel to abort"
            )
            return STATE_AWAIT_ACCOUNT_COUNT

        # ── Input Credentials → tampilkan railway selector ──
        if data == "input_creds":
            urls = _get_executor_urls()
            conf_count = len(urls)
            conf_str = f"{conf_count} executor URL{'s' if conf_count != 1 else ''} configured" if conf_count else "no executor URLs (EXECUTOR_URLS env)"
            await query.message.reply_text(
                f"{_lux_title('Input Credentials')}\n\n"
                f"{_lux_field('Executor URLs', conf_str)}\n\n"
                "  Select target Railway\n\n"
                "  Railway 1 = this service (local)\n"
                "  Railway 2-4 = executor services\n\n"
                "  /cancel to abort",
                reply_markup=_railway_select_keyboard(),
            )
            return STATE_AWAIT_RAILWAY_SELECT

        # ── Start Bot ──
        elif data == "start_bot":
            accounts = CredentialStore.load_all()
            if not accounts:
                await query.message.reply_text(
                    f"{_lux_title('Cannot Start')}\n\n"
                    "No accounts configured.\n"
                    "Please input credentials first."
                )
                return STATE_IDLE
            now = datetime.now(timezone.utc).isoformat()
            BotStateStore.save(BotState(running=True, started_by=name, started_at=now))
            await query.message.reply_text(
                f"{_lux_title('Bot Started')}\n\n"
                f"{_lux_field('Started by', name)}\n"
                f"{_lux_field('Accounts', str(len(accounts)))}\n"
                f"{_lux_field('Time UTC', now[:19].replace('T', ' '))}\n\n"
                "  Bot is active and monitoring gas fees."
            )
            await self._notify_others(uid, f"Bot STARTED by {name}.")
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("start_bot"))
            return STATE_IDLE

        # ── Stop Bot ──
        elif data == "stop_bot":
            state = BotStateStore.load()
            now = datetime.now(timezone.utc).isoformat()
            state.running = False
            state.stopped_by = name
            state.stopped_at = now
            BotStateStore.save(state)
            await query.message.reply_text(
                f"{_lux_title('Bot Stopped')}\n\n"
                f"{_lux_field('Stopped by', name)}\n"
                f"{_lux_field('Time UTC', now[:19].replace('T', ' '))}\n\n"
                "  Execution halted.\n"
                "  Use Start Bot to resume."
            )
            await self._notify_others(uid, f"Bot STOPPED by {name}.")
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("stop_bot"))
            return STATE_IDLE

        # ── Set Proxy ──
        elif data == "set_proxy":
            accounts = CredentialStore.load_all()
            if not accounts:
                await query.message.reply_text(
                    f"{_lux_title('No Accounts')}\n\n"
                    "Input credentials before setting proxy."
                )
                return STATE_IDLE
            self._pending_input[uid] = {"action": "proxy", "accounts": accounts, "index": 0}
            acct = accounts[0]
            cur = f"\n{_lux_field('Current', acct.proxy)}" if acct.proxy else ""
            await query.message.reply_text(
                f"{_lux_title('Set Proxy')}\n\n"
                f"{_lux_field('Account', acct.name)}{cur}\n\n"
                "  Format: username:password@ip:port\n"
                "  Type skip to leave unchanged.\n\n"
                "  /cancel to abort"
            )
            return STATE_AWAIT_PROXY

        # ── Set Fee Cookie ──
        elif data == "set_fee_cookie":
            cfg = FeeWatcherStore.load()
            cur = f"\n{_lux_field('Current', cfg.cantex_cookie[:30] + '...')}" if cfg.cantex_cookie else ""
            await query.message.reply_text(
                f"{_lux_title('Set Fee Cookie')}\n\n"
                "  Paste the Cantex cookie value below.\n\n"
                "  How to retrieve:\n"
                "  1. Open cantex.io in browser\n"
                "  2. DevTools (F12) > Network tab\n"
                f"  3. Any request > copy cookie header{cur}\n\n"
                "  /cancel to abort"
            )
            return STATE_AWAIT_FEE_COOKIE

        # ── Set Max Fee ──
        elif data == "set_max_fee":
            cfg = FeeWatcherStore.load()
            await query.message.reply_text(
                f"{_lux_title('Set Max Fee')}\n\n"
                f"{_lux_field('Current limit', f'{cfg.max_fee_cc} CC')}\n\n"
                "  Enter the max gas fee threshold in CC.\n"
                "  Execution triggers when fee is at or below this.\n\n"
                "  Example: 0.27\n\n"
                "  /cancel to abort"
            )
            return STATE_AWAIT_MAX_FEE

        # ── Status ──
        elif data == "status":
            await query.message.reply_text(await self._build_status_text())
            return STATE_IDLE

        # ── Summary ──
        elif data == "summary":
            await query.message.reply_text(await self._build_summary_text())
            return STATE_IDLE

        # ── Delete Accounts ──
        elif data == "delete_accounts":
            await query.message.reply_text(
                f"{_lux_title('Delete All Accounts')}\n\n"
                "  This permanently removes all stored credentials.\n\n"
                "  Type CONFIRM to proceed, or /cancel to abort."
            )
            return STATE_AWAIT_DELETE_CONFIRM

        # ── Delete Volume ──
        elif data == "delete_volume":
            urls = _get_executor_urls()
            scope = f"Railway 1 (local) + {len(urls)} executor(s)" if urls else "Railway 1 (local only)"
            await query.message.reply_text(
                f"{_lux_title('Delete All Volume Data')}\n\n"
                f"{_lux_field('Scope', scope)}\n\n"
                "  WARNING — this erases ALL volume data:\n"
                "  credentials, fee config, progress,\n"
                "  bot state, and signal files.\n\n"
                "  This action is irreversible.\n\n"
                "  Type CONFIRM to proceed, or /cancel to abort."
            )
            return STATE_AWAIT_VOLUME_CONFIRM

        return STATE_IDLE

    # ── Railway selector ───────────────────────────────────────────────

    async def _recv_railway_select(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        await query.answer()

        data = query.data  # creds_r1 / creds_r2 / creds_r3 / creds_r4
        railway_num = int(data[-1])  # 1,2,3,4
        uid = update.effective_user.id

        # Validasi: railway 2-4 butuh URL yang dikonfigurasi
        if railway_num >= 2:
            urls = _get_executor_urls()
            url_idx = railway_num - 2
            if url_idx >= len(urls):
                await query.message.reply_text(
                    f"{_lux_title('Railway Not Configured')}\n\n"
                    f"  Railway {railway_num} URL not found in EXECUTOR_URLS.\n\n"
                    "  Add the URL to EXECUTOR_URLS env variable\n"
                    "  in Railway #1 settings, then retry.\n\n"
                    "  Format: https://r2.railway.app,https://r3.railway.app"
                )
                return STATE_AWAIT_RAILWAY_SELECT

        self._pending_input[uid] = {
            "action": "creds",
            "railway": railway_num,
            "total": None,
            "current": 1,
            "collected": [],
        }

        label = _railway_label(railway_num)
        await query.message.reply_text(
            f"{_lux_title(f'Railway {railway_num} Credentials')}\n\n"
            f"{_lux_field('Target', label)}\n\n"
            "  How many accounts do you want to configure?\n"
            "  Enter a number between 1 and 50.\n\n"
            "  /cancel to abort"
        )
        return STATE_AWAIT_ACCOUNT_COUNT

    # ── Input Receivers ────────────────────────────────────────────────

    async def _recv_account_count(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        state = self._pending_input.get(uid, {})
        if state.get("action") != "creds":
            return STATE_IDLE

        try:
            count = int(update.message.text.strip())
            if not 1 <= count <= 50:
                raise ValueError
        except ValueError:
            await update.message.reply_text("Invalid. Enter a number between 1 and 50.")
            return STATE_AWAIT_ACCOUNT_COUNT

        state["total"] = count
        railway_num = state.get("railway", 1)
        await update.message.reply_text(
            f"{_lux_title(f'Account 1 of {count}')}\n\n"
            f"{_lux_field('Railway', str(railway_num))}\n\n"
            "  Paste credentials (two lines):\n\n"
            "  Line 1: operator_key\n"
            "  Line 2: trading_key\n\n"
            "  /cancel to abort"
        )
        return STATE_AWAIT_CREDS

    async def _recv_creds(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        state = self._pending_input.get(uid, {})
        if state.get("action") != "creds":
            return STATE_IDLE

        lines = [l.strip() for l in update.message.text.strip().splitlines() if l.strip()]
        if len(lines) < 2:
            await update.message.reply_text(
                "Invalid format. Provide two lines:\n"
                "  Line 1: operator_key\n"
                "  Line 2: trading_key"
            )
            return STATE_AWAIT_CREDS

        idx   = state["current"]
        total = state["total"]
        cred  = AccountCredential(
            name=f"account_{idx}",
            operator_key=lines[0],
            trading_key=lines[1],
        )
        state["collected"].append(cred)
        state["current"] += 1

        if state["current"] > total:
            # Semua credentials terkumpul — kirim ke Railway yang dipilih
            railway_num = state.get("railway", 1)
            collected   = state["collected"]

            if railway_num == 1:
                # Simpan lokal
                for c in collected:
                    CredentialStore.save(c)
                result_str = f"Saved locally ({len(collected)} accounts)"
            else:
                # Kirim ke executor Railway via HTTP
                urls = _get_executor_urls()
                url  = urls[railway_num - 2]
                res  = await push_credentials_to_url(url, collected, replace=True)
                if res.get("ok"):
                    result_str = f"Sent to Railway {railway_num} ({res.get('saved', len(collected))} accounts)"
                else:
                    result_str = f"FAILED sending to Railway {railway_num}: {res.get('error', 'unknown error')}"
                    log.warning("Gagal kirim credentials ke Railway %d: %s", railway_num, res)

            self._pending_input.pop(uid, None)
            names = ", ".join(c.name for c in collected)
            await update.message.reply_text(
                f"{_lux_title('Credentials Saved')}\n\n"
                f"{_lux_field('Result', result_str)}\n"
                f"{_lux_field('Accounts', str(len(collected)))}\n"
                f"{_lux_field('Names', names[:60])}\n\n"
                "  Use /menu to continue."
            )
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("credentials"))
            return STATE_IDLE
        else:
            railway_num = state.get("railway", 1)
            await update.message.reply_text(
                f"{_lux_title(f'Account {idx} Saved')}\n\n"
                f"{_lux_field('Progress', f'{idx} / {total}')}\n"
                f"{_lux_field('Railway', str(railway_num))}\n\n"
                f"  Now enter credentials for Account {state['current']}:\n\n"
                "  Line 1: operator_key\n"
                "  Line 2: trading_key\n\n"
                "  /cancel to abort"
            )
            return STATE_AWAIT_CREDS

    async def _recv_proxy(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        state = self._pending_input.get(uid, {})
        if state.get("action") != "proxy":
            return STATE_IDLE

        text     = update.message.text.strip()
        accounts = state["accounts"]
        index    = state["index"]
        acct     = accounts[index]

        if text.lower() != "skip":
            if "@" not in text or ":" not in text.split("@")[0]:
                await update.message.reply_text(
                    "Invalid format. Required: username:password@ip:port\n"
                    "Type skip to leave unchanged."
                )
                return STATE_AWAIT_PROXY
            CredentialStore.save(AccountCredential(
                name=acct.name,
                operator_key=acct.operator_key,
                trading_key=acct.trading_key,
                proxy=text,
            ))
            await update.message.reply_text(f"Proxy saved for '{acct.name}'.")
        else:
            await update.message.reply_text(f"Proxy skipped for '{acct.name}'.")

        next_idx = index + 1
        if next_idx >= len(accounts):
            self._pending_input.pop(uid, None)
            await update.message.reply_text(
                f"{_lux_title('Proxy Configuration Complete')}\n\n"
                "  All accounts processed.\n"
                "  Use /menu to continue."
            )
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("proxy"))
            return STATE_IDLE
        else:
            state["index"] = next_idx
            nxt = accounts[next_idx]
            cur = f"\n{_lux_field('Current', nxt.proxy)}" if nxt.proxy else ""
            await update.message.reply_text(
                f"{_lux_title(f'Account: {nxt.name}')}\n\n"
                f"{_lux_field('Account', nxt.name)}{cur}\n\n"
                "  Format: username:password@ip:port\n"
                "  Type skip to leave unchanged.\n\n"
                "  /cancel to abort"
            )
            return STATE_AWAIT_PROXY

    async def _recv_fee_cookie(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid    = update.effective_user.id
        cookie = update.message.text.strip()
        if len(cookie) < 10:
            await update.message.reply_text("Cookie appears too short. Try again or /cancel.")
            return STATE_AWAIT_FEE_COOKIE

        cfg = FeeWatcherStore.load()
        cfg.cantex_cookie = cookie
        FeeWatcherStore.save(cfg)
        self._pending_input.pop(uid, None)

        try:
            await update.message.delete()
        except Exception:
            pass

        await update.effective_chat.send_message(
            f"{_lux_title('Fee Cookie Saved')}\n\n"
            "  Cookie stored securely.\n"
            "  Use /menu to continue."
        )
        if self._on_config_updated:
            asyncio.create_task(self._on_config_updated("fee_cookie"))
        return STATE_IDLE

    async def _recv_max_fee(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid  = update.effective_user.id
        text = update.message.text.strip()
        try:
            val = float(text)
            if not 0 < val <= 100:
                raise ValueError
        except ValueError:
            await update.message.reply_text("Invalid. Enter a positive number, e.g. 0.27")
            return STATE_AWAIT_MAX_FEE

        cfg = FeeWatcherStore.load()
        cfg.max_fee_cc = val
        FeeWatcherStore.save(cfg)
        self._pending_input.pop(uid, None)
        await update.message.reply_text(
            f"{_lux_title('Max Fee Updated')}\n\n"
            f"{_lux_field('New limit', f'{val} CC')}\n\n"
            "  Execution triggers when fee is at or below this.\n"
            "  Use /menu to continue."
        )
        if self._on_config_updated:
            asyncio.create_task(self._on_config_updated("max_fee"))
        return STATE_IDLE

    async def _recv_delete_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid  = update.effective_user.id
        name = _admin_name(update)
        if update.message.text.strip() == "CONFIRM":
            CredentialStore.clear_all()
            now   = datetime.now(timezone.utc).isoformat()
            state = BotStateStore.load()
            state.running = False
            state.stopped_by = name
            state.stopped_at = now
            BotStateStore.save(state)
            await update.message.reply_text(
                f"{_lux_title('All Accounts Deleted')}\n\n"
                "  Credentials removed. Bot stopped.\n"
                "  Use /menu to reconfigure."
            )
            await self._notify_others(uid, f"All accounts DELETED by {name}. Bot stopped.")
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("delete"))
        else:
            await update.message.reply_text(
                f"{_lux_title('Cancelled')}\n\n"
                "  No accounts deleted.\n"
                "  Use /menu to return."
            )
        self._pending_input.pop(uid, None)
        return STATE_IDLE

    async def _recv_volume_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid  = update.effective_user.id
        name = _admin_name(update)
        if update.message.text.strip() == "CONFIRM":
            # Hapus lokal (Railway 1)
            clear_all_volume()

            # Broadcast ke Railway 2-4
            remote_results = await broadcast_clear_volume()

            # Susun laporan
            lines = [
                f"{_lux_title('Volume Cleared')}\n",
                f"{_lux_field('Railway 1', 'cleared (local)')}",
            ]
            urls = _get_executor_urls()
            for i, url in enumerate(urls, start=2):
                res = remote_results.get(url, {})
                status = "cleared" if res.get("ok") else f"FAILED: {res.get('error', 'no response')[:40]}"
                lines.append(_lux_field(f"Railway {i}", status))

            lines.append("\n  All data permanently erased.")
            lines.append("  Use /menu to start fresh.")

            await update.message.reply_text("\n".join(lines))
            await self._notify_others(uid, f"ALL VOLUME DATA erased by {name}.")
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("delete_volume"))
        else:
            await update.message.reply_text(
                f"{_lux_title('Cancelled')}\n\n"
                "  Volume data unchanged.\n"
                "  Use /menu to return."
            )
        self._pending_input.pop(uid, None)
        return STATE_IDLE

    # ── Status & Summary ───────────────────────────────────────────────

    async def _build_status_text(self) -> str:
        accounts  = CredentialStore.load_all()
        cfg       = FeeWatcherStore.load()
        bot_state = BotStateStore.load()
        today     = datetime.now(timezone.utc).date().isoformat()
        progress  = ProgressStore.load_all_today(today)

        status_str = "RUNNING" if bot_state.running else "STOPPED"
        if bot_state.running and bot_state.started_by:
            status_str += f"  ({bot_state.started_by})"

        urls = _get_executor_urls()
        railways_str = f"1 + {len(urls)} executor" if urls else "1"

        lines = [
            f"{_lux_title(f'System Status — {today}')}\n",
            _lux_field("Bot", status_str),
            _lux_field("Railways", railways_str),
            _lux_field("Accounts", str(len(accounts))),
            _lux_field("Max Fee", f"{cfg.max_fee_cc} CC"),
            _lux_field("Fee Cookie", "configured" if cfg.cantex_cookie else "not set"),
            "",
            "  Account Progress\n",
        ]

        if not accounts:
            lines.append("  No accounts configured.")
        for acct in accounts:
            prog      = progress.get(acct.name)
            completed = prog.completed_tx if prog else 0
            proxy_str = acct.proxy.split("@")[-1] if acct.proxy else "none"
            lines.append(f"  {acct.name:<18}  {completed}/6 tx   proxy: {proxy_str}")

        return "\n".join(lines)

    async def _build_summary_text(self) -> str:
        today     = datetime.now(timezone.utc).date().isoformat()
        progress  = ProgressStore.load_all_today(today)
        accounts  = CredentialStore.load_all()
        total_tx  = total_ok = total_fail = 0

        lines = [f"{_lux_title(f'Execution Summary — {today}')}\n"]

        for acct in accounts:
            prog = progress.get(acct.name)
            if not prog or not prog.tx_log:
                lines.append(f"  {acct.name}   ·   no transactions yet")
                continue
            lines.append(f"  {acct.name}  ({prog.completed_tx} tx):")
            for tx in prog.tx_log:
                ok     = tx.get("success")
                status = "OK" if ok else "FAIL"
                pair   = tx.get("pair", "?")
                sell   = tx.get("sell_amount", "?")
                recv   = tx.get("received_amount", "?")
                fee    = tx.get("fee_cc", "?")
                err    = f"   err: {tx['error'][:40]}" if tx.get("error") else ""
                lines.append(
                    f"    TX{tx['tx_index']+1}  {pair}  "
                    f"sell={sell}  recv={recv}  fee={fee} CC  [{status}]{err}"
                )
                total_tx += 1
                total_ok += 1 if ok else 0
                total_fail += 0 if ok else 1
            lines.append("")

        lines += [
            f"  Total    ·  {total_tx} tx",
            f"  Success  ·  {total_ok}",
            f"  Failed   ·  {total_fail}",
        ]
        return "\n".join(lines)
