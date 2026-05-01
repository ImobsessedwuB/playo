from __future__ import annotations
import json

"""
Telegram bot control panel — Cantex Swap System
Admin IDs: 6469077855, 1118770958

Env vars:
  EXECUTOR_URLS   : comma-separated URLs untuk Railway #2, #3, #4
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
    broadcast_bot_state,
    broadcast_clear_credentials,
    broadcast_clear_volume,
    fetch_all_executor_account_counts,
    fetch_all_executor_progress,
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

# Back button — reusable
_BTN_BACK = InlineKeyboardButton("Back", callback_data="back_menu")
_KB_BACK  = InlineKeyboardMarkup([[_BTN_BACK]])


def _btn(text: str, cb: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text, callback_data=cb)


def _raw_btn(text: str, cb: str, style: str | None = None) -> dict:
    """Raw dict button untuk pakai style/color via Telegram API langsung."""
    b = {"text": text, "callback_data": cb}
    if style:
        b["style"] = style
    return b


def _raw_keyboard(rows: list[list[dict]]) -> str:
    """Serialize raw keyboard ke JSON string untuk api_kwargs."""
    return json.dumps({"inline_keyboard": rows})


def _field(key: str, val: str, w: int = 14) -> str:
    return f"  {key:<{w}}:  {val}"


def _is_admin(update: Update) -> bool:
    return update.effective_user is not None and update.effective_user.id in ADMIN_IDS


def _admin_name(update: Update) -> str:
    u = update.effective_user
    if u is None:
        return "Unknown"
    return u.full_name or u.username or str(u.id)


def _railway_label(idx: int) -> str:
    if idx == 1:
        return "Railway 1 (local)"
    urls = _get_executor_urls()
    url_idx = idx - 2
    if url_idx < len(urls):
        return f"Railway {idx} ({urls[url_idx].split('//')[-1][:30]})"
    return f"Railway {idx} (not configured)"


# ---------------------------------------------------------------------------
# Main menu keyboard
# ---------------------------------------------------------------------------

def _main_menu_keyboard(has_accounts: bool, bot_running: bool) -> str:
    """
    Return raw JSON string keyboard.
    Start Bot = hijau (positive), Stop Bot = merah (destructive).
    Dikirim via api_kwargs={"reply_markup": ...} bukan InlineKeyboardMarkup.
    """
    rows = []
    rows.append([_raw_btn("Input Credentials", "input_creds")])

    if has_accounts:
        if bot_running:
            rows.append([_raw_btn("Stop Bot", "stop_bot", "danger")])
        else:
            rows.append([_raw_btn("Start Bot", "start_bot", "success")])

    rows.append([_raw_btn("Set Fee Cookie", "set_fee_cookie")])
    rows.append([_raw_btn("Set Max Fee (CC)", "set_max_fee")])
    rows.append([
        _raw_btn("Status", "status"),
        _raw_btn("Summary", "summary"),
    ])

    if has_accounts:
        rows.append([_raw_btn("Delete All Accounts", "delete_accounts", "danger")])
    rows.append([_raw_btn("Delete All Volume Data", "delete_volume", "danger")])

    return _raw_keyboard(rows)


def _railway_select_keyboard() -> InlineKeyboardMarkup:
    urls = _get_executor_urls()
    rows: list[list[InlineKeyboardButton]] = []

    r1 = InlineKeyboardButton("Railway 1", callback_data="creds_r1")
    r2_label = "Railway 2" if len(urls) >= 1 else "Railway 2 (not set)"
    r2 = InlineKeyboardButton(r2_label, callback_data="creds_r2")
    rows.append([r1, r2])

    r3_label = "Railway 3" if len(urls) >= 2 else "Railway 3 (not set)"
    r4_label = "Railway 4" if len(urls) >= 3 else "Railway 4 (not set)"
    r3 = InlineKeyboardButton(r3_label, callback_data="creds_r3")
    r4 = InlineKeyboardButton(r4_label, callback_data="creds_r4")
    rows.append([r3, r4])

    rows.append([_BTN_BACK])
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

            back_handler = CallbackQueryHandler(self._handle_callback, pattern="^back_menu$")

            conv = ConversationHandler(
                entry_points=[
                    CommandHandler("start", self._cmd_start),
                    CommandHandler("menu", self._cmd_menu),
                    CallbackQueryHandler(self._handle_callback),
                ],
                states={
                    STATE_IDLE: [CallbackQueryHandler(self._handle_callback)],
                    STATE_AWAIT_RAILWAY_SELECT: [
                        CallbackQueryHandler(self._recv_railway_select, pattern="^creds_r[1-4]$"),
                        back_handler,
                    ],
                    STATE_AWAIT_ACCOUNT_COUNT: [
                        MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_account_count),
                        back_handler,
                    ],
                    STATE_AWAIT_CREDS: [
                        MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_creds),
                        back_handler,
                    ],
                    STATE_AWAIT_FEE_COOKIE: [
                        MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_fee_cookie),
                        back_handler,
                    ],
                    STATE_AWAIT_MAX_FEE: [
                        MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_max_fee),
                        back_handler,
                    ],
                    STATE_AWAIT_DELETE_CONFIRM: [
                        MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_delete_confirm),
                        back_handler,
                    ],
                    STATE_AWAIT_VOLUME_CONFIRM: [
                        MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_volume_confirm),
                        back_handler,
                    ],
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
            await self._show_menu(send_fn=update.message.reply_text)
        else:
            await update.message.reply_text(
                "CANTEX SWAP SYSTEM\n\n"
                "  Access Level  :  Observer\n\n"
                "  You will receive execution summaries\n"
                "  automatically when trades complete."
            )
        return STATE_IDLE

    async def _cmd_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not _is_admin(update):
            return STATE_IDLE
        src = update.message or (update.callback_query and update.callback_query.message)
        if src:
            await self._show_menu(send_fn=src.reply_text)
        return STATE_IDLE

    async def _cmd_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        self._pending_input.pop(update.effective_user.id, None)
        await update.message.reply_text(
            "CANCELLED\n\n  Operation cancelled.\n  Use /menu to continue."
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

    # ── Menu helper ────────────────────────────────────────────────────

    async def _show_menu(self, send_fn=None, edit_msg=None) -> None:
        local_accounts = CredentialStore.load_all()
        state = BotStateStore.load()
        cfg = FeeWatcherStore.load()

        bot_status = "RUNNING" if state.running else "STOPPED"
        if state.running and state.started_by:
            bot_status += f"  ({state.started_by})"

        executor_urls = _get_executor_urls()
        remote_count = await fetch_all_executor_account_counts()
        total_accounts = len(local_accounts) + remote_count
        railways_str = f"1 + {len(executor_urls)} executor" if executor_urls else "1 (local only)"

        text = (
            "CANTEX SWAP SYSTEM\n\n"
            f"{_field('Bot Status', bot_status)}\n"
            f"{_field('Accounts', f'{total_accounts} loaded')}\n"
            f"{_field('Max Fee', f'{cfg.max_fee_cc} CC')}\n"
            f"{_field('Fee Cookie', 'configured' if cfg.cantex_cookie else 'not set')}\n"
            f"{_field('Railways', railways_str)}\n\n"
            "  Select an action"
        )
        markup = _main_menu_keyboard(total_accounts > 0, state.running)  # raw JSON string

        if edit_msg:
            try:
                await edit_msg.edit_text(text, api_kwargs={"reply_markup": markup})
                return
            except Exception:
                pass
        if send_fn:
            await send_fn(text, api_kwargs={"reply_markup": markup})

    async def _notify_others(self, acting_uid: int, message: str) -> None:
        if not self._app:
            return
        for uid in ADMIN_IDS:
            if uid == acting_uid:
                continue
            try:
                await self._app.bot.send_message(chat_id=uid, text=f"ADMIN NOTIFICATION\n\n{message}")
            except Exception as exc:
                log.debug("Gagal notify admin %s: %s", uid, exc)

    # ── Callback dispatcher ────────────────────────────────────────────

    async def _handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        await query.answer()

        if not _is_admin(update):
            await query.message.reply_text("ACCESS DENIED\n\nYou do not have admin access.")
            return STATE_IDLE

        data = query.data
        uid  = update.effective_user.id
        name = _admin_name(update)

        # ── Back to menu ──
        if data == "back_menu":
            self._pending_input.pop(uid, None)
            await self._show_menu(edit_msg=query.message)
            return STATE_IDLE

        # ── Railway selector buttons (creds_r1..creds_r4) ──
        if data in ("creds_r1", "creds_r2", "creds_r3", "creds_r4"):
            railway_num = int(data[-1])
            if railway_num >= 2:
                urls = _get_executor_urls()
                url_idx = railway_num - 2
                if url_idx >= len(urls):
                    await query.edit_message_text(
                        f"RAILWAY NOT CONFIGURED\n\n"
                        f"  Railway {railway_num} URL not found in EXECUTOR_URLS.\n\n"
                        "  Add the URL to EXECUTOR_URLS env variable in Railway 1 settings.\n\n"
                        "  Format:\n  https://r2.railway.app,https://r3.railway.app",
                        reply_markup=_KB_BACK,
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
            await query.edit_message_text(
                f"RAILWAY {railway_num} — CREDENTIALS\n\n"
                f"{_field('Target', label)}\n\n"
                "  How many accounts do you want to configure?\n"
                "  Enter a number (1 to 50).\n\n"
                "  /cancel to abort",
                reply_markup=_KB_BACK,
            )
            return STATE_AWAIT_ACCOUNT_COUNT

        # ── Input Credentials ──
        if data == "input_creds":
            urls = _get_executor_urls()
            conf_str = (
                f"{len(urls)} executor URL(s) configured"
                if urls else "no executor URLs (set EXECUTOR_URLS env)"
            )
            await query.edit_message_text(
                "INPUT CREDENTIALS\n\n"
                f"{_field('Executor URLs', conf_str)}\n\n"
                "  Select target Railway.\n\n"
                "  Railway 1 = this service (local)\n"
                "  Railway 2-4 = executor services",
                reply_markup=_railway_select_keyboard(),
            )
            return STATE_AWAIT_RAILWAY_SELECT

        # ── Start Bot ──
        elif data == "start_bot":
            accounts = CredentialStore.load_all()
            remote_count = await fetch_all_executor_account_counts()
            total_accounts = len(accounts) + remote_count
            if total_accounts == 0:
                await query.edit_message_text(
                    "CANNOT START\n\n"
                    "  No accounts configured.\n"
                    "  Please input credentials first.",
                    reply_markup=_KB_BACK,
                )
                return STATE_IDLE
            now = datetime.now(timezone.utc).isoformat()
            BotStateStore.save(BotState(running=True, started_by=name, started_at=now))
            # FIX: broadcast ke semua executor Railways
            asyncio.create_task(broadcast_bot_state(True))
            await query.edit_message_text(
                "BOT STARTED\n\n"
                f"{_field('Started by', name)}\n"
                f"{_field('Accounts', str(total_accounts))}\n"
                f"{_field('Time UTC', now[:19].replace('T', ' '))}\n\n"
                "  Bot is active and monitoring gas fees.",
                reply_markup=_KB_BACK,
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
            # FIX: broadcast ke semua executor Railways
            asyncio.create_task(broadcast_bot_state(False))
            await query.edit_message_text(
                "BOT STOPPED\n\n"
                f"{_field('Stopped by', name)}\n"
                f"{_field('Time UTC', now[:19].replace('T', ' '))}\n\n"
                "  Execution halted.\n"
                "  Use Start Bot to resume.",
                reply_markup=_KB_BACK,
            )
            await self._notify_others(uid, f"Bot STOPPED by {name}.")
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("stop_bot"))
            return STATE_IDLE

        # ── Set Fee Cookie ──
        elif data == "set_fee_cookie":
            cfg = FeeWatcherStore.load()
            cur = f"\n{_field('Current', cfg.cantex_cookie[:30] + '...')}" if cfg.cantex_cookie else ""
            await query.edit_message_text(
                "SET FEE COOKIE\n\n"
                "  Paste the Cantex cookie value below.\n\n"
                "  How to get it:\n"
                "  1. Open cantex.io in browser\n"
                "  2. DevTools (F12) > Network tab\n"
                f"  3. Any request > copy cookie header{cur}\n\n"
                "  /cancel to abort",
                reply_markup=_KB_BACK,
            )
            return STATE_AWAIT_FEE_COOKIE

        # ── Set Max Fee ──
        elif data == "set_max_fee":
            cfg = FeeWatcherStore.load()
            await query.edit_message_text(
                "SET MAX FEE\n\n"
                f"{_field('Current limit', f'{cfg.max_fee_cc} CC')}\n\n"
                "  Enter the max gas fee threshold in CC.\n"
                "  Execution triggers when fee is at or below this.\n\n"
                "  Example: 0.27\n\n"
                "  /cancel to abort",
                reply_markup=_KB_BACK,
            )
            return STATE_AWAIT_MAX_FEE

        # ── Status ──
        elif data == "status":
            text = await self._build_status_text()
            await query.edit_message_text(text, reply_markup=_KB_BACK)
            return STATE_IDLE

        # ── Summary ──
        elif data == "summary":
            text = await self._build_summary_text()
            await query.edit_message_text(text, reply_markup=_KB_BACK)
            return STATE_IDLE

        # ── Delete Accounts ──
        elif data == "delete_accounts":
            await query.edit_message_text(
                "DELETE ALL ACCOUNTS\n\n"
                "  This permanently removes all stored credentials\n"
                "  from Railway 1 AND all executor Railways.\n\n"
                "  Type CONFIRM to proceed, or /cancel to abort.",
                reply_markup=_KB_BACK,
            )
            return STATE_AWAIT_DELETE_CONFIRM

        # ── Delete Volume ──
        elif data == "delete_volume":
            urls = _get_executor_urls()
            scope = f"Railway 1 + {len(urls)} executor(s)" if urls else "Railway 1 only"
            await query.edit_message_text(
                "DELETE ALL VOLUME DATA\n\n"
                f"{_field('Scope', scope)}\n\n"
                "  WARNING: this erases ALL data permanently:\n"
                "  credentials, fee config, progress,\n"
                "  bot state, and signal files.\n\n"
                "  Type CONFIRM to proceed, or /cancel to abort.",
                reply_markup=_KB_BACK,
            )
            return STATE_AWAIT_VOLUME_CONFIRM

        return STATE_IDLE

    # ── Railway selector ───────────────────────────────────────────────

    async def _recv_railway_select(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        await query.answer()

        data = query.data
        railway_num = int(data[-1])
        uid = update.effective_user.id

        if railway_num >= 2:
            urls = _get_executor_urls()
            url_idx = railway_num - 2
            if url_idx >= len(urls):
                await query.edit_message_text(
                    f"RAILWAY NOT CONFIGURED\n\n"
                    f"  Railway {railway_num} URL not found in EXECUTOR_URLS.\n\n"
                    "  Format:\n  https://r2.railway.app,https://r3.railway.app",
                    reply_markup=_KB_BACK,
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
        await query.edit_message_text(
            f"RAILWAY {railway_num} — CREDENTIALS\n\n"
            f"{_field('Target', label)}\n\n"
            "  How many accounts do you want to configure?\n"
            "  Enter a number (1 to 50).\n\n"
            "  /cancel to abort",
            reply_markup=_KB_BACK,
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

        try:
            await update.message.delete()
        except Exception:
            pass

        await update.effective_chat.send_message(
            f"ACCOUNT 1 OF {count}\n\n"
            f"{_field('Railway', str(railway_num))}\n\n"
            "  Paste credentials (two lines):\n\n"
            "  Line 1: operator_key\n"
            "  Line 2: trading_key\n\n"
            "  /cancel to abort",
            reply_markup=_KB_BACK,
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

        # Delete credentials message for security
        try:
            await update.message.delete()
        except Exception:
            pass

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
            # Semua credentials terkumpul — langsung simpan
            collected   = state["collected"]
            railway_num = state.get("railway", 1)

            if railway_num == 1:
                for c in collected:
                    CredentialStore.save(c)
                result_str = f"Saved locally ({len(collected)} accounts)"
            else:
                urls = _get_executor_urls()
                url  = urls[railway_num - 2]
                res  = await push_credentials_to_url(url, collected, replace=True)
                if res.get("ok"):
                    result_str = f"Sent to Railway {railway_num} ({res.get('saved', len(collected))} accounts)"
                else:
                    result_str = f"FAILED sending to Railway {railway_num}: {res.get('error', 'unknown')[:40]}"
                    log.warning("Gagal kirim credentials ke Railway %d: %s", railway_num, res)

            self._pending_input.pop(uid, None)
            await update.effective_chat.send_message(
                "CREDENTIALS SAVED\n\n"
                f"{_field('Result', result_str)}\n"
                f"{_field('Accounts', str(len(collected)))}\n\n"
                "  Use /menu to continue.",
                reply_markup=_KB_BACK,
            )
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("credentials"))
            return STATE_IDLE
        else:
            railway_num = state.get("railway", 1)
            await update.effective_chat.send_message(
                f"ACCOUNT {idx} SAVED\n\n"
                f"{_field('Progress', f'{idx} / {total}')}\n"
                f"{_field('Railway', str(railway_num))}\n\n"
                f"  Now enter credentials for Account {state['current']}:\n\n"
                "  Line 1: operator_key\n"
                "  Line 2: trading_key\n\n"
                "  /cancel to abort",
                reply_markup=_KB_BACK,
            )
            return STATE_AWAIT_CREDS

        return STATE_IDLE

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

        # Hapus pesan cookie untuk keamanan
        try:
            await update.message.delete()
        except Exception:
            pass

        await update.effective_chat.send_message(
            "FEE COOKIE SAVED\n\n"
            "  Cookie stored securely.",
            reply_markup=_KB_BACK,
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

        try:
            await update.message.delete()
        except Exception:
            pass

        await update.effective_chat.send_message(
            "MAX FEE UPDATED\n\n"
            f"{_field('New limit', f'{val} CC')}\n\n"
            "  Execution triggers when fee is at or below this.",
            reply_markup=_KB_BACK,
        )
        if self._on_config_updated:
            asyncio.create_task(self._on_config_updated("max_fee"))
        return STATE_IDLE

    async def _recv_delete_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid  = update.effective_user.id
        name = _admin_name(update)
        if update.message.text.strip() == "CONFIRM":
            # Hapus Railway 1 lokal
            CredentialStore.clear_all()
            # FIX: broadcast clear ke Railway 2-4
            asyncio.create_task(broadcast_clear_credentials())
            now   = datetime.now(timezone.utc).isoformat()
            state = BotStateStore.load()
            state.running = False
            state.stopped_by = name
            state.stopped_at = now
            BotStateStore.save(state)
            asyncio.create_task(broadcast_bot_state(False))

            try:
                await update.message.delete()
            except Exception:
                pass

            await update.effective_chat.send_message(
                "ALL ACCOUNTS DELETED\n\n"
                "  Credentials removed from all Railways.\n"
                "  Bot stopped.",
                reply_markup=_KB_BACK,
            )
            await self._notify_others(uid, f"All accounts DELETED by {name}. Bot stopped.")
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("delete"))
        else:
            await update.message.reply_text(
                "CANCELLED\n\n  No accounts deleted.",
                reply_markup=_KB_BACK,
            )
        self._pending_input.pop(uid, None)
        return STATE_IDLE

    async def _recv_volume_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid  = update.effective_user.id
        name = _admin_name(update)
        if update.message.text.strip() == "CONFIRM":
            clear_all_volume()
            remote_results = await broadcast_clear_volume()

            try:
                await update.message.delete()
            except Exception:
                pass

            lines = [
                "VOLUME CLEARED\n",
                _field("Railway 1", "cleared (local)"),
            ]
            urls = _get_executor_urls()
            for i, url in enumerate(urls, start=2):
                res = remote_results.get(url, {})
                status = "cleared" if res.get("ok") else f"FAILED: {res.get('error', 'no response')[:30]}"
                lines.append(_field(f"Railway {i}", status))

            lines.append("\n  All data permanently erased.")

            await update.effective_chat.send_message(
                "\n".join(lines),
                reply_markup=_KB_BACK,
            )
            await self._notify_others(uid, f"ALL VOLUME DATA erased by {name}.")
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("delete_volume"))
        else:
            await update.message.reply_text(
                "CANCELLED\n\n  Volume data unchanged.",
                reply_markup=_KB_BACK,
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
            f"SYSTEM STATUS — {today}\n",
            _field("Bot", status_str),
            _field("Railways", railways_str),
            _field("Max Fee", f"{cfg.max_fee_cc} CC"),
            _field("Fee Cookie", "configured" if cfg.cantex_cookie else "not set"),
            "",
            "  RAILWAY 1 (local)\n",
        ]

        if not accounts:
            lines.append("  No accounts configured.")
        for acct in accounts:
            prog    = progress.get(acct.name)
            success = prog.success_tx if prog else 0
            lines.append(f"  {acct.name:<18}  {success}/6 sukses")

        # FIX: Fetch status dari Railway 2-4
        executor_progress = await fetch_all_executor_progress()
        for railway_num, prog_data in executor_progress:
            lines.append("")
            if prog_data and "accounts" in prog_data:
                r_accounts = prog_data["accounts"]
                lines.append(f"  RAILWAY {railway_num}\n")
                if not r_accounts:
                    lines.append("  No accounts configured.")
                for ap in r_accounts:
                    name    = ap["name"]
                    success = ap.get("success_tx", ap.get("completed_tx", 0))
                    lines.append(f"  {name:<18}  {success}/6 sukses")
            else:
                url_idx = railway_num - 2
                from .signal_broker import _get_executor_urls
                urls = _get_executor_urls()
                url_short = urls[url_idx].split("//")[-1][:30] if url_idx < len(urls) else "not configured"
                lines.append(f"  RAILWAY {railway_num} — unreachable ({url_short})")

        return "\n".join(lines)

    async def _build_summary_text(self) -> str:
        today    = datetime.now(timezone.utc).date().isoformat()
        progress = ProgressStore.load_all_today(today)
        accounts = CredentialStore.load_all()
        total_tx = total_ok = total_fail = 0

        lines = [f"EXECUTION SUMMARY — {today}\n", "  RAILWAY 1 (local)\n"]

        for acct in accounts:
            prog = progress.get(acct.name)
            if not prog or not prog.tx_log:
                lines.append(f"  {acct.name}   no transactions yet")
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

        # FIX: Fetch summary dari Railway 2-4
        executor_progress = await fetch_all_executor_progress()
        for railway_num, prog_data in executor_progress:
            lines.append("")
            if prog_data and "accounts" in prog_data:
                lines.append(f"  RAILWAY {railway_num}\n")
                for ap in prog_data["accounts"]:
                    name = ap["name"]
                    tx_log = ap.get("tx_log", [])
                    if not tx_log:
                        lines.append(f"  {name}   no transactions yet")
                        continue
                    completed = ap.get("completed_tx", 0)
                    lines.append(f"  {name}  ({completed} tx):")
                    for tx in tx_log:
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
            else:
                url_idx = railway_num - 2
                from .signal_broker import _get_executor_urls
                urls = _get_executor_urls()
                url_short = urls[url_idx].split("//")[-1][:30] if url_idx < len(urls) else "not configured"
                lines.append(f"  RAILWAY {railway_num} — unreachable ({url_short})")

        lines += [
            f"  Total    :  {total_tx} tx",
            f"  Success  :  {total_ok}",
            f"  Failed   :  {total_fail}",
        ]
        return "\n".join(lines)
