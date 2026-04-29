from __future__ import annotations

"""
Telegram bot control panel — Cantex Swap System
Admin IDs: 6469077855, 1118770958
Both admins share the same credential state via Railway volume.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
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
    STATE_AWAIT_ACCOUNT_COUNT,
    STATE_AWAIT_CREDS,
    STATE_AWAIT_PROXY,
    STATE_AWAIT_FEE_COOKIE,
    STATE_AWAIT_MAX_FEE,
    STATE_AWAIT_DELETE_CONFIRM,
    STATE_AWAIT_VOLUME_CONFIRM,
) = range(8)

_DIV = "─" * 32


def _is_admin(update: Update) -> bool:
    return update.effective_user is not None and update.effective_user.id in ADMIN_IDS


def _admin_name(update: Update) -> str:
    u = update.effective_user
    if u is None:
        return "Unknown"
    return u.full_name or u.username or str(u.id)


def _header(title: str) -> str:
    return f"{_DIV}\n  {title.upper()}\n{_DIV}"


def _main_menu_keyboard(
    has_accounts: bool,
    has_proxy: bool,
    bot_running: bool,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    rows.append([InlineKeyboardButton("INPUT CREDENTIALS", callback_data="input_creds")])

    if has_accounts:
        label = "STOP BOT" if bot_running else "START BOT"
        rows.append([InlineKeyboardButton(label, callback_data="stop_bot" if bot_running else "start_bot")])

    rows.append([
        InlineKeyboardButton("CHANGE PROXY" if has_proxy else "SET PROXY", callback_data="set_proxy")
    ])
    rows.append([InlineKeyboardButton("SET FEE COOKIE", callback_data="set_fee_cookie")])
    rows.append([InlineKeyboardButton("SET MAX FEE (CC)", callback_data="set_max_fee")])
    rows.append([
        InlineKeyboardButton("STATUS", callback_data="status"),
        InlineKeyboardButton("SUMMARY", callback_data="summary"),
    ])

    if has_accounts:
        rows.append([InlineKeyboardButton("DELETE ALL ACCOUNTS", callback_data="delete_accounts")])
    rows.append([InlineKeyboardButton("DELETE ALL VOLUME DATA", callback_data="delete_volume")])

    return InlineKeyboardMarkup(rows)


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
                f"{_header('Cantex Swap System')}\n\n"
                "  Access Level   :  OBSERVER\n\n"
                "You will receive execution summaries\n"
                "automatically when trades complete.\n\n"
                f"{_DIV}"
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
            f"{_header('Cancelled')}\n\nOperation cancelled. Use /menu to continue.\n\n{_DIV}"
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
            bot_status += f"  (by {state.started_by})"

        text = (
            f"{_header('Cantex Swap System')}\n\n"
            f"  Bot Status     :  {bot_status}\n"
            f"  Accounts       :  {len(accounts)} loaded\n"
            f"  Max Fee        :  {cfg.max_fee_cc} CC\n"
            f"  Fee Cookie     :  {'configured' if cfg.cantex_cookie else 'not set'}\n\n"
            f"{_DIV}\n"
            f"  Select an action below\n"
            f"{_DIV}"
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
                    text=f"{_header('Admin Notification')}\n\n{message}\n\n{_DIV}",
                )
            except Exception as exc:
                log.debug("Gagal notify admin %s: %s", uid, exc)

    # ── Callback ───────────────────────────────────────────────────────

    async def _handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        await query.answer()

        if not _is_admin(update):
            await query.message.reply_text(
                f"{_header('Access Denied')}\n\nYou do not have admin access.\n\n{_DIV}"
            )
            return STATE_IDLE

        data  = query.data
        uid   = update.effective_user.id
        name  = _admin_name(update)

        # ── Input Credentials ──
        if data == "input_creds":
            await query.message.reply_text(
                f"{_header('Input Credentials')}\n\n"
                "How many accounts do you want to configure?\n"
                "Enter a number between 1 and 50.\n\n"
                "  /cancel to abort\n\n{_DIV}"
            )
            return STATE_AWAIT_ACCOUNT_COUNT

        # ── Start Bot ──
        elif data == "start_bot":
            accounts = CredentialStore.load_all()
            if not accounts:
                await query.message.reply_text(
                    f"{_header('Cannot Start')}\n\n"
                    "No accounts configured. Please input credentials first.\n\n"
                    f"{_DIV}"
                )
                return STATE_IDLE
            now = datetime.now(timezone.utc).isoformat()
            BotStateStore.save(BotState(running=True, started_by=name, started_at=now))
            await query.message.reply_text(
                f"{_header('Bot Started')}\n\n"
                f"  Started by     :  {name}\n"
                f"  Accounts       :  {len(accounts)}\n"
                f"  Time (UTC)     :  {now[:19].replace('T', ' ')}\n\n"
                "Bot is active and monitoring gas fees.\n\n"
                f"{_DIV}"
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
                f"{_header('Bot Stopped')}\n\n"
                f"  Stopped by     :  {name}\n"
                f"  Time (UTC)     :  {now[:19].replace('T', ' ')}\n\n"
                "Execution halted. Use START BOT to resume.\n\n"
                f"{_DIV}"
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
                    f"{_header('No Accounts')}\n\nInput credentials before setting proxy.\n\n{_DIV}"
                )
                return STATE_IDLE
            self._pending_input[uid] = {"action": "proxy", "accounts": accounts, "index": 0}
            acct = accounts[0]
            cur = f"\n  Current        :  {acct.proxy}" if acct.proxy else ""
            await query.message.reply_text(
                f"{_header('Set Proxy')}\n\n"
                f"  Account        :  {acct.name}{cur}\n\n"
                "Format: username:password@ip:port\n"
                "Type 'skip' to leave unchanged.\n\n"
                "  /cancel to abort\n\n"
                f"{_DIV}"
            )
            return STATE_AWAIT_PROXY

        # ── Set Fee Cookie ──
        elif data == "set_fee_cookie":
            cfg = FeeWatcherStore.load()
            cur = f"\n  Current        :  {cfg.cantex_cookie[:30]}..." if cfg.cantex_cookie else ""
            await query.message.reply_text(
                f"{_header('Set Fee Cookie')}\n\n"
                "Paste the Cantex cookie value below.\n\n"
                "How to retrieve:\n"
                "  1. Open cantex.io in browser\n"
                "  2. DevTools (F12) > Network tab\n"
                "  3. Any request > copy 'cookie' header{cur}\n\n"
                "  /cancel to abort\n\n"
                f"{_DIV}"
            )
            return STATE_AWAIT_FEE_COOKIE

        # ── Set Max Fee ──
        elif data == "set_max_fee":
            cfg = FeeWatcherStore.load()
            await query.message.reply_text(
                f"{_header('Set Max Fee')}\n\n"
                f"  Current limit  :  {cfg.max_fee_cc} CC\n\n"
                "Enter the max gas fee threshold in CC.\n"
                "Execution triggers when fee is at or below this.\n\n"
                "Example: 0.27\n\n"
                "  /cancel to abort\n\n"
                f"{_DIV}"
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
                f"{_header('Delete All Accounts')}\n\n"
                "This permanently removes all stored credentials.\n\n"
                "Type CONFIRM to proceed, or /cancel to abort.\n\n"
                f"{_DIV}"
            )
            return STATE_AWAIT_DELETE_CONFIRM

        # ── Delete Volume ──
        elif data == "delete_volume":
            await query.message.reply_text(
                f"{_header('Delete All Volume Data')}\n\n"
                "WARNING — this erases ALL Railway volume data:\n\n"
                "  Credentials, fee config, progress,\n"
                "  bot state, and signal files.\n\n"
                "This action is irreversible.\n\n"
                "Type CONFIRM to proceed, or /cancel to abort.\n\n"
                f"{_DIV}"
            )
            return STATE_AWAIT_VOLUME_CONFIRM

        return STATE_IDLE

    # ── Input Receivers ────────────────────────────────────────────────

    async def _recv_account_count(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        try:
            count = int(update.message.text.strip())
            if not 1 <= count <= 50:
                raise ValueError
        except ValueError:
            await update.message.reply_text("Invalid. Enter a number between 1 and 50.")
            return STATE_AWAIT_ACCOUNT_COUNT

        self._pending_input[uid] = {"action": "creds", "total": count, "current": 1, "collected": []}
        await update.message.reply_text(
            f"{_header(f'Account 1 of {count}')}\n\n"
            "Paste credentials (two lines):\n\n"
            "  Line 1: operator_key\n"
            "  Line 2: trading_key\n\n"
            "  /cancel to abort\n\n"
            f"{_DIV}"
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
                "Invalid format. Provide two lines:\n  Line 1: operator_key\n  Line 2: trading_key"
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
            for c in state["collected"]:
                CredentialStore.save(c)
            self._pending_input.pop(uid, None)
            names = ", ".join(c.name for c in state["collected"])
            await update.message.reply_text(
                f"{_header('Credentials Saved')}\n\n"
                f"  Accounts saved :  {len(state['collected'])}\n"
                f"  Names          :  {names}\n\n"
                "You can now START the bot or configure proxy.\n"
                "Use /menu to continue.\n\n"
                f"{_DIV}"
            )
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("credentials"))
            return STATE_IDLE
        else:
            await update.message.reply_text(
                f"{_header(f'Account {idx} Saved')}\n\n"
                f"  Progress       :  {idx} / {total}\n\n"
                f"Now enter credentials for Account {state['current']}:\n\n"
                "  Line 1: operator_key\n"
                "  Line 2: trading_key\n\n"
                "  /cancel to abort\n\n"
                f"{_DIV}"
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
                    "Type 'skip' to leave unchanged."
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
                f"{_header('Proxy Configuration Complete')}\n\n"
                "All accounts processed.\n"
                "Use /menu to continue.\n\n"
                f"{_DIV}"
            )
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("proxy"))
            return STATE_IDLE
        else:
            state["index"] = next_idx
            nxt = accounts[next_idx]
            cur = f"\n  Current        :  {nxt.proxy}" if nxt.proxy else ""
            await update.message.reply_text(
                f"{_header(f'Account: {nxt.name}')}\n\n"
                f"  Account        :  {nxt.name}{cur}\n\n"
                "Format: username:password@ip:port\n"
                "Type 'skip' to leave unchanged.\n\n"
                "  /cancel to abort\n\n"
                f"{_DIV}"
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
            f"{_header('Fee Cookie Saved')}\n\n"
            "Cookie stored securely. Use /menu to continue.\n\n"
            f"{_DIV}"
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
            f"{_header('Max Fee Updated')}\n\n"
            f"  New limit      :  {val} CC\n\n"
            "Execution triggers when fee is at or below this.\n"
            "Use /menu to continue.\n\n"
            f"{_DIV}"
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
                f"{_header('All Accounts Deleted')}\n\n"
                "Credentials removed. Bot stopped.\n"
                "Use /menu to reconfigure.\n\n"
                f"{_DIV}"
            )
            await self._notify_others(uid, f"All accounts DELETED by {name}. Bot stopped.")
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("delete"))
        else:
            await update.message.reply_text(
                f"{_header('Cancelled')}\n\nNo accounts deleted. Use /menu to return.\n\n{_DIV}"
            )
        self._pending_input.pop(uid, None)
        return STATE_IDLE

    async def _recv_volume_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid  = update.effective_user.id
        name = _admin_name(update)
        if update.message.text.strip() == "CONFIRM":
            clear_all_volume()
            await update.message.reply_text(
                f"{_header('Volume Cleared')}\n\n"
                "All Railway volume data permanently erased.\n\n"
                "  Credentials, fee config, progress,\n"
                "  bot state, and signal files removed.\n\n"
                "Use /menu to start fresh.\n\n"
                f"{_DIV}"
            )
            await self._notify_others(uid, f"ALL VOLUME DATA erased by {name}. Full reset performed.")
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("delete_volume"))
        else:
            await update.message.reply_text(
                f"{_header('Cancelled')}\n\nVolume data unchanged. Use /menu to return.\n\n{_DIV}"
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
            status_str += f"  (by {bot_state.started_by})"

        lines = [
            _header(f"System Status — {today}"),
            "",
            f"  Bot            :  {status_str}",
            f"  Accounts       :  {len(accounts)}",
            f"  Max Fee        :  {cfg.max_fee_cc} CC",
            f"  Fee Cookie     :  {'configured' if cfg.cantex_cookie else 'not set'}",
            "",
            _DIV,
            "  ACCOUNT PROGRESS",
            _DIV,
        ]

        if not accounts:
            lines.append("  No accounts configured.")
        for acct in accounts:
            prog      = progress.get(acct.name)
            completed = prog.completed_tx if prog else 0
            proxy_str = acct.proxy.split("@")[-1] if acct.proxy else "none"
            lines.append(f"  {acct.name:<18}  {completed}/6 tx  |  proxy: {proxy_str}")

        lines.append(_DIV)
        return "\n".join(lines)

    async def _build_summary_text(self) -> str:
        today     = datetime.now(timezone.utc).date().isoformat()
        progress  = ProgressStore.load_all_today(today)
        accounts  = CredentialStore.load_all()
        total_tx  = total_ok = total_fail = 0

        lines = [_header(f"Execution Summary — {today}"), ""]

        for acct in accounts:
            prog = progress.get(acct.name)
            if not prog or not prog.tx_log:
                lines.append(f"  {acct.name}   :   no transactions yet")
                continue
            lines.append(f"  {acct.name}  ({prog.completed_tx} tx):")
            for tx in prog.tx_log:
                ok     = tx.get("success")
                status = "OK" if ok else "FAIL"
                pair   = tx.get("pair", "?")
                sell   = tx.get("sell_amount", "?")
                recv   = tx.get("received_amount", "?")
                fee    = tx.get("fee_cc", "?")
                err    = f"  err: {tx['error'][:40]}" if tx.get("error") else ""
                lines.append(
                    f"    TX{tx['tx_index']+1}  {pair}  "
                    f"sell={sell}  recv={recv}  fee={fee} CC  [{status}]{err}"
                )
                total_tx += 1
                total_ok += 1 if ok else 0
                total_fail += 0 if ok else 1
            lines.append("")

        lines += [
            _DIV,
            f"  Total   :  {total_tx} tx",
            f"  Success :  {total_ok}",
            f"  Failed  :  {total_fail}",
            _DIV,
        ]
        return "\n".join(lines)
