from __future__ import annotations

"""
Telegram bot control panel.
Admin user ID: 6469077855
- Admin: full control (input credentials, set fee, start/stop, lihat summary)
- Non-admin: hanya terima summary
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any

from telegram import (
    Bot,
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
    CredentialStore,
    FeeWatcherConfig,
    FeeWatcherStore,
    ProgressStore,
)

log = logging.getLogger("cantex.telegram")

ADMIN_IDS = {6469077855}

# Conversation states
(
    STATE_IDLE,
    STATE_AWAIT_ACCOUNT_COUNT,
    STATE_AWAIT_CREDS,
    STATE_AWAIT_PROXY,
    STATE_AWAIT_FEE_COOKIE,
    STATE_AWAIT_MAX_FEE,
    STATE_AWAIT_DELETE_CONFIRM,
) = range(7)


def _is_admin(update: Update) -> bool:
    return update.effective_user is not None and update.effective_user.id in ADMIN_IDS


def _main_menu_keyboard(has_accounts: bool, has_proxy: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("Input Credentials Canton", callback_data="input_creds")],
        [
            InlineKeyboardButton(
                "Change Proxy" if has_proxy else "Set Proxy",
                callback_data="set_proxy",
            )
        ],
        [InlineKeyboardButton("Set Fee Watcher Cookie", callback_data="set_fee_cookie")],
        [InlineKeyboardButton("Set Max Fee (CC)", callback_data="set_max_fee")],
        [InlineKeyboardButton("Lihat Status", callback_data="status")],
        [InlineKeyboardButton("Lihat Summary Hari Ini", callback_data="summary")],
    ]
    if has_accounts:
        rows.append([InlineKeyboardButton("Hapus Semua Akun", callback_data="delete_accounts")])
    return InlineKeyboardMarkup(rows)


class TelegramPanel:
    def __init__(self, token: str, *, on_config_updated=None) -> None:
        self._token = token
        self._on_config_updated = on_config_updated
        self._app: Application | None = None
        self._pending_input: dict[int, dict] = {}
        self._subscriber_ids: set[int] = set()
        self._subscriber_ids.update(ADMIN_IDS)

    async def start(self) -> None:
        self._app = Application.builder().token(self._token).build()
        app = self._app

        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", self._cmd_start),
                CallbackQueryHandler(self._handle_callback),
            ],
            states={
                STATE_AWAIT_ACCOUNT_COUNT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_account_count)
                ],
                STATE_AWAIT_CREDS: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_creds)
                ],
                STATE_AWAIT_PROXY: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_proxy)
                ],
                STATE_AWAIT_FEE_COOKIE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_fee_cookie)
                ],
                STATE_AWAIT_MAX_FEE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_max_fee)
                ],
                STATE_AWAIT_DELETE_CONFIRM: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._recv_delete_confirm)
                ],
            },
            fallbacks=[
                CommandHandler("cancel", self._cmd_cancel),
                CommandHandler("menu", self._cmd_menu),
            ],
            per_chat=True,
            allow_reentry=True,
        )

        app.add_handler(conv_handler)
        app.add_handler(CommandHandler("menu", self._cmd_menu))
        app.add_handler(CommandHandler("status", self._cmd_status))
        app.add_handler(CommandHandler("summary", self._cmd_summary))

        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        log.info("Telegram bot started")

    async def stop(self) -> None:
        if self._app:
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()

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
                log.debug("Gagal kirim ke admin %s: %s", uid, exc)

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        self._subscriber_ids.add(uid)
        if _is_admin(update):
            accounts = CredentialStore.load_all()
            has_proxy = any(a.proxy for a in accounts)
            await update.message.reply_text(
                "Cantex Swap Bot\n\nPanel kontrol admin:",
                reply_markup=_main_menu_keyboard(bool(accounts), has_proxy),
            )
        else:
            await update.message.reply_text(
                "Cantex Swap Bot\n\nAnda akan menerima summary setelah transaksi selesai."
            )
        return STATE_IDLE

    async def _cmd_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not _is_admin(update):
            return STATE_IDLE
        accounts = CredentialStore.load_all()
        has_proxy = any(a.proxy for a in accounts)
        msg = update.message or (update.callback_query and update.callback_query.message)
        if msg:
            await msg.reply_text(
                "Menu utama:",
                reply_markup=_main_menu_keyboard(bool(accounts), has_proxy),
            )
        return STATE_IDLE

    async def _cmd_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        self._pending_input.pop(update.effective_user.id, None)
        await update.message.reply_text("Dibatalkan. Ketik /menu untuk kembali.")
        return STATE_IDLE

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        if not _is_admin(update):
            return STATE_IDLE
        text = await self._build_status_text()
        await update.message.reply_text(text)
        return STATE_IDLE

    async def _cmd_summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        text = await self._build_summary_text()
        await update.message.reply_text(text)
        return STATE_IDLE

    # ------------------------------------------------------------------
    # Callback query handler
    # ------------------------------------------------------------------

    async def _handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        await query.answer()
        if not _is_admin(update):
            await query.message.reply_text("Akses ditolak.")
            return STATE_IDLE

        data = query.data
        uid = update.effective_user.id

        if data == "input_creds":
            await query.message.reply_text(
                "Berapa akun yang ingin diinput?\n(Ketik angka, contoh: 3)\n\n/cancel untuk batal"
            )
            return STATE_AWAIT_ACCOUNT_COUNT

        elif data == "set_proxy":
            accounts = CredentialStore.load_all()
            count = len(accounts)
            if count == 0:
                await query.message.reply_text("Belum ada akun. Input credentials dulu.")
                return STATE_IDLE
            self._pending_input[uid] = {"action": "proxy", "accounts": accounts, "index": 0}
            acct = accounts[0]
            current = f"\nProxy saat ini: {acct.proxy}" if acct.proxy else ""
            await query.message.reply_text(
                f"Input proxy untuk akun '{acct.name}'{current}\n"
                "Format: username:password@ip:port\n"
                "Ketik 'skip' untuk lewati akun ini.\n\n/cancel untuk batal"
            )
            return STATE_AWAIT_PROXY

        elif data == "set_fee_cookie":
            cfg = FeeWatcherStore.load()
            current = f"\nCookie saat ini: {cfg.cantex_cookie[:40]}..." if cfg.cantex_cookie else ""
            await query.message.reply_text(
                f"Input cookie Cantex untuk fee watcher:{current}\n\n"
                "Cara ambil cookie: buka cantex.io di browser, buka DevTools (F12), "
                "tab Network, request apapun, copy nilai header 'cookie'.\n\n/cancel untuk batal"
            )
            return STATE_AWAIT_FEE_COOKIE

        elif data == "set_max_fee":
            cfg = FeeWatcherStore.load()
            await query.message.reply_text(
                f"Set maksimum fee untuk eksekusi (dalam CC).\n"
                f"Saat ini: {cfg.max_fee_cc}\n\n"
                "Contoh: 0.27\n\n/cancel untuk batal"
            )
            return STATE_AWAIT_MAX_FEE

        elif data == "status":
            text = await self._build_status_text()
            await query.message.reply_text(text)
            return STATE_IDLE

        elif data == "summary":
            text = await self._build_summary_text()
            await query.message.reply_text(text)
            return STATE_IDLE

        elif data == "delete_accounts":
            await query.message.reply_text(
                "Yakin hapus semua akun? Ketik 'HAPUS' untuk konfirmasi.\n\n/cancel untuk batal"
            )
            return STATE_AWAIT_DELETE_CONFIRM

        return STATE_IDLE

    # ------------------------------------------------------------------
    # Input receivers
    # ------------------------------------------------------------------

    async def _recv_account_count(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        text = update.message.text.strip()
        try:
            count = int(text)
            if count < 1 or count > 50:
                raise ValueError
        except ValueError:
            await update.message.reply_text("Angka tidak valid. Masukkan angka 1-50.")
            return STATE_AWAIT_ACCOUNT_COUNT

        self._pending_input[uid] = {
            "action": "creds",
            "total": count,
            "current": 1,
            "collected": [],
        }
        await update.message.reply_text(
            f"Input credentials untuk {count} akun.\n\n"
            "Format untuk Akun 1:\n"
            "operator_key\ntrading_key\n\n"
            "(2 baris: baris pertama operator key, baris kedua trading key)\n\n/cancel untuk batal"
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
                "Format salah. Masukkan 2 baris:\nbaris 1: operator_key\nbaris 2: trading_key"
            )
            return STATE_AWAIT_CREDS

        operator_key = lines[0]
        trading_key = lines[1]
        current_index = state["current"]
        name = f"akun_{current_index}"

        cred = AccountCredential(
            name=name,
            operator_key=operator_key,
            trading_key=trading_key,
        )
        state["collected"].append(cred)
        state["current"] += 1

        if state["current"] > state["total"]:
            # simpan semua
            for c in state["collected"]:
                CredentialStore.save(c)
            self._pending_input.pop(uid, None)
            saved_names = ", ".join(c.name for c in state["collected"])
            await update.message.reply_text(
                f"Berhasil simpan {len(state['collected'])} akun: {saved_names}\n\n"
                "Selanjutnya set proxy jika diperlukan.\nKetik /menu untuk kembali."
            )
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("credentials"))
            return STATE_IDLE
        else:
            await update.message.reply_text(
                f"Akun {current_index} tersimpan.\n\n"
                f"Sekarang input credentials untuk Akun {state['current']}:\n"
                "Format: operator_key\ntrading_key\n\n/cancel untuk batal"
            )
            return STATE_AWAIT_CREDS

    async def _recv_proxy(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        state = self._pending_input.get(uid, {})
        if state.get("action") != "proxy":
            return STATE_IDLE

        text = update.message.text.strip()
        accounts: list[AccountCredential] = state["accounts"]
        index: int = state["index"]
        current_acct = accounts[index]

        if text.lower() != "skip":
            # validasi format proxy sederhana
            if "@" not in text or ":" not in text.split("@")[0]:
                await update.message.reply_text(
                    "Format proxy tidak valid.\nFormat: username:password@ip:port\nAtau ketik 'skip'."
                )
                return STATE_AWAIT_PROXY
            current_acct = AccountCredential(
                name=current_acct.name,
                operator_key=current_acct.operator_key,
                trading_key=current_acct.trading_key,
                proxy=text,
            )
            CredentialStore.save(current_acct)
            await update.message.reply_text(f"Proxy untuk '{current_acct.name}' tersimpan.")
        else:
            await update.message.reply_text(f"Proxy untuk '{current_acct.name}' dilewati.")

        next_index = index + 1
        if next_index >= len(accounts):
            self._pending_input.pop(uid, None)
            await update.message.reply_text("Selesai set proxy. Ketik /menu untuk kembali.")
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("proxy"))
            return STATE_IDLE
        else:
            state["index"] = next_index
            next_acct = accounts[next_index]
            current_proxy = f"\nProxy saat ini: {next_acct.proxy}" if next_acct.proxy else ""
            await update.message.reply_text(
                f"Input proxy untuk akun '{next_acct.name}'{current_proxy}\n"
                "Format: username:password@ip:port\n"
                "Ketik 'skip' untuk lewati.\n\n/cancel untuk batal"
            )
            return STATE_AWAIT_PROXY

    async def _recv_fee_cookie(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        cookie = update.message.text.strip()
        if len(cookie) < 10:
            await update.message.reply_text("Cookie terlalu pendek. Coba lagi atau /cancel.")
            return STATE_AWAIT_FEE_COOKIE

        cfg = FeeWatcherStore.load()
        cfg.cantex_cookie = cookie
        FeeWatcherStore.save(cfg)
        self._pending_input.pop(uid, None)

        # hapus pesan yang berisi cookie untuk keamanan
        try:
            await update.message.delete()
        except Exception:
            pass

        await update.effective_chat.send_message(
            "Cookie fee watcher tersimpan.\nKetik /menu untuk kembali."
        )
        if self._on_config_updated:
            asyncio.create_task(self._on_config_updated("fee_cookie"))
        return STATE_IDLE

    async def _recv_max_fee(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        text = update.message.text.strip()
        try:
            val = float(text)
            if val <= 0 or val > 100:
                raise ValueError
        except ValueError:
            await update.message.reply_text("Nilai tidak valid. Masukkan angka positif, contoh: 0.27")
            return STATE_AWAIT_MAX_FEE

        cfg = FeeWatcherStore.load()
        cfg.max_fee_cc = val
        FeeWatcherStore.save(cfg)
        self._pending_input.pop(uid, None)
        await update.message.reply_text(
            f"Max fee diset ke {val} CC.\nKetik /menu untuk kembali."
        )
        if self._on_config_updated:
            asyncio.create_task(self._on_config_updated("max_fee"))
        return STATE_IDLE

    async def _recv_delete_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        uid = update.effective_user.id
        if update.message.text.strip() == "HAPUS":
            CredentialStore.clear_all()
            await update.message.reply_text("Semua akun dihapus. Ketik /menu.")
            if self._on_config_updated:
                asyncio.create_task(self._on_config_updated("delete"))
        else:
            await update.message.reply_text("Batal hapus. Ketik /menu.")
        self._pending_input.pop(uid, None)
        return STATE_IDLE

    # ------------------------------------------------------------------
    # Status & Summary builders
    # ------------------------------------------------------------------

    async def _build_status_text(self) -> str:
        accounts = CredentialStore.load_all()
        cfg = FeeWatcherStore.load()
        today = datetime.now(timezone.utc).date().isoformat()
        all_progress = ProgressStore.load_all_today(today)

        lines = [
            f"Status - {today}",
            f"Jumlah akun: {len(accounts)}",
            f"Max fee: {cfg.max_fee_cc} CC",
            f"Cookie fee watcher: {'ada' if cfg.cantex_cookie else 'belum diset'}",
            "",
            "Progress hari ini:",
        ]
        if not accounts:
            lines.append("  (belum ada akun)")
        for acct in accounts:
            prog = all_progress.get(acct.name, None)
            completed = prog.completed_tx if prog else 0
            proxy_info = f" | proxy: {acct.proxy.split('@')[-1] if acct.proxy else 'tidak ada'}"
            lines.append(f"  {acct.name}: {completed}/{6} tx{proxy_info}")
        return "\n".join(lines)

    async def _build_summary_text(self) -> str:
        today = datetime.now(timezone.utc).date().isoformat()
        all_progress = ProgressStore.load_all_today(today)
        accounts = CredentialStore.load_all()

        lines = [f"Summary - {today}", ""]
        total_tx = 0
        total_success = 0
        total_failed = 0

        for acct in accounts:
            prog = all_progress.get(acct.name)
            if not prog or not prog.tx_log:
                lines.append(f"{acct.name}: belum ada tx")
                continue
            lines.append(f"{acct.name} ({prog.completed_tx} tx):")
            for tx in prog.tx_log:
                status = "OK" if tx.get("success") else "GAGAL"
                pair = tx.get("pair", "?")
                sell_amount = tx.get("sell_amount", "?")
                received = tx.get("received_amount", "?")
                fee = tx.get("fee_cc", "?")
                err = f" | err: {tx['error'][:50]}" if tx.get("error") else ""
                lines.append(
                    f"  TX{tx['tx_index']+1}: {pair} | jual={sell_amount} | dapat={received} | fee={fee} CC | {status}{err}"
                )
                if tx.get("success"):
                    total_success += 1
                else:
                    total_failed += 1
                total_tx += 1
            lines.append("")

        lines.append(f"Total: {total_tx} tx | sukses: {total_success} | gagal: {total_failed}")
        return "\n".join(lines)
