from __future__ import annotations

import logging

from telegram import BotCommand, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

from .config import BotConfig, EnvSettings
from .formatting import format_portfolio_report, split_messages
from .ownership import OwnershipService
from .technical import TechnicalService


HELP_TEXT = (
    "Commands:\n"
    "/chat_id - show this chat ID\n"
    "/check_portfolio - check every configured holding\n"
    "/status - show bot status\n"
    "/help - show this help"
)

BOT_COMMANDS = [
    BotCommand("start", "Start the bot"),
    BotCommand("help", "Show available commands"),
    BotCommand("status", "Show bot status"),
    BotCommand("chat_id", "Show this chat ID"),
    BotCommand("check_portfolio", "Check every configured holding"),
]


def _authorized(config: BotConfig, update: Update) -> bool:
    chat = update.effective_chat
    return bool(chat and chat.id in config.telegram.allowed_chat_ids)


async def _guard(config: BotConfig, update: Update) -> bool:
    if _authorized(config, update):
        return True
    if update.message:
        await update.message.reply_text("Unauthorized chat.")
    return False


def build_application(
    settings: EnvSettings,
    config: BotConfig,
    technical_service: TechnicalService | None = None,
    ownership_service: OwnershipService | None = None,
) -> Application:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    technical_service = technical_service or TechnicalService(config)
    ownership_service = ownership_service or OwnershipService()
    app = Application.builder().token(settings.telegram_bot_token).build()

    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await _guard(config, update) and update.message:
            await update.message.reply_text("Screener bot is ready. Use /check_portfolio.")

    async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await _guard(config, update) and update.message:
            await update.message.reply_text(HELP_TEXT)

    async def chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_chat and update.message:
            await update.message.reply_text(f"Chat ID: {update.effective_chat.id}")

    async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await _guard(config, update) and update.message:
            await update.message.reply_text(
                f"Configured holdings: {len(config.portfolio)}\n"
                f"Timezone: {config.timezone}"
            )

    async def check_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await _guard(config, update) or not update.message:
            return
        await update.message.reply_text("Checking portfolio...")
        try:
            technical = technical_service.check_portfolio()
            ownership = ownership_service.check_portfolio(config.portfolio)
            report = format_portfolio_report(technical, ownership)
        except Exception:
            logging.exception("portfolio check failed")
            await update.message.reply_text("Portfolio check failed. See logs.")
            return
        for message in split_messages(report):
            await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("chat_id", chat_id))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("check_portfolio", check_portfolio))
    app.post_init = _register_commands
    return app


async def _register_commands(app: Application) -> None:
    await app.bot.set_my_commands(BOT_COMMANDS)
