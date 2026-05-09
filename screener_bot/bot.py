from __future__ import annotations

import logging
from datetime import time
from zoneinfo import ZoneInfo

from telegram import BotCommand, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

from .config import BotConfig, EnvSettings
from .formatting import format_portfolio_report, split_messages
from .ownership import OwnershipService
from .scheduled_screener import ScheduledScreenerService, send_screener_report
from .technical import TechnicalService


HELP_TEXT = (
    "Commands:\n"
    "/run - run EMA and holding-change screener now\n"
    "/run india ema - run one screener and show all returned rows\n"
    "/check_portfolio - check every configured holding\n"
    "/status - show bot status\n"
    "/help - show this help"
)

BOT_COMMANDS = [
    BotCommand("start", "Start the bot"),
    BotCommand("help", "Show available commands"),
    BotCommand("status", "Show bot status"),
    BotCommand("run", "Run EMA and holding-change screener now"),
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
    screener_service: ScheduledScreenerService | None = None,
) -> Application:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    technical_service = technical_service or TechnicalService(config)
    ownership_service = ownership_service or OwnershipService()
    screener_service = screener_service or ScheduledScreenerService(config)
    app = Application.builder().token(settings.telegram_bot_token).build()

    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await _guard(config, update) and update.message:
            await update.message.reply_text("Screener bot is ready. Use /check_portfolio.")

    async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await _guard(config, update) and update.message:
            await update.message.reply_text(HELP_TEXT)

    async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await _guard(config, update) and update.message:
            await update.message.reply_text(
                f"Configured holdings: {len(config.portfolio)}\n"
                f"Timezone: {config.timezone}\n"
                f"Scheduled screener: {_scheduled_status(config)}"
            )

    async def _run_portfolio_check(update: Update) -> None:
        if not update.message:
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

    async def run(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await _guard(config, update) and update.effective_chat and update.message:
            query = " ".join(context.args) if context.args else None
            label = f"Running screener for {query}..." if query else "Running screener..."
            await update.message.reply_text(label)
            try:
                await send_screener_report(
                    context,
                    screener_service,
                    [update.effective_chat.id],
                    query=query,
                )
            except Exception:
                logging.exception("scheduled screener manual run failed")
                await update.message.reply_text("Screener run failed. See logs.")

    async def check_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await _guard(config, update):
            await _run_portfolio_check(update)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("run", run))
    app.add_handler(CommandHandler("check_portfolio", check_portfolio))
    app.post_init = _post_init(config, screener_service)
    return app


async def _register_commands(app: Application) -> None:
    await app.bot.set_my_commands(BOT_COMMANDS)


def _post_init(config: BotConfig, screener_service: ScheduledScreenerService):
    async def post_init(app: Application) -> None:
        await _register_commands(app)
        _schedule_screener_jobs(app, config, screener_service)

    return post_init


def _schedule_screener_jobs(
    app: Application,
    config: BotConfig,
    screener_service: ScheduledScreenerService,
) -> None:
    scheduled = config.scheduled_screener
    if not scheduled.enabled or not scheduled.times:
        return
    if app.job_queue is None:
        logging.warning("scheduled screener disabled: application has no job queue")
        return

    tz = ZoneInfo(config.timezone)
    for item in scheduled.times:
        hour, minute = (int(part) for part in item.split(":"))
        run_time = time(hour=hour, minute=minute, tzinfo=tz)
        app.job_queue.run_daily(
            _scheduled_screener_callback,
            time=run_time,
            data=screener_service,
            name=f"scheduled-screener-{item}",
        )


async def _scheduled_screener_callback(context: ContextTypes.DEFAULT_TYPE) -> None:
    service = context.job.data
    if not isinstance(service, ScheduledScreenerService):
        logging.error("scheduled screener job missing service")
        return
    try:
        await send_screener_report(context, service)
    except Exception:
        logging.exception("scheduled screener job failed")


def _scheduled_status(config: BotConfig) -> str:
    scheduled = config.scheduled_screener
    if not scheduled.enabled:
        return "disabled"
    if not scheduled.times:
        return "enabled, no times configured"
    return "enabled at " + ", ".join(scheduled.times)
