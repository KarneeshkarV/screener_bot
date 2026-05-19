from __future__ import annotations

import logging
from datetime import time
from zoneinfo import ZoneInfo

from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from .config import BotConfig, EnvSettings
from .formatting import (
    format_detail_report,
    format_portfolio_report,
    split_messages,
)
from .ownership import OwnershipService
from .scheduled_screener import ScheduledScreenerService, send_screener_report
from .technical import TechnicalService


HELP_TEXT = (
    "Commands:\n"
    "/run - run screener changes now\n"
    "/run india ema - run one screener and show added/removed entries\n"
    "/run_all - run all screeners and show the current lists\n"
    "/run_all india ema - run one screener and show all returned rows\n"
    "/check_portfolio - check every configured holding\n"
    "/stock SYMBOL - detailed technicals for any symbol\n"
    "/status - show bot status\n"
    "/help - show this help"
)

CALLBACK_DETAIL = "d"

BOT_COMMANDS = [
    BotCommand("start", "Start the bot"),
    BotCommand("help", "Show available commands"),
    BotCommand("status", "Show bot status"),
    BotCommand("run", "Run screener changes now"),
    BotCommand("run_all", "Run all screeners and show current lists"),
    BotCommand("check_portfolio", "Check every configured holding"),
    BotCommand("stock", "Detailed technicals for any symbol"),
]


def _holdings_keyboard(config: BotConfig) -> InlineKeyboardMarkup:
    buttons = []
    row: list[InlineKeyboardButton] = []
    for item in config.portfolio:
        label = item.symbol.split(":")[-1]
        row.append(
            InlineKeyboardButton(
                f"📈 {label}",
                callback_data=f"{CALLBACK_DETAIL}|{item.market}|{item.symbol}"[:64],
            )
        )
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)


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
                f"Scheduled screener: {_scheduled_status(config)}\n"
                f"Scheduled portfolio check: daily at "
                f"{PORTFOLIO_CHECK_TIME} {config.timezone}"
            )

    def _portfolio_report() -> str:
        technical = technical_service.check_portfolio()
        ownership = ownership_service.check_portfolio(config.portfolio)
        return format_portfolio_report(technical, ownership)

    async def _run_portfolio_check(update: Update) -> None:
        if not update.message:
            return
        await update.message.reply_text("Checking portfolio...")
        try:
            report = _portfolio_report()
        except Exception:
            logging.exception("portfolio check failed")
            await update.message.reply_text("Portfolio check failed. See logs.")
            return
        messages = split_messages(report)
        for index, message in enumerate(messages):
            await update.message.reply_text(
                message,
                parse_mode=ParseMode.HTML,
                reply_markup=(
                    _holdings_keyboard(config)
                    if index == len(messages) - 1
                    else None
                ),
            )

    async def _scheduled_portfolio_check(
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        try:
            report = _portfolio_report()
        except Exception:
            logging.exception("scheduled portfolio check failed")
            return
        targets = (
            config.scheduled_screener.chat_ids
            or config.telegram.allowed_chat_ids
        )
        for chat_id in targets:
            messages = split_messages(report)
            for index, message in enumerate(messages):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode=ParseMode.HTML,
                    reply_markup=(
                        _holdings_keyboard(config)
                        if index == len(messages) - 1
                        else None
                    ),
                )

    async def run(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await _guard(config, update) and update.effective_chat and update.message:
            query = " ".join(context.args) if context.args else None
            label = (
                f"Running screener changes for {query}..."
                if query
                else "Running screener changes..."
            )
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

    async def run_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await _guard(config, update) and update.effective_chat and update.message:
            query = " ".join(context.args) if context.args else None
            label = (
                f"Running full screener for {query}..."
                if query
                else "Running full screener..."
            )
            await update.message.reply_text(label)
            try:
                await send_screener_report(
                    context,
                    screener_service,
                    [update.effective_chat.id],
                    query=query,
                    full_list=True,
                )
            except Exception:
                logging.exception("scheduled screener full manual run failed")
                await update.message.reply_text("Screener run failed. See logs.")

    async def check_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await _guard(config, update):
            await _run_portfolio_check(update)

    def _detail_report(symbol: str, market: str | None) -> str:
        status = technical_service.detail(symbol, market)
        return format_detail_report(status)

    async def stock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not (await _guard(config, update) and update.message):
            return
        if not context.args:
            await update.message.reply_text(
                "Usage: /stock SYMBOL [us|india]\n"
                "Examples: /stock AAPL · /stock NSE:NMDC · /stock TCS india"
            )
            return
        symbol = context.args[0]
        market = None
        if len(context.args) > 1 and context.args[1].lower() in {"us", "india"}:
            market = context.args[1].lower()
        await update.message.reply_text(f"Fetching {symbol}...")
        try:
            report = _detail_report(symbol, market)
        except Exception:
            logging.exception("stock detail failed for %s", symbol)
            await update.message.reply_text("Stock lookup failed. See logs.")
            return
        await update.message.reply_text(report, parse_mode=ParseMode.HTML)

    async def detail_callback(
        update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if query is None:
            return
        if not _authorized(config, update):
            await query.answer("Unauthorized chat.", show_alert=True)
            return
        await query.answer()
        parts = (query.data or "").split("|")
        if len(parts) != 3 or parts[0] != CALLBACK_DETAIL:
            return
        _, market, symbol = parts
        try:
            report = _detail_report(symbol, market or None)
        except Exception:
            logging.exception("detail callback failed for %s", symbol)
            report = "Stock lookup failed. See logs."
        if query.message:
            await query.message.reply_text(report, parse_mode=ParseMode.HTML)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("run", run))
    app.add_handler(CommandHandler("run_all", run_all))
    app.add_handler(CommandHandler("check_portfolio", check_portfolio))
    app.add_handler(CommandHandler("stock", stock))
    app.add_handler(
        CallbackQueryHandler(detail_callback, pattern=f"^{CALLBACK_DETAIL}\\|")
    )
    app.post_init = _post_init(config, screener_service, _scheduled_portfolio_check)
    return app


async def _register_commands(app: Application) -> None:
    await app.bot.set_my_commands(BOT_COMMANDS)


def _post_init(
    config: BotConfig,
    screener_service: ScheduledScreenerService,
    portfolio_callback,
):
    async def post_init(app: Application) -> None:
        await _register_commands(app)
        _schedule_screener_jobs(app, config, screener_service)
        _schedule_portfolio_jobs(app, config, portfolio_callback)

    return post_init


PORTFOLIO_CHECK_TIME = "06:00"


def _schedule_portfolio_jobs(app: Application, config: BotConfig, callback) -> None:
    if app.job_queue is None:
        logging.warning(
            "scheduled portfolio check disabled: application has no job queue"
        )
        return

    tz = ZoneInfo(config.timezone)
    hour, minute = (int(part) for part in PORTFOLIO_CHECK_TIME.split(":"))
    run_time = time(hour=hour, minute=minute, tzinfo=tz)
    app.job_queue.run_daily(
        callback,
        time=run_time,
        name="scheduled-portfolio-check",
    )


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
