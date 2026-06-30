from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, time
from time import monotonic
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

from . import portfolio_store
from .alerts import AlertService
from .charts import render_price_chart
from .config import BotConfig, EnvSettings, PortfolioItem
from .formatting import (
    format_detail_report,
    format_portfolio_report,
    split_messages,
)
from .ownership import OwnershipService
from .paper.engine import PaperTradingEngine
from .paper.reporting import (
    format_daily_report,
    format_metrics,
    format_portfolios_list,
    format_portfolio_status,
    format_trades,
    format_weekly_report,
)
from .paper.store import PaperStore
from .scheduled_screener import ScheduledScreenerService, send_screener_report
from .technical import TechnicalService


HELP_TEXT = (
    "Commands:\n"
    "/run - run screener changes now\n"
    "/run india ema - run one screener and show added/removed entries\n"
    "/run_all - run all screeners and show the current lists\n"
    "/run_all india ema - run one screener and show all returned rows\n"
    "/check_portfolio - check every configured holding\n"
    "/stock SYMBOL - detailed technicals + chart for any symbol\n"
    "/add SYMBOL AVG_PRICE [us|india] [sl=STOP] - add or update a holding\n"
    "/remove SYMBOL - remove a holding\n"
    "/setstop SYMBOL STOP - update a holding's stop-loss\n"
    "/alerts - check holdings for changes now\n"
    "/paper_status [name] - paper portfolio status\n"
    "/paper_portfolios - list all paper portfolios\n"
    "/paper_trades [name] - recent paper trades\n"
    "/paper_enable name - enable a paper portfolio\n"
    "/paper_disable name - disable a paper portfolio\n"
    "/paper_reset name - reset a paper portfolio\n"
    "/status - show bot status\n"
    "/help - show this help"
)

ADD_USAGE = (
    "Usage: /add SYMBOL AVG_PRICE [us|india] [sl=STOP]\n"
    "Examples: /add AAPL 190.5 · /add NSE:TCS 3500 sl=3300 · /add TCS 3500 india"
)
REMOVE_USAGE = "Usage: /remove SYMBOL"
SETSTOP_USAGE = "Usage: /setstop SYMBOL STOP"

CALLBACK_DETAIL = "d"

BOT_COMMANDS = [
    BotCommand("start", "Start the bot"),
    BotCommand("help", "Show available commands"),
    BotCommand("status", "Show bot status"),
    BotCommand("run", "Run screener changes now"),
    BotCommand("run_all", "Run all screeners and show current lists"),
    BotCommand("check_portfolio", "Check every configured holding"),
    BotCommand("stock", "Detailed technicals + chart for any symbol"),
    BotCommand("add", "Add or update a holding"),
    BotCommand("remove", "Remove a holding"),
    BotCommand("setstop", "Update a holding's stop-loss"),
    BotCommand("alerts", "Check holdings for changes now"),
    BotCommand("paper_status", "Paper portfolio status"),
    BotCommand("paper_portfolios", "List all paper portfolios"),
    BotCommand("paper_trades", "Recent paper trades"),
    BotCommand("paper_enable", "Enable a paper portfolio"),
    BotCommand("paper_disable", "Disable a paper portfolio"),
    BotCommand("paper_reset", "Reset a paper portfolio"),
]


def _parse_positive(text: str) -> float | None:
    """Parse a strictly positive, finite number; None when invalid."""
    try:
        value = float(text.replace(",", ""))
    except ValueError:
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    return value


def _infer_market(symbol: str) -> str:
    """Default market for a normalized symbol.

    Mirrors ``TechnicalService._candidate_markets``: NSE/BSE prefixes and
    .NS/.BO suffixes mean India, everything else defaults to the US market.
    """
    if ":" in symbol:
        exchange = symbol.split(":", 1)[0]
        return "india" if exchange in {"NSE", "BSE"} else "us"
    if symbol.endswith((".NS", ".BO")):
        return "india"
    return "us"


def _sync_saved_holding(config: BotConfig, saved: dict) -> None:
    """Reflect an upserted row in the in-memory portfolio."""
    item = PortfolioItem.model_validate(saved)
    for index, existing in enumerate(config.portfolio):
        if existing.symbol == item.symbol and existing.market == item.market:
            config.portfolio[index] = item
            return
    config.portfolio.append(item)


def _sync_removed_holding(config: BotConfig, symbol: str) -> None:
    config.portfolio[:] = [
        item for item in config.portfolio if item.symbol != symbol
    ]


def _sync_stop_loss(config: BotConfig, symbol: str, stop_loss: float) -> None:
    for item in config.portfolio:
        if item.symbol == symbol:
            item.stop_loss = stop_loss


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


# Notify the admin at most once per job per hour.
_ADMIN_NOTIFY_SECONDS = 3600.0


class _AdminNotifier:
    """Send short failure notes to an optional admin chat, throttled per job."""

    def __init__(self, config: BotConfig) -> None:
        self._chat_id = config.telegram.admin_chat_id
        self._timezone = config.timezone
        self._last_sent: dict[str, float] = {}

    async def notify(self, bot, job_name: str, error: BaseException) -> None:
        if self._chat_id is None:
            return
        now = monotonic()
        last = self._last_sent.get(job_name)
        if last is not None and now - last < _ADMIN_NOTIFY_SECONDS:
            return
        self._last_sent[job_name] = now
        timestamp = datetime.now(ZoneInfo(self._timezone)).strftime("%Y-%m-%d %H:%M")
        text = f"⚠️ Job {job_name} failed: {type(error).__name__} at {timestamp}"
        try:
            await bot.send_message(chat_id=self._chat_id, text=text)
        except Exception:
            logging.exception("admin error notification failed for job %s", job_name)


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
    alert_service: AlertService | None = None,
    portfolio_repo: portfolio_store.PortfolioRepo | None = None,
    paper_engine: PaperTradingEngine | None = None,
) -> Application:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    technical_service = technical_service or TechnicalService(config)
    ownership_service = ownership_service or OwnershipService()
    screener_service = screener_service or ScheduledScreenerService(config)
    # Share the technical service (and its price cache) with the alert engine.
    alert_service = alert_service or AlertService(config, technical_service)
    portfolio_repo = portfolio_repo or portfolio_store.PortfolioRepo()
    paper_engine = paper_engine or PaperTradingEngine()
    app = Application.builder().token(settings.telegram_bot_token).build()
    notifier = _AdminNotifier(config)
    # Prevents a manual /alerts check and the scheduled job from overlapping.
    alert_lock = asyncio.Lock()

    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await _guard(config, update) and update.message:
            await update.message.reply_text(
                "Screener bot is ready. Use /check_portfolio."
            )

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
                f"{PORTFOLIO_CHECK_TIME} {config.timezone}\n"
                f"Alerts: {_alerts_status(config)}"
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
            report = await asyncio.to_thread(_portfolio_report)
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
                    _holdings_keyboard(config) if index == len(messages) - 1 else None
                ),
            )

    async def _scheduled_portfolio_check(
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        try:
            report = await asyncio.to_thread(_portfolio_report)
        except Exception as exc:
            logging.exception("scheduled portfolio check failed")
            await notifier.notify(context.bot, "scheduled-portfolio-check", exc)
            return
        targets = config.scheduled_screener.chat_ids or config.telegram.allowed_chat_ids
        for chat_id in targets:
            messages = split_messages(report)
            for index, message in enumerate(messages):
                try:
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
                except Exception:
                    logging.exception(
                        "failed to send portfolio report message to chat %s", chat_id
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

    async def check_portfolio(
        update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if await _guard(config, update):
            await _run_portfolio_check(update)

    def _detail_report(symbol: str, market: str | None) -> str:
        status = technical_service.detail(symbol, market)
        return format_detail_report(status)

    def _chart_png(symbol: str, market: str | None) -> bytes | None:
        _, _, bars = technical_service.bars(symbol, market)
        if bars is None:
            return None
        return render_price_chart(bars, symbol)

    async def _build_chart(symbol: str, market: str | None) -> bytes | None:
        try:
            return await asyncio.to_thread(_chart_png, symbol, market)
        except Exception:  # a chart failure should never block the text report
            logging.exception("chart render failed for %s", symbol)
            return None

    async def _send_detail(message, symbol: str, market: str | None) -> None:
        try:
            report = await asyncio.to_thread(_detail_report, symbol, market)
        except Exception:
            logging.exception("stock detail failed for %s", symbol)
            await message.reply_text("Stock lookup failed. See logs.")
            return
        chart = await _build_chart(symbol, market)
        if chart is not None:
            try:
                await message.reply_photo(photo=chart)
            except Exception:
                logging.exception("sending chart failed for %s", symbol)
        await message.reply_text(report, parse_mode=ParseMode.HTML)

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
        await _send_detail(update.message, symbol, market)

    async def add_holding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not (await _guard(config, update) and update.message):
            return
        args = list(context.args or [])
        if len(args) < 2 or not args[0].strip():
            await update.message.reply_text(ADD_USAGE)
            return
        symbol = args[0].strip().upper()
        avg_price = _parse_positive(args[1])
        if avg_price is None:
            await update.message.reply_text("AVG_PRICE must be a positive number.")
            return
        market: str | None = None
        stop_loss: float | None = None
        for extra in args[2:]:
            token = extra.lower()
            if token in {"us", "india"}:
                market = token
            elif token.startswith("sl="):
                stop_loss = _parse_positive(token[3:])
                if stop_loss is None:
                    await update.message.reply_text("STOP must be a positive number.")
                    return
            else:
                await update.message.reply_text(ADD_USAGE)
                return
        market = market or _infer_market(symbol)
        try:
            saved = await asyncio.to_thread(
                portfolio_repo.upsert, symbol, market, avg_price, stop_loss
            )
        except Exception:
            logging.exception("adding holding %s failed", symbol)
            await update.message.reply_text("Portfolio update failed. See logs.")
            return
        _sync_saved_holding(config, saved)
        stop_text = saved["stop_loss"] if saved["stop_loss"] is not None else "none"
        await update.message.reply_text(
            f"Saved {saved['symbol']} ({saved['market']}): "
            f"avg {saved['avg_price']}, stop {stop_text}, "
            f"ruleset {saved['ruleset']}"
        )

    async def remove_holding(
        update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not (await _guard(config, update) and update.message):
            return
        args = list(context.args or [])
        if not args or not args[0].strip():
            await update.message.reply_text(REMOVE_USAGE)
            return
        symbol = args[0].strip().upper()
        try:
            removed = await asyncio.to_thread(portfolio_repo.remove, symbol)
        except Exception:
            logging.exception("removing holding %s failed", symbol)
            await update.message.reply_text("Portfolio update failed. See logs.")
            return
        if not removed:
            await update.message.reply_text(f"{symbol} is not in the portfolio.")
            return
        _sync_removed_holding(config, symbol)
        await update.message.reply_text(f"Removed {symbol}.")

    async def set_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not (await _guard(config, update) and update.message):
            return
        args = list(context.args or [])
        if len(args) < 2 or not args[0].strip():
            await update.message.reply_text(SETSTOP_USAGE)
            return
        symbol = args[0].strip().upper()
        stop_loss = _parse_positive(args[1])
        if stop_loss is None:
            await update.message.reply_text("STOP must be a positive number.")
            return
        try:
            updated = await asyncio.to_thread(
                portfolio_repo.set_stop, symbol, stop_loss
            )
        except Exception:
            logging.exception("updating stop for %s failed", symbol)
            await update.message.reply_text("Portfolio update failed. See logs.")
            return
        if not updated:
            await update.message.reply_text(f"{symbol} is not in the portfolio.")
            return
        _sync_stop_loss(config, symbol, stop_loss)
        await update.message.reply_text(f"Stop for {symbol} set to {stop_loss}.")

    async def alerts_command(
        update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not (await _guard(config, update) and update.message):
            return
        if alert_lock.locked():
            logging.warning("alert check skipped: previous run still in progress")
            await update.message.reply_text("Alert check already running.")
            return
        async with alert_lock:
            await update.message.reply_text("Checking for alerts...")
            try:
                report = await asyncio.to_thread(alert_service.evaluate)
            except Exception:
                logging.exception("manual alert check failed")
                await update.message.reply_text("Alert check failed. See logs.")
                return
            if not report:
                await update.message.reply_text("No changes since last check.")
                return
            for message in split_messages(report):
                await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    async def _alert_check(context: ContextTypes.DEFAULT_TYPE) -> None:
        if alert_lock.locked():
            logging.warning("alert check skipped: previous run still in progress")
            return
        async with alert_lock:
            try:
                report = await asyncio.to_thread(alert_service.evaluate)
            except Exception as exc:
                logging.exception("scheduled alert check failed")
                await notifier.notify(context.bot, "portfolio-alerts", exc)
                return
            if not report:
                return
            for chat_id in alert_service.chat_ids():
                for message in split_messages(report):
                    try:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=message,
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        logging.exception(
                            "failed to send alert message to chat %s", chat_id
                        )

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
        if query.message:
            await _send_detail(query.message, symbol, market or None)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("run", run))
    app.add_handler(CommandHandler("run_all", run_all))
    app.add_handler(CommandHandler("check_portfolio", check_portfolio))
    app.add_handler(CommandHandler("stock", stock))
    app.add_handler(CommandHandler("add", add_holding))
    app.add_handler(CommandHandler("remove", remove_holding))
    app.add_handler(CommandHandler("setstop", set_stop))
    app.add_handler(CommandHandler("alerts", alerts_command))

    # -- Paper Trading Command Handlers --

    async def paper_status_cmd(
        update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not (await _guard(config, update) and update.message):
            return
        name = context.args[0] if context.args else None
        await update.message.reply_text("Fetching paper status...")
        try:
            if name:
                status = await asyncio.to_thread(
                    paper_engine.get_portfolio_status, name
                )
                if status is None:
                    await update.message.reply_text(
                        f"Portfolio '{name}' not found."
                    )
                    return
                report = format_portfolio_status(status)
            else:
                statuses = await asyncio.to_thread(
                    paper_engine.get_all_portfolios_status
                )
                if not statuses:
                    await update.message.reply_text("No paper portfolios configured.")
                    return
                parts = [format_portfolio_status(s) for s in statuses]
                report = "\n\n".join(parts)
        except Exception:
            logging.exception("paper_status failed")
            await update.message.reply_text("Paper status check failed.")
            return
        for msg in split_messages(report):
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

    async def paper_portfolios_cmd(
        update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not (await _guard(config, update) and update.message):
            return
        try:
            statuses = await asyncio.to_thread(
                paper_engine.get_all_portfolios_status
            )
            report = format_portfolios_list(statuses)
        except Exception:
            logging.exception("paper_portfolios failed")
            await update.message.reply_text("Failed to list paper portfolios.")
            return
        await update.message.reply_text(report, parse_mode=ParseMode.HTML)

    async def paper_trades_cmd(
        update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not (await _guard(config, update) and update.message):
            return
        if not context.args:
            await update.message.reply_text(
                "Usage: /paper_trades PORTFOLIO_NAME [LIMIT]"
            )
            return
        name = context.args[0]
        limit = 10
        if len(context.args) > 1:
            try:
                limit = int(context.args[1])
            except ValueError:
                pass
        try:
            pf = await asyncio.to_thread(
                paper_engine._store.fetch_portfolio_by_name, name
            )
            if pf is None:
                await update.message.reply_text(f"Portfolio '{name}' not found.")
                return
            trades = await asyncio.to_thread(
                paper_engine._store.fetch_trades, pf["id"], limit
            )
            report = format_trades(trades, name, pf["market"])
        except Exception:
            logging.exception("paper_trades failed")
            await update.message.reply_text("Failed to fetch trades.")
            return
        for msg in split_messages(report):
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

    async def paper_enable_cmd(
        update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not (await _guard(config, update) and update.message):
            return
        if not context.args:
            await update.message.reply_text("Usage: /paper_enable PORTFOLIO_NAME")
            return
        name = context.args[0]
        try:
            updated = await asyncio.to_thread(
                paper_engine._store.update_portfolio_enabled, name, True
            )
        except Exception:
            logging.exception("paper_enable failed")
            await update.message.reply_text("Failed to enable portfolio.")
            return
        if updated:
            await update.message.reply_text(f"✅ Paper portfolio '{name}' enabled.")
        else:
            await update.message.reply_text(f"Portfolio '{name}' not found.")

    async def paper_disable_cmd(
        update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not (await _guard(config, update) and update.message):
            return
        if not context.args:
            await update.message.reply_text("Usage: /paper_disable PORTFOLIO_NAME")
            return
        name = context.args[0]
        try:
            updated = await asyncio.to_thread(
                paper_engine._store.update_portfolio_enabled, name, False
            )
        except Exception:
            logging.exception("paper_disable failed")
            await update.message.reply_text("Failed to disable portfolio.")
            return
        if updated:
            await update.message.reply_text(f"❌ Paper portfolio '{name}' disabled.")
        else:
            await update.message.reply_text(f"Portfolio '{name}' not found.")

    async def paper_reset_cmd(
        update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not (await _guard(config, update) and update.message):
            return
        if not context.args:
            await update.message.reply_text("Usage: /paper_reset PORTFOLIO_NAME")
            return
        name = context.args[0]
        try:
            pf = await asyncio.to_thread(
                paper_engine._store.fetch_portfolio_by_name, name
            )
            if pf is None:
                await update.message.reply_text(f"Portfolio '{name}' not found.")
                return
            await asyncio.to_thread(
                paper_engine._store.reset_portfolio,
                pf["id"],
                pf["initial_capital"],
            )
        except Exception:
            logging.exception("paper_reset failed")
            await update.message.reply_text("Failed to reset portfolio.")
            return
        await update.message.reply_text(
            f"🔄 Paper portfolio '{name}' reset to "
            f"initial capital {pf['initial_capital']:,.0f}."
        )

    app.add_handler(CommandHandler("paper_status", paper_status_cmd))
    app.add_handler(CommandHandler("paper_portfolios", paper_portfolios_cmd))
    app.add_handler(CommandHandler("paper_trades", paper_trades_cmd))
    app.add_handler(CommandHandler("paper_enable", paper_enable_cmd))
    app.add_handler(CommandHandler("paper_disable", paper_disable_cmd))
    app.add_handler(CommandHandler("paper_reset", paper_reset_cmd))

    app.add_handler(
        CallbackQueryHandler(detail_callback, pattern=f"^{CALLBACK_DETAIL}\\|")
    )
    app.post_init = _post_init(
        config, screener_service, _scheduled_portfolio_check, _alert_check,
        notifier, paper_engine,
    )
    return app


async def _register_commands(app: Application) -> None:
    await app.bot.set_my_commands(BOT_COMMANDS)


def _post_init(
    config: BotConfig,
    screener_service: ScheduledScreenerService,
    portfolio_callback,
    alert_callback,
    notifier: _AdminNotifier | None = None,
    paper_engine: PaperTradingEngine | None = None,
):
    async def post_init(app: Application) -> None:
        await _register_commands(app)
        _schedule_screener_jobs(app, config, screener_service, notifier)
        _schedule_portfolio_jobs(app, config, portfolio_callback)
        _schedule_alert_jobs(app, config, alert_callback)
        if paper_engine is not None:
            _schedule_paper_trading_jobs(app, config, paper_engine, notifier)

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


def _schedule_alert_jobs(app: Application, config: BotConfig, callback) -> None:
    if not config.alerts.enabled:
        return
    if app.job_queue is None:
        logging.warning("alerts disabled: application has no job queue")
        return
    interval = config.alerts.interval_minutes * 60
    app.job_queue.run_repeating(
        callback,
        interval=interval,
        first=60,
        name="portfolio-alerts",
    )


def _schedule_screener_jobs(
    app: Application,
    config: BotConfig,
    screener_service: ScheduledScreenerService,
    notifier: _AdminNotifier | None = None,
) -> None:
    scheduled = config.scheduled_screener
    if not scheduled.enabled or not scheduled.times:
        return
    if app.job_queue is None:
        logging.warning("scheduled screener disabled: application has no job queue")
        return

    async def callback(context: ContextTypes.DEFAULT_TYPE) -> None:
        await _scheduled_screener_callback(context, notifier)

    tz = ZoneInfo(config.timezone)
    for item in scheduled.times:
        hour, minute = (int(part) for part in item.split(":"))
        run_time = time(hour=hour, minute=minute, tzinfo=tz)
        app.job_queue.run_daily(
            callback,
            time=run_time,
            data=screener_service,
            name=f"scheduled-screener-{item}",
        )


async def _scheduled_screener_callback(
    context: ContextTypes.DEFAULT_TYPE,
    notifier: _AdminNotifier | None = None,
) -> None:
    service = context.job.data
    if not isinstance(service, ScheduledScreenerService):
        logging.error("scheduled screener job missing service")
        return
    try:
        await send_screener_report(context, service)
    except Exception as exc:
        logging.exception("scheduled screener job failed")
        if notifier is not None:
            await notifier.notify(context.bot, "scheduled-screener", exc)


def _scheduled_status(config: BotConfig) -> str:
    scheduled = config.scheduled_screener
    if not scheduled.enabled:
        return "disabled"
    if not scheduled.times:
        return "enabled, no times configured"
    return "enabled at " + ", ".join(scheduled.times)


def _alerts_status(config: BotConfig) -> str:
    alerts = config.alerts
    if not alerts.enabled:
        return "disabled"
    return f"enabled, every {alerts.interval_minutes}m"


# ---------------------------------------------------------------------------
# Paper Trading Scheduled Jobs
# ---------------------------------------------------------------------------

# Schedule times (IST)
_INDIA_EVENING_TIME = "16:00"   # India market close
_US_EVENING_TIME = "02:30"      # US market close (IST)
_INDIA_MORNING_TIME = "09:20"   # India market open
_US_MORNING_TIME = "15:00"      # US market open (IST)
_PAPER_DAILY_SUMMARY_TIME = "18:00"
_PAPER_WEEKLY_SUMMARY_DAY = 6   # Sunday
_PAPER_WEEKLY_SUMMARY_TIME = "10:00"


def _schedule_paper_trading_jobs(
    app: Application,
    config: BotConfig,
    paper_engine: PaperTradingEngine,
    notifier: _AdminNotifier | None = None,
) -> None:
    """Register all paper trading scheduled jobs."""
    if not config.paper_trading.portfolios:
        logging.info("paper trading: no portfolios configured, skipping scheduling")
        return
    if app.job_queue is None:
        logging.warning("paper trading disabled: application has no job queue")
        return

    tz = ZoneInfo(config.timezone)
    targets = (
        config.scheduled_screener.chat_ids or config.telegram.allowed_chat_ids
    )

    # Determine which markets are active
    markets = {pf.market for pf in config.paper_trading.portfolios.values() if pf.enabled}

    async def _evening_callback(
        context: ContextTypes.DEFAULT_TYPE, market: str = "india"
    ) -> None:
        try:
            # Run evening signals for portfolios of this market
            reports = await asyncio.to_thread(
                paper_engine.run_evening_signals
            )
            # Filter to this market's portfolios
            market_reports = [r for r in reports if r.market == market]
            if market_reports:
                pending_count = sum(
                    len(r.actions) for r in market_reports
                )
                if pending_count > 0:
                    summary = (
                        f"📋 Paper Trading ({market.upper()}): "
                        f"{pending_count} pending orders created for tomorrow's fill."
                    )
                    for chat_id in targets:
                        try:
                            await context.bot.send_message(
                                chat_id=chat_id, text=summary
                            )
                        except Exception:
                            logging.exception(
                                "failed to send paper evening summary to %s", chat_id
                            )
        except Exception as exc:
            logging.exception("paper evening signals failed for %s", market)
            if notifier:
                await notifier.notify(
                    context.bot, f"paper-evening-{market}", exc
                )

    async def _morning_callback(
        context: ContextTypes.DEFAULT_TYPE, market: str = "india"
    ) -> None:
        try:
            reports = await asyncio.to_thread(
                paper_engine.run_morning_fills
            )
            market_reports = [r for r in reports if r.market == market]
            if market_reports and any(r.actions for r in market_reports):
                report_text = format_daily_report(market_reports)
                for chat_id in targets:
                    for msg in split_messages(report_text):
                        try:
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=msg,
                                parse_mode=ParseMode.HTML,
                            )
                        except Exception:
                            logging.exception(
                                "failed to send paper morning report to %s", chat_id
                            )
        except Exception as exc:
            logging.exception("paper morning fills failed for %s", market)
            if notifier:
                await notifier.notify(
                    context.bot, f"paper-morning-{market}", exc
                )

    async def _daily_summary_callback(
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        try:
            statuses = await asyncio.to_thread(
                paper_engine.get_all_portfolios_status
            )
            if not statuses:
                return
            report = format_portfolios_list(statuses)
            for chat_id in targets:
                for msg in split_messages(report):
                    try:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=msg,
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        logging.exception(
                            "failed to send paper daily summary to %s", chat_id
                        )
        except Exception as exc:
            logging.exception("paper daily summary failed")
            if notifier:
                await notifier.notify(context.bot, "paper-daily-summary", exc)

    async def _weekly_summary_callback(
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        try:
            from datetime import timedelta

            statuses = await asyncio.to_thread(
                paper_engine.get_all_portfolios_status
            )
            if not statuses:
                return
            # Gather trades from the past week
            weekly_trades: dict[str, list[dict]] = {}
            for status in statuses:
                pf = status["portfolio"]
                all_trades = await asyncio.to_thread(
                    paper_engine._store.fetch_all_trades, pf["id"]
                )
                week_ago = (
                    datetime.now(ZoneInfo(config.timezone)) - timedelta(days=7)
                ).date().isoformat()
                recent = [
                    t for t in all_trades if t["exit_date"] >= week_ago
                ]
                weekly_trades[pf["name"]] = recent
            report = format_weekly_report(statuses, weekly_trades)
            for chat_id in targets:
                for msg in split_messages(report):
                    try:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=msg,
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        logging.exception(
                            "failed to send paper weekly summary to %s", chat_id
                        )
        except Exception as exc:
            logging.exception("paper weekly summary failed")
            if notifier:
                await notifier.notify(context.bot, "paper-weekly-summary", exc)

    # Schedule evening signal evaluation
    if "india" in markets:
        h, m = (int(p) for p in _INDIA_EVENING_TIME.split(":"))
        app.job_queue.run_daily(
            lambda ctx: asyncio.ensure_future(_evening_callback(ctx, "india")),
            time=time(hour=h, minute=m, tzinfo=tz),
            name="paper-evening-india",
        )
        h, m = (int(p) for p in _INDIA_MORNING_TIME.split(":"))
        app.job_queue.run_daily(
            lambda ctx: asyncio.ensure_future(_morning_callback(ctx, "india")),
            time=time(hour=h, minute=m, tzinfo=tz),
            name="paper-morning-india",
        )

    if "us" in markets:
        h, m = (int(p) for p in _US_EVENING_TIME.split(":"))
        app.job_queue.run_daily(
            lambda ctx: asyncio.ensure_future(_evening_callback(ctx, "us")),
            time=time(hour=h, minute=m, tzinfo=tz),
            name="paper-evening-us",
        )
        h, m = (int(p) for p in _US_MORNING_TIME.split(":"))
        app.job_queue.run_daily(
            lambda ctx: asyncio.ensure_future(_morning_callback(ctx, "us")),
            time=time(hour=h, minute=m, tzinfo=tz),
            name="paper-morning-us",
        )

    # Daily summary
    h, m = (int(p) for p in _PAPER_DAILY_SUMMARY_TIME.split(":"))
    app.job_queue.run_daily(
        _daily_summary_callback,
        time=time(hour=h, minute=m, tzinfo=tz),
        name="paper-daily-summary",
    )

    # Weekly summary (Sunday)
    h, m = (int(p) for p in _PAPER_WEEKLY_SUMMARY_TIME.split(":"))
    app.job_queue.run_daily(
        _weekly_summary_callback,
        time=time(hour=h, minute=m, tzinfo=tz),
        days=(_PAPER_WEEKLY_SUMMARY_DAY,),
        name="paper-weekly-summary",
    )

    logging.info(
        "paper trading: scheduled %d jobs for markets %s",
        sum([
            2 * len(markets),  # evening + morning per market
            1,  # daily summary
            1,  # weekly summary
        ]),
        markets,
    )
