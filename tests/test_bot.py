from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest
from telegram.ext import CallbackQueryHandler

from screener_bot import bot as botmod
from screener_bot.bot import (
    BOT_COMMANDS,
    HELP_TEXT,
    _alerts_status,
    _authorized,
    _guard,
    _holdings_keyboard,
    _register_commands,
    _schedule_alert_jobs,
    _schedule_portfolio_jobs,
    _schedule_screener_jobs,
    _scheduled_screener_callback,
    _scheduled_status,
    build_application,
)
from screener_bot.config import BotConfig, EnvSettings, PortfolioItem
from screener_bot.scheduled_screener import ScheduledScreenerService
from screener_bot.technical import DetailStatus, RuleStatus, TechnicalStatus


# --- config / stub services ------------------------------------------------


def _config(**overrides) -> BotConfig:
    base = {
        "telegram": {"allowed_chat_ids": [1]},
        "portfolio": [
            {"symbol": "NSE:RELIANCE", "market": "india", "ruleset": "x"},
            {"symbol": "AAPL", "market": "us", "ruleset": "x"},
        ],
        "rulesets": {"x": {}},
    }
    base.update(overrides)
    return BotConfig.model_validate(base)


class StubTechnical:
    def __init__(
        self, *, statuses=None, detail=None, bars=(None, None, None), raises=False
    ):
        self._statuses = statuses or []
        self._detail = detail
        self._bars = bars
        self._raises = raises

    def check_portfolio(self):
        if self._raises:
            raise RuntimeError("boom")
        return self._statuses

    def detail(self, symbol, market=None):
        if self._raises:
            raise RuntimeError("boom")
        return self._detail or DetailStatus(symbol=symbol, market=market)

    def bars(self, symbol, market=None):
        return self._bars


class StubOwnership:
    def check_portfolio(self, items):
        return {}


class StubScreener:
    def __init__(self, *, report="<b>Screener</b>", raises=False):
        self.report = report
        self.raises = raises
        self.calls: list[tuple] = []

    async def run(self, query=None, full_list=False):
        self.calls.append((query, full_list))
        if self.raises:
            raise RuntimeError("boom")
        return self.report

    def chat_ids(self):
        return [1]


class StubAlert:
    def __init__(self, *, report=None, raises=False):
        self.report = report
        self.raises = raises

    def evaluate(self):
        if self.raises:
            raise RuntimeError("boom")
        return self.report

    def chat_ids(self):
        return [1]


def _settings(token="123:ABC") -> EnvSettings:
    return EnvSettings(telegram_bot_token=token)


def _build(*, config=None, technical=None, ownership=None, screener=None, alert=None):
    return build_application(
        _settings(),
        config or _config(),
        technical or StubTechnical(),
        ownership or StubOwnership(),
        screener or StubScreener(),
        alert or StubAlert(),
    )


# --- fakes for telegram Update / Context -----------------------------------


class FakeMessage:
    def __init__(self):
        self.reply_text = AsyncMock()
        self.reply_photo = AsyncMock()


def make_update(*, chat_id=1, message=True, callback_data=None):
    msg = FakeMessage() if message else None
    cq = None
    if callback_data is not None:
        cq = SimpleNamespace(
            answer=AsyncMock(), data=callback_data, message=FakeMessage()
        )
    return SimpleNamespace(
        effective_chat=SimpleNamespace(id=chat_id), message=msg, callback_query=cq
    )


def make_context(args=None):
    return SimpleNamespace(
        args=list(args or []), bot=SimpleNamespace(send_message=AsyncMock())
    )


def run(coro):
    return asyncio.run(coro)


def handler(app, command):
    for group in app.handlers.values():
        for h in group:
            commands = getattr(h, "commands", None)
            if commands and command in commands:
                return h.callback
    raise KeyError(command)


def callback_handler(app):
    for group in app.handlers.values():
        for h in group:
            if isinstance(h, CallbackQueryHandler):
                return h.callback
    raise KeyError("callback")


def texts(message):
    return [call.args[0] for call in message.reply_text.await_args_list]


class FakeJobQueue:
    def __init__(self):
        self.daily: list = []
        self.repeating: list = []

    def run_daily(self, callback, *, time, name, data=None):
        self.daily.append(
            SimpleNamespace(callback=callback, time=time, name=name, data=data)
        )

    def run_repeating(self, callback, *, interval, first, name):
        self.repeating.append(
            SimpleNamespace(
                callback=callback, interval=interval, first=first, name=name
            )
        )


# --- guards / helpers ------------------------------------------------------


def test_authorized_true_false_and_no_chat() -> None:
    config = _config()
    assert _authorized(config, make_update(chat_id=1)) is True
    assert _authorized(config, make_update(chat_id=999)) is False
    no_chat = SimpleNamespace(effective_chat=None, message=None, callback_query=None)
    assert _authorized(config, no_chat) is False


def test_guard_replies_when_unauthorized() -> None:
    update = make_update(chat_id=999)
    assert run(_guard(_config(), update)) is False
    update.message.reply_text.assert_awaited_once_with("Unauthorized chat.")


def test_guard_passes_when_authorized() -> None:
    assert run(_guard(_config(), make_update(chat_id=1))) is True


def test_holdings_keyboard_groups_into_rows_of_three() -> None:
    config = _config(
        portfolio=[
            {"symbol": f"S{i}", "market": "us", "ruleset": "x"} for i in range(5)
        ]
    )
    rows = _holdings_keyboard(config).inline_keyboard
    assert len(rows) == 2
    assert len(rows[0]) == 3 and len(rows[1]) == 2
    assert rows[0][0].callback_data.startswith("d|us|S0")


def test_build_application_requires_token() -> None:
    with pytest.raises(RuntimeError):
        build_application(SimpleNamespace(telegram_bot_token=""), _config())


def test_build_application_constructs_default_services(monkeypatch) -> None:
    monkeypatch.setattr(botmod, "TechnicalService", lambda config: StubTechnical())
    monkeypatch.setattr(botmod, "OwnershipService", lambda: StubOwnership())
    monkeypatch.setattr(
        botmod, "ScheduledScreenerService", lambda config: StubScreener()
    )
    monkeypatch.setattr(botmod, "AlertService", lambda config, technical: StubAlert())
    app = build_application(_settings(), _config())
    assert handler(app, "start") is not None


# --- simple command handlers -----------------------------------------------


def test_start_handler() -> None:
    app = _build()
    update = make_update()
    run(handler(app, "start")(update, make_context()))
    assert "ready" in texts(update.message)[0]


def test_help_handler() -> None:
    app = _build()
    update = make_update()
    run(handler(app, "help")(update, make_context()))
    assert texts(update.message)[0] == HELP_TEXT


def test_status_handler() -> None:
    config = _config(
        scheduled_screener={
            "enabled": True,
            "times": ["16:00"],
            "commands": [{"label": "t", "command": ["true"]}],
        },
        alerts={"enabled": True, "interval_minutes": 30},
    )
    app = _build(config=config)
    update = make_update()
    run(handler(app, "status")(update, make_context()))
    text = texts(update.message)[0]
    assert "Configured holdings: 2" in text
    assert "enabled at 16:00" in text
    assert "every 30m" in text


def test_handler_blocks_unauthorized_chat() -> None:
    app = _build()
    update = make_update(chat_id=999)
    run(handler(app, "status")(update, make_context()))
    update.message.reply_text.assert_awaited_once_with("Unauthorized chat.")


# --- check_portfolio -------------------------------------------------------


def _holding_status(close=100.0):
    item = PortfolioItem(symbol="AAPL", market="us", ruleset="x")
    status = TechnicalStatus(item=item, ticker="AAPL", close=close)
    status.entry = RuleStatus(True)
    status.exit = RuleStatus(False)
    return status


def test_check_portfolio_sends_report() -> None:
    app = _build(technical=StubTechnical(statuses=[_holding_status()]))
    update = make_update()
    run(handler(app, "check_portfolio")(update, make_context()))
    sent = texts(update.message)
    assert sent[0] == "Checking portfolio..."
    assert len(sent) >= 2


def test_check_portfolio_reports_failure() -> None:
    app = _build(technical=StubTechnical(raises=True))
    update = make_update()
    run(handler(app, "check_portfolio")(update, make_context()))
    assert "Portfolio check failed. See logs." in texts(update.message)


def test_check_portfolio_no_message_is_noop() -> None:
    app = _build()
    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=1), message=None, callback_query=None
    )
    run(handler(app, "check_portfolio")(update, make_context()))  # must not raise


# --- run / run_all ---------------------------------------------------------


def test_run_handler_with_query() -> None:
    screener = StubScreener(report="<b>Changes</b>")
    app = _build(screener=screener)
    update = make_update()
    context = make_context(args=["india", "ema"])
    run(handler(app, "run")(update, context))
    assert screener.calls == [("india ema", False)]
    assert "india ema" in texts(update.message)[0]
    context.bot.send_message.assert_awaited()


def test_run_handler_without_args() -> None:
    screener = StubScreener()
    app = _build(screener=screener)
    update = make_update()
    run(handler(app, "run")(update, make_context()))
    assert screener.calls == [(None, False)]
    assert texts(update.message)[0] == "Running screener changes..."


def test_run_handler_reports_failure() -> None:
    app = _build(screener=StubScreener(raises=True))
    update = make_update()
    run(handler(app, "run")(update, make_context()))
    assert "Screener run failed. See logs." in texts(update.message)


def test_run_all_handler_with_query() -> None:
    screener = StubScreener()
    app = _build(screener=screener)
    update = make_update()
    run(handler(app, "run_all")(update, make_context(args=["india"])))
    assert screener.calls == [("india", True)]
    assert "india" in texts(update.message)[0]


def test_run_all_handler_without_args_and_failure() -> None:
    app = _build(screener=StubScreener(raises=True))
    update = make_update()
    run(handler(app, "run_all")(update, make_context()))
    sent = texts(update.message)
    assert sent[0] == "Running full screener..."
    assert "Screener run failed. See logs." in sent


# --- stock -----------------------------------------------------------------


def test_stock_handler_unauthorized() -> None:
    app = _build()
    update = make_update(chat_id=999)
    run(handler(app, "stock")(update, make_context(args=["AAPL"])))
    update.message.reply_text.assert_awaited_once_with("Unauthorized chat.")


def test_stock_handler_usage_without_args() -> None:
    app = _build()
    update = make_update()
    run(handler(app, "stock")(update, make_context()))
    assert "Usage:" in texts(update.message)[0]


def test_stock_handler_sends_detail_with_market_hint() -> None:
    detail = DetailStatus(symbol="AAPL", market="us", close=190.0)
    app = _build(technical=StubTechnical(detail=detail))
    update = make_update()
    run(handler(app, "stock")(update, make_context(args=["AAPL", "us"])))
    sent = texts(update.message)
    assert sent[0] == "Fetching AAPL..."
    assert any("AAPL" in t for t in sent[1:])
    update.message.reply_photo.assert_not_awaited()


def _chart_bars():
    idx = pd.date_range("2025-01-01", periods=30)
    close = pd.Series([1.5] * 30, index=idx, dtype=float)
    return pd.DataFrame(
        {
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": 100.0,
        },
        index=idx,
    )


def test_stock_handler_sends_chart(monkeypatch) -> None:
    monkeypatch.setattr(botmod, "render_price_chart", lambda bars, symbol: b"PNGDATA")
    detail = DetailStatus(symbol="AAPL", market="us", close=1.5)
    app = _build(
        technical=StubTechnical(detail=detail, bars=("us", "AAPL", _chart_bars()))
    )
    update = make_update()
    run(handler(app, "stock")(update, make_context(args=["aapl"])))
    update.message.reply_photo.assert_awaited_once()
    assert update.message.reply_photo.await_args.kwargs["photo"] == b"PNGDATA"


def test_stock_handler_detail_failure() -> None:
    app = _build(technical=StubTechnical(raises=True))
    update = make_update()
    run(handler(app, "stock")(update, make_context(args=["AAPL"])))
    assert "Stock lookup failed. See logs." in texts(update.message)


def test_stock_chart_render_failure_is_swallowed(monkeypatch) -> None:
    def boom(bars, symbol):
        raise RuntimeError("render fail")

    monkeypatch.setattr(botmod, "render_price_chart", boom)
    detail = DetailStatus(symbol="AAPL", market="us", close=1.5)
    app = _build(
        technical=StubTechnical(detail=detail, bars=("us", "AAPL", _chart_bars()))
    )
    update = make_update()
    run(handler(app, "stock")(update, make_context(args=["AAPL"])))
    update.message.reply_photo.assert_not_awaited()
    assert any("AAPL" in t for t in texts(update.message))


def test_stock_send_chart_failure_is_swallowed(monkeypatch) -> None:
    monkeypatch.setattr(botmod, "render_price_chart", lambda bars, symbol: b"PNG")
    detail = DetailStatus(symbol="AAPL", market="us", close=1.5)
    app = _build(
        technical=StubTechnical(detail=detail, bars=("us", "AAPL", _chart_bars()))
    )
    update = make_update()
    update.message.reply_photo.side_effect = RuntimeError("send fail")
    run(handler(app, "stock")(update, make_context(args=["AAPL"])))
    assert any("AAPL" in t for t in texts(update.message))


# --- alerts ----------------------------------------------------------------


def test_alerts_command_unauthorized() -> None:
    app = _build()
    update = make_update(chat_id=999)
    run(handler(app, "alerts")(update, make_context()))
    update.message.reply_text.assert_awaited_once_with("Unauthorized chat.")


def test_alerts_command_reports_changes() -> None:
    app = _build(alert=StubAlert(report="<b>Alerts</b> changed"))
    update = make_update()
    run(handler(app, "alerts")(update, make_context()))
    sent = texts(update.message)
    assert sent[0] == "Checking for alerts..."
    assert any("Alerts" in t for t in sent[1:])


def test_alerts_command_no_changes() -> None:
    app = _build(alert=StubAlert(report=None))
    update = make_update()
    run(handler(app, "alerts")(update, make_context()))
    assert "No changes since last check." in texts(update.message)


def test_alerts_command_failure() -> None:
    app = _build(alert=StubAlert(raises=True))
    update = make_update()
    run(handler(app, "alerts")(update, make_context()))
    assert "Alert check failed. See logs." in texts(update.message)


# --- detail callback -------------------------------------------------------


def test_detail_callback_sends_detail() -> None:
    detail = DetailStatus(symbol="AAPL", market="us", close=10.0)
    app = _build(technical=StubTechnical(detail=detail))
    update = make_update(callback_data="d|us|AAPL")
    run(callback_handler(app)(update, make_context()))
    update.callback_query.answer.assert_awaited()
    assert update.callback_query.message.reply_text.await_count >= 1


def test_detail_callback_unauthorized() -> None:
    app = _build()
    update = make_update(chat_id=999, callback_data="d|us|AAPL")
    run(callback_handler(app)(update, make_context()))
    update.callback_query.answer.assert_awaited_once_with(
        "Unauthorized chat.", show_alert=True
    )


def test_detail_callback_none_query_is_noop() -> None:
    app = _build()
    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=1), message=None, callback_query=None
    )
    run(callback_handler(app)(update, make_context()))  # must not raise


def test_detail_callback_bad_data_is_ignored() -> None:
    app = _build()
    update = make_update(callback_data="garbage")
    run(callback_handler(app)(update, make_context()))
    update.callback_query.answer.assert_awaited()
    update.callback_query.message.reply_text.assert_not_awaited()


def test_detail_callback_without_message() -> None:
    app = _build()
    cq = SimpleNamespace(answer=AsyncMock(), data="d|us|AAPL", message=None)
    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=1), message=None, callback_query=cq
    )
    run(callback_handler(app)(update, make_context()))
    cq.answer.assert_awaited()


# --- scheduling: standalone branches ---------------------------------------


def test_schedule_portfolio_jobs_without_queue() -> None:
    _schedule_portfolio_jobs(SimpleNamespace(job_queue=None), _config(), AsyncMock())


def test_schedule_alert_jobs_disabled() -> None:
    config = _config(alerts={"enabled": False})
    app = SimpleNamespace(job_queue=FakeJobQueue())
    _schedule_alert_jobs(app, config, AsyncMock())
    assert app.job_queue.repeating == []


def test_schedule_alert_jobs_without_queue() -> None:
    _schedule_alert_jobs(SimpleNamespace(job_queue=None), _config(), AsyncMock())


def test_schedule_screener_jobs_disabled() -> None:
    config = _config(scheduled_screener={"enabled": False})
    app = SimpleNamespace(job_queue=FakeJobQueue())
    _schedule_screener_jobs(app, config, ScheduledScreenerService(config))
    assert app.job_queue.daily == []


def test_schedule_screener_jobs_without_queue() -> None:
    config = _config(
        scheduled_screener={
            "enabled": True,
            "times": ["16:00"],
            "commands": [{"label": "t", "command": ["true"]}],
        }
    )
    _schedule_screener_jobs(
        SimpleNamespace(job_queue=None), config, ScheduledScreenerService(config)
    )


def test_register_commands() -> None:
    app = SimpleNamespace(bot=SimpleNamespace(set_my_commands=AsyncMock()))
    run(_register_commands(app))
    app.bot.set_my_commands.assert_awaited_once_with(BOT_COMMANDS)


def test_scheduled_status_variants() -> None:
    assert (
        _scheduled_status(_config(scheduled_screener={"enabled": False})) == "disabled"
    )
    assert (
        _scheduled_status(_config(scheduled_screener={"enabled": True}))
        == "enabled, no times configured"
    )
    enabled = _config(scheduled_screener={"enabled": True, "times": ["16:00", "02:30"]})
    assert _scheduled_status(enabled) == "enabled at 16:00, 02:30"


def test_alerts_status_variants() -> None:
    assert _alerts_status(_config(alerts={"enabled": False})) == "disabled"
    assert (
        _alerts_status(_config(alerts={"enabled": True, "interval_minutes": 45}))
        == "enabled, every 45m"
    )


# --- scheduled screener callback (standalone) ------------------------------


def test_scheduled_screener_callback_missing_service() -> None:
    context = SimpleNamespace(
        job=SimpleNamespace(data="not a service"),
        bot=SimpleNamespace(send_message=AsyncMock()),
    )
    run(_scheduled_screener_callback(context))
    context.bot.send_message.assert_not_awaited()


def test_scheduled_screener_callback_handles_failure(monkeypatch, tmp_path) -> None:
    config = _config(
        scheduled_screener={"commands": [{"label": "t", "command": ["true"]}]}
    )
    service = ScheduledScreenerService(config, tmp_path / "s.json")

    async def boom(*args, **kwargs):
        raise RuntimeError("send failed")

    monkeypatch.setattr(botmod, "send_screener_report", boom)
    context = SimpleNamespace(
        job=SimpleNamespace(data=service), bot=SimpleNamespace(send_message=AsyncMock())
    )
    run(_scheduled_screener_callback(context))  # must swallow the failure


# --- post_init wires + runs every scheduled callback -----------------------


def test_post_init_schedules_and_invokes_callbacks(monkeypatch, tmp_path) -> None:
    config = _config(
        scheduled_screener={
            "enabled": True,
            "times": ["16:00", "02:30"],
            "commands": [
                {
                    "label": "India EMA",
                    "command": [
                        sys.executable,
                        "-c",
                        "print('name,close,change,setup_score');print('AAA,1,1,1')",
                    ],
                }
            ],
        },
        alerts={"enabled": True, "interval_minutes": 30},
    )
    real_screener = ScheduledScreenerService(config, tmp_path / "snap.json")
    app = _build(
        config=config,
        technical=StubTechnical(statuses=[_holding_status()]),
        screener=real_screener,
        alert=StubAlert(report="<b>Alert</b>"),
    )

    fake_jq = FakeJobQueue()
    app._job_queue = fake_jq
    monkeypatch.setattr(botmod, "_register_commands", AsyncMock())

    run(app.post_init(app))

    botmod._register_commands.assert_awaited_once()
    daily_names = {job.name for job in fake_jq.daily}
    assert "scheduled-screener-16:00" in daily_names
    assert "scheduled-screener-02:30" in daily_names
    assert "scheduled-portfolio-check" in daily_names
    assert [job.name for job in fake_jq.repeating] == ["portfolio-alerts"]

    # scheduled screener callback
    screener_job = next(
        j for j in fake_jq.daily if j.name == "scheduled-screener-16:00"
    )
    sctx = SimpleNamespace(
        bot=SimpleNamespace(send_message=AsyncMock()),
        job=SimpleNamespace(data=screener_job.data),
    )
    run(screener_job.callback(sctx))
    sctx.bot.send_message.assert_awaited()

    # scheduled portfolio check
    portfolio_job = next(
        j for j in fake_jq.daily if j.name == "scheduled-portfolio-check"
    )
    pctx = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
    run(portfolio_job.callback(pctx))
    pctx.bot.send_message.assert_awaited()

    # scheduled alert check
    alert_job = fake_jq.repeating[0]
    actx = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
    run(alert_job.callback(actx))
    actx.bot.send_message.assert_awaited()


def test_scheduled_portfolio_check_handles_failure(monkeypatch, tmp_path) -> None:
    config = _config(
        scheduled_screener={
            "enabled": True,
            "times": ["16:00"],
            "commands": [{"label": "t", "command": ["true"]}],
        }
    )
    app = _build(config=config, technical=StubTechnical(raises=True))
    fake_jq = FakeJobQueue()
    app._job_queue = fake_jq
    monkeypatch.setattr(botmod, "_register_commands", AsyncMock())
    run(app.post_init(app))

    portfolio_job = next(
        j for j in fake_jq.daily if j.name == "scheduled-portfolio-check"
    )
    pctx = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
    run(portfolio_job.callback(pctx))  # swallows failure
    pctx.bot.send_message.assert_not_awaited()


def test_scheduled_alert_check_handles_failure(monkeypatch) -> None:
    # alert raising -> swallowed; no message sent
    app = _build(alert=StubAlert(raises=True))
    fake_jq = FakeJobQueue()
    app._job_queue = fake_jq
    monkeypatch.setattr(botmod, "_register_commands", AsyncMock())
    run(app.post_init(app))
    alert_job = fake_jq.repeating[0]
    actx = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
    run(alert_job.callback(actx))
    actx.bot.send_message.assert_not_awaited()


def test_scheduled_alert_check_with_no_report(monkeypatch) -> None:
    # evaluate() returning None -> early return, no message sent
    app = _build(alert=StubAlert(report=None))
    fake_jq = FakeJobQueue()
    app._job_queue = fake_jq
    monkeypatch.setattr(botmod, "_register_commands", AsyncMock())
    run(app.post_init(app))
    alert_job = fake_jq.repeating[0]
    actx = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
    run(alert_job.callback(actx))
    actx.bot.send_message.assert_not_awaited()
