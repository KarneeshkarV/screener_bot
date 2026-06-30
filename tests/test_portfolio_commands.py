from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from screener_bot import portfolio_store
from screener_bot.bot import (
    ADD_USAGE,
    BOT_COMMANDS,
    HELP_TEXT,
    REMOVE_USAGE,
    SETSTOP_USAGE,
    _infer_market,
    _parse_positive,
    build_application,
)
from screener_bot.config import BotConfig, EnvSettings


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
    def check_portfolio(self):
        return []

    def detail(self, symbol, market=None):  # pragma: no cover - unused here
        raise NotImplementedError

    def bars(self, symbol, market=None):  # pragma: no cover - unused here
        return None, None, None


class StubOwnership:
    def check_portfolio(self, items):
        return {}


class StubScreener:
    async def run(self, query=None, full_list=False):  # pragma: no cover
        return ""

    def chat_ids(self):
        return [1]


class StubAlert:
    def evaluate(self):
        return None

    def chat_ids(self):
        return [1]


class StubRepo:
    def __init__(self, *, removed=1, updated=1, raises=False):
        self.removed = removed
        self.updated = updated
        self.raises = raises
        self.upsert_calls: list[tuple] = []
        self.remove_calls: list[str] = []
        self.set_stop_calls: list[tuple] = []

    def upsert(
        self, symbol, market, avg_price, stop_loss=None, ruleset="swing_momentum"
    ):
        if self.raises:
            raise RuntimeError("boom")
        self.upsert_calls.append((symbol, market, avg_price, stop_loss, ruleset))
        return {
            "symbol": symbol,
            "market": market,
            "avg_price": avg_price,
            "stop_loss": stop_loss,
            "ruleset": ruleset,
        }

    def remove(self, symbol):
        if self.raises:
            raise RuntimeError("boom")
        self.remove_calls.append(symbol)
        return self.removed

    def set_stop(self, symbol, stop_loss):
        if self.raises:
            raise RuntimeError("boom")
        self.set_stop_calls.append((symbol, stop_loss))
        return self.updated


def _build(*, config=None, repo=None):
    config = config or _config()
    app = build_application(
        EnvSettings(telegram_bot_token="123:ABC"),
        config,
        StubTechnical(),
        StubOwnership(),
        StubScreener(),
        StubAlert(),
        repo or StubRepo(),
    )
    return app, config


# --- fakes for telegram Update / Context -----------------------------------


class FakeMessage:
    def __init__(self):
        self.reply_text = AsyncMock()
        self.reply_photo = AsyncMock()


def make_update(*, chat_id=1):
    return SimpleNamespace(
        effective_chat=SimpleNamespace(id=chat_id),
        message=FakeMessage(),
        callback_query=None,
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


def texts(message):
    return [call.args[0] for call in message.reply_text.await_args_list]


# --- helpers ----------------------------------------------------------------


def test_parse_positive() -> None:
    assert _parse_positive("190.5") == 190.5
    assert _parse_positive("1,250") == 1250.0
    assert _parse_positive("abc") is None
    assert _parse_positive("-5") is None
    assert _parse_positive("0") is None
    assert _parse_positive("nan") is None
    assert _parse_positive("inf") is None


def test_infer_market() -> None:
    assert _infer_market("AAPL") == "us"
    assert _infer_market("NSE:TCS") == "india"
    assert _infer_market("BSE:TCS") == "india"
    assert _infer_market("NYSE:IBM") == "us"
    assert _infer_market("TCS.NS") == "india"
    assert _infer_market("TCS.BO") == "india"


def test_help_and_bot_commands_mention_portfolio_management() -> None:
    for fragment in ("/add", "/remove", "/setstop"):
        assert fragment in HELP_TEXT
    names = {command.command for command in BOT_COMMANDS}
    assert {"add", "remove", "setstop"} <= names


# --- /add --------------------------------------------------------------------


def test_add_happy_path_defaults_market_and_updates_config() -> None:
    repo = StubRepo()
    app, config = _build(repo=repo)
    update = make_update()
    run(handler(app, "add")(update, make_context(args=["aapl", "190.5"])))
    assert repo.upsert_calls == [("AAPL", "us", 190.5, None, "swing_momentum")]
    reply = texts(update.message)[0]
    assert "Saved AAPL (us)" in reply
    assert "avg 190.5" in reply
    item = next(i for i in config.portfolio if i.symbol == "AAPL")
    assert item.avg_price == 190.5


def test_add_with_market_and_stop() -> None:
    repo = StubRepo()
    app, config = _build(repo=repo)
    update = make_update()
    run(
        handler(app, "add")(
            update, make_context(args=["tcs", "3500", "india", "sl=3300"])
        )
    )
    assert repo.upsert_calls == [("TCS", "india", 3500.0, 3300.0, "swing_momentum")]
    reply = texts(update.message)[0]
    assert "Saved TCS (india)" in reply
    assert "stop 3300.0" in reply
    item = next(i for i in config.portfolio if i.symbol == "TCS")
    assert item.market == "india" and item.stop_loss == 3300.0


def test_add_infers_india_market_from_exchange_prefix() -> None:
    repo = StubRepo()
    app, _ = _build(repo=repo)
    update = make_update()
    run(handler(app, "add")(update, make_context(args=["nse:tcs", "3500"])))
    assert repo.upsert_calls == [("NSE:TCS", "india", 3500.0, None, "swing_momentum")]


def test_add_usage_when_args_missing() -> None:
    repo = StubRepo()
    app, _ = _build(repo=repo)
    update = make_update()
    run(handler(app, "add")(update, make_context(args=["AAPL"])))
    assert texts(update.message) == [ADD_USAGE]
    assert repo.upsert_calls == []


def test_add_rejects_bad_price() -> None:
    repo = StubRepo()
    app, _ = _build(repo=repo)
    update = make_update()
    run(handler(app, "add")(update, make_context(args=["AAPL", "abc"])))
    assert texts(update.message) == ["AVG_PRICE must be a positive number."]
    assert repo.upsert_calls == []


def test_add_rejects_bad_stop() -> None:
    repo = StubRepo()
    app, _ = _build(repo=repo)
    update = make_update()
    run(handler(app, "add")(update, make_context(args=["AAPL", "190", "sl=-5"])))
    assert texts(update.message) == ["STOP must be a positive number."]
    assert repo.upsert_calls == []


def test_add_rejects_unknown_extra_token() -> None:
    repo = StubRepo()
    app, _ = _build(repo=repo)
    update = make_update()
    run(handler(app, "add")(update, make_context(args=["AAPL", "190", "bogus"])))
    assert texts(update.message) == [ADD_USAGE]
    assert repo.upsert_calls == []


def test_add_reports_store_failure() -> None:
    app, _ = _build(repo=StubRepo(raises=True))
    update = make_update()
    run(handler(app, "add")(update, make_context(args=["AAPL", "190"])))
    assert "Portfolio update failed. See logs." in texts(update.message)


def test_add_unauthorized() -> None:
    repo = StubRepo()
    app, _ = _build(repo=repo)
    update = make_update(chat_id=999)
    run(handler(app, "add")(update, make_context(args=["AAPL", "190"])))
    update.message.reply_text.assert_awaited_once_with("Unauthorized chat.")
    assert repo.upsert_calls == []


# --- /remove ------------------------------------------------------------------


def test_remove_happy_path_updates_config() -> None:
    repo = StubRepo()
    app, config = _build(repo=repo)
    update = make_update()
    run(handler(app, "remove")(update, make_context(args=["aapl"])))
    assert repo.remove_calls == ["AAPL"]
    assert texts(update.message) == ["Removed AAPL."]
    assert all(item.symbol != "AAPL" for item in config.portfolio)


def test_remove_not_found() -> None:
    app, config = _build(repo=StubRepo(removed=0))
    update = make_update()
    run(handler(app, "remove")(update, make_context(args=["ZZZ"])))
    assert texts(update.message) == ["ZZZ is not in the portfolio."]
    assert len(config.portfolio) == 2


def test_remove_usage_without_args() -> None:
    repo = StubRepo()
    app, _ = _build(repo=repo)
    update = make_update()
    run(handler(app, "remove")(update, make_context()))
    assert texts(update.message) == [REMOVE_USAGE]
    assert repo.remove_calls == []


def test_remove_reports_store_failure() -> None:
    app, _ = _build(repo=StubRepo(raises=True))
    update = make_update()
    run(handler(app, "remove")(update, make_context(args=["AAPL"])))
    assert "Portfolio update failed. See logs." in texts(update.message)


def test_remove_unauthorized() -> None:
    repo = StubRepo()
    app, _ = _build(repo=repo)
    update = make_update(chat_id=999)
    run(handler(app, "remove")(update, make_context(args=["AAPL"])))
    update.message.reply_text.assert_awaited_once_with("Unauthorized chat.")
    assert repo.remove_calls == []


# --- /setstop -------------------------------------------------------------------


def test_setstop_happy_path_updates_config() -> None:
    repo = StubRepo()
    app, config = _build(repo=repo)
    update = make_update()
    run(handler(app, "setstop")(update, make_context(args=["aapl", "180"])))
    assert repo.set_stop_calls == [("AAPL", 180.0)]
    assert texts(update.message) == ["Stop for AAPL set to 180.0."]
    item = next(i for i in config.portfolio if i.symbol == "AAPL")
    assert item.stop_loss == 180.0


def test_setstop_rejects_bad_number() -> None:
    repo = StubRepo()
    app, _ = _build(repo=repo)
    update = make_update()
    run(handler(app, "setstop")(update, make_context(args=["AAPL", "abc"])))
    assert texts(update.message) == ["STOP must be a positive number."]
    assert repo.set_stop_calls == []


def test_setstop_not_found() -> None:
    app, _ = _build(repo=StubRepo(updated=0))
    update = make_update()
    run(handler(app, "setstop")(update, make_context(args=["ZZZ", "180"])))
    assert texts(update.message) == ["ZZZ is not in the portfolio."]


def test_setstop_usage_without_enough_args() -> None:
    repo = StubRepo()
    app, _ = _build(repo=repo)
    update = make_update()
    run(handler(app, "setstop")(update, make_context(args=["AAPL"])))
    assert texts(update.message) == [SETSTOP_USAGE]
    assert repo.set_stop_calls == []


def test_setstop_reports_store_failure() -> None:
    app, _ = _build(repo=StubRepo(raises=True))
    update = make_update()
    run(handler(app, "setstop")(update, make_context(args=["AAPL", "180"])))
    assert "Portfolio update failed. See logs." in texts(update.message)


def test_setstop_unauthorized() -> None:
    repo = StubRepo()
    app, _ = _build(repo=repo)
    update = make_update(chat_id=999)
    run(handler(app, "setstop")(update, make_context(args=["AAPL", "180"])))
    update.message.reply_text.assert_awaited_once_with("Unauthorized chat.")
    assert repo.set_stop_calls == []


# --- store CRUD (fake client, no network) -----------------------------------


class _FakeRows:
    def __init__(self, rows):
        self.rows = rows


class _FakeStoreClient:
    def __init__(self, *, count=1, row=("AAPL", "us", 190.5, 180.0, "swing_momentum")):
        self.count = count
        self.row = row
        self.statements: list[tuple[str, list | None]] = []
        self.closed = False

    def execute(self, stmt: str, args=None):
        self.statements.append((" ".join(stmt.split()), args))
        if "PRAGMA table_info" in stmt:
            return _FakeRows([(0, "stop_loss")])
        if "SELECT COUNT" in stmt:
            return _FakeRows([(self.count,)])
        if "SELECT symbol" in stmt:
            return _FakeRows([self.row])
        return _FakeRows([])

    def close(self) -> None:
        self.closed = True


def test_upsert_holding_writes_and_returns_saved_row() -> None:
    client = _FakeStoreClient()
    saved = portfolio_store.upsert_holding(client, "AAPL", "us", 190.5, 180.0)
    assert saved == {
        "symbol": "AAPL",
        "market": "us",
        "avg_price": 190.5,
        "stop_loss": 180.0,
        "ruleset": "swing_momentum",
    }
    upsert = next(s for s, _ in client.statements if "ON CONFLICT" in s)
    assert "COALESCE(excluded.stop_loss, stop_loss)" in upsert


def test_delete_holding_returns_removed_count() -> None:
    client = _FakeStoreClient(count=2)
    assert portfolio_store.delete_holding(client, "AAPL") == 2
    assert any(s.startswith("DELETE FROM") for s, _ in client.statements)


def test_delete_holding_skips_delete_when_absent() -> None:
    client = _FakeStoreClient(count=0)
    assert portfolio_store.delete_holding(client, "ZZZ") == 0
    assert not any(s.startswith("DELETE FROM") for s, _ in client.statements)


def test_update_stop_loss_returns_updated_count() -> None:
    client = _FakeStoreClient(count=1)
    assert portfolio_store.update_stop_loss(client, "AAPL", 180.0) == 1
    update = next(s for s, a in client.statements if s.startswith("UPDATE"))
    assert "stop_loss" in update


def test_update_stop_loss_skips_update_when_absent() -> None:
    client = _FakeStoreClient(count=0)
    assert portfolio_store.update_stop_loss(client, "ZZZ", 180.0) == 0
    assert not any(s.startswith("UPDATE") for s, _ in client.statements)


def test_repo_raises_when_turso_unconfigured(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_store, "connect", lambda: None)
    repo = portfolio_store.PortfolioRepo()
    with pytest.raises(RuntimeError, match="Turso is not configured"):
        repo.upsert("AAPL", "us", 190.5)


def test_repo_closes_client_after_each_operation(monkeypatch) -> None:
    clients: list[_FakeStoreClient] = []

    def fake_connect():
        client = _FakeStoreClient()
        clients.append(client)
        return client

    monkeypatch.setattr(portfolio_store, "connect", fake_connect)
    repo = portfolio_store.PortfolioRepo()
    repo.upsert("AAPL", "us", 190.5, 180.0)
    repo.remove("AAPL")
    repo.set_stop("AAPL", 170.0)
    assert len(clients) == 3
    assert all(client.closed for client in clients)
