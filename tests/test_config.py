from __future__ import annotations

import pytest

from screener_bot import config as config_module
from screener_bot.config import (
    AlertsConfig,
    BotConfig,
    EnvSettings,
    PortfolioItem,
    ScheduledScreenerConfig,
    ScreenerCommandConfig,
    TelegramConfig,
    load_config,
    load_settings,
)


class _FakeRows:
    def __init__(self, rows):
        self.rows = rows


class _FakeClient:
    def __init__(self, portfolio_rows):
        self._portfolio_rows = portfolio_rows
        self.closed = False

    def execute(self, stmt: str, args=None):
        if "SELECT symbol" in stmt:
            return _FakeRows(self._portfolio_rows)
        if "SELECT COUNT" in stmt:
            return _FakeRows([(len(self._portfolio_rows),)])
        return _FakeRows([])

    def close(self) -> None:
        self.closed = True


def test_load_config_pulls_portfolio_from_turso(monkeypatch) -> None:
    rows = [
        ("AAPL", "us", 267.18, None, "swing_momentum"),
        ("NSE:ATHERENERG", "india", 906.78, 850.0, "swing_momentum"),
    ]
    fake = _FakeClient(rows)
    monkeypatch.setattr(config_module.portfolio_store, "connect", lambda: fake)
    monkeypatch.setenv("TELEGRAM_ALLOWED_CHAT_IDS", "5526359855, 12345")

    config = load_config()
    assert {item.symbol for item in config.portfolio} == {"AAPL", "NSE:ATHERENERG"}
    assert {item.market for item in config.portfolio} == {"india", "us"}
    stops = {item.symbol: item.stop_loss for item in config.portfolio}
    assert stops == {"AAPL": None, "NSE:ATHERENERG": 850.0}
    assert config.telegram.allowed_chat_ids == [5526359855, 12345]
    assert config.scheduled_screener.enabled is True
    assert config.scheduled_screener.times == ["16:00", "02:30"]
    assert len(config.scheduled_screener.commands) == 6
    assert fake.closed is True


def test_load_config_raises_when_turso_unconfigured(monkeypatch) -> None:
    monkeypatch.setattr(config_module.portfolio_store, "connect", lambda: None)
    with pytest.raises(RuntimeError, match="Turso is not configured"):
        load_config()


def test_rejects_missing_symbol() -> None:
    raw = {
        "telegram": {"allowed_chat_ids": [1]},
        "portfolio": [{"market": "india", "ruleset": "x"}],
        "rulesets": {"x": {}},
    }
    with pytest.raises(Exception):
        BotConfig.model_validate(raw)


def test_rejects_missing_market() -> None:
    raw = {
        "telegram": {"allowed_chat_ids": [1]},
        "portfolio": [{"symbol": "NSE:RELIANCE", "ruleset": "x"}],
        "rulesets": {"x": {}},
    }
    with pytest.raises(Exception):
        BotConfig.model_validate(raw)


def test_portfolio_item_strips_and_rejects_empty() -> None:
    item = PortfolioItem(symbol=" AAPL ", market="us", ruleset=" x ")
    assert item.symbol == "AAPL"
    assert item.ruleset == "x"
    with pytest.raises(Exception):
        PortfolioItem(symbol="   ", market="us", ruleset="x")


def test_rejects_empty_portfolio() -> None:
    with pytest.raises(Exception):
        BotConfig.model_validate(
            {"telegram": {"allowed_chat_ids": [1]}, "portfolio": [], "rulesets": {}}
        )


def test_scheduled_times_validation() -> None:
    for bad in (["9am"], ["25:00"], ["10:99"]):
        with pytest.raises(Exception):
            ScheduledScreenerConfig(times=bad)
    assert ScheduledScreenerConfig(times=["09:30", "23:59"]).times == ["09:30", "23:59"]


def test_scheduled_timeout_must_be_positive() -> None:
    with pytest.raises(Exception):
        ScheduledScreenerConfig(timeout_seconds=0)


def test_screener_command_validation() -> None:
    with pytest.raises(Exception):
        ScreenerCommandConfig(label="  ", command=["x"])
    with pytest.raises(Exception):
        ScreenerCommandConfig(label="x", command=[])
    ok = ScreenerCommandConfig(label="India EMA", command=["echo", "hi"])
    assert ok.label == "India EMA"


def test_alerts_validation() -> None:
    with pytest.raises(Exception):
        AlertsConfig(interval_minutes=0)
    with pytest.raises(Exception):
        AlertsConfig(near_high_pct=0)
    with pytest.raises(Exception):
        AlertsConfig(near_high_pct=100)
    with pytest.raises(Exception):
        AlertsConfig(volume_spike_multiple=0)
    ok = AlertsConfig(
        interval_minutes=30, near_high_pct=10.0, volume_spike_multiple=1.5
    )
    assert ok.interval_minutes == 30


def test_env_settings_parses_chat_ids(monkeypatch) -> None:
    monkeypatch.setenv("TELEGRAM_ALLOWED_CHAT_IDS", "1, 2 , 3")
    settings = EnvSettings()
    assert settings.chat_ids() == [1, 2, 3]


def test_admin_chat_id_optional(monkeypatch) -> None:
    assert TelegramConfig(allowed_chat_ids=[1]).admin_chat_id is None
    assert TelegramConfig(allowed_chat_ids=[1], admin_chat_id=7).admin_chat_id == 7
    monkeypatch.setenv("TELEGRAM_ADMIN_CHAT_ID", "99")
    assert EnvSettings().telegram_admin_chat_id == 99


def test_load_settings_returns_env_settings() -> None:
    assert isinstance(load_settings(), EnvSettings)
