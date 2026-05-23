from __future__ import annotations

import pytest

from screener_bot.config import (
    AlertsConfig,
    BotConfig,
    EnvSettings,
    PortfolioItem,
    ScheduledScreenerConfig,
    ScreenerCommandConfig,
    load_config,
    load_settings,
)


def test_loads_config() -> None:
    config = load_config("config/bot.yaml")
    assert all(isinstance(chat_id, int) for chat_id in config.telegram.allowed_chat_ids)
    assert config.telegram.allowed_chat_ids
    assert {item.market for item in config.portfolio} == {"india", "us"}
    assert config.scheduled_screener.enabled is True
    assert config.scheduled_screener.times == ["16:00", "02:30"]
    assert len(config.scheduled_screener.commands) == 6


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


def test_load_settings_returns_env_settings() -> None:
    assert isinstance(load_settings(), EnvSettings)
