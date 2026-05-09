from __future__ import annotations

import pytest

from screener_bot.config import BotConfig, load_config


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
