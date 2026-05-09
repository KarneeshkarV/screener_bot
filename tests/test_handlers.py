from __future__ import annotations

from screener_bot.config import BotConfig
from screener_bot.formatting import split_messages


def test_unauthorized_chat_logic() -> None:
    from screener_bot.bot import _authorized

    class Chat:
        id = 2

    class Update:
        effective_chat = Chat()

    config = BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "x"}],
            "rulesets": {"x": {}},
        }
    )
    assert _authorized(config, Update()) is False


def test_long_output_is_split() -> None:
    messages = split_messages("x\n" * 5000, limit=100)
    assert len(messages) > 1
    assert all(len(message) <= 100 for message in messages)
