from __future__ import annotations

from datetime import date

import pandas as pd

from screener_bot.config import BotConfig
from screener_bot.technical import TechnicalService


class StubFetcher:
    def __init__(self, frames):
        self.frames = frames

    def fetch(self, tickers, start: date, end: date):
        return {ticker: self.frames.get(ticker, pd.DataFrame()) for ticker in tickers}


def _config() -> BotConfig:
    return BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [
                {"symbol": "AAPL", "market": "us", "ruleset": "swing"},
            ],
            "rulesets": {
                "swing": {
                    "entry": {"all": [{"expression": "rsi(close, 14) > 55"}]},
                    "exit": {"any": [{"expression": "close < ema(close, 20)"}]},
                }
            },
            "technical_snapshot": {
                "expressions": [
                    {"label": "RSI 14", "expression": "rsi(close, 14)"},
                    {"label": "Above EMA20", "expression": "close > ema(close, 20)"},
                ]
            },
        }
    )


def test_computes_technical_snapshot() -> None:
    idx = pd.date_range("2025-01-01", periods=80)
    close = pd.Series(range(100, 180), index=idx, dtype=float)
    frame = pd.DataFrame(
        {
            "open": close - 1,
            "high": close + 1,
            "low": close - 2,
            "close": close,
            "volume": 1000,
        },
        index=idx,
    )
    statuses = TechnicalService(_config(), StubFetcher({"AAPL": frame})).check_portfolio()
    assert len(statuses) == 1
    assert statuses[0].close == 179
    assert statuses[0].entry.matched is True
    assert statuses[0].snapshot[0].value is not None


def test_handles_empty_price_data() -> None:
    statuses = TechnicalService(_config(), StubFetcher({})).check_portfolio()
    assert statuses[0].error == "No price data available"
