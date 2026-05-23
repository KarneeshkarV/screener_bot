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
    statuses = TechnicalService(
        _config(), StubFetcher({"AAPL": frame})
    ).check_portfolio()
    assert len(statuses) == 1
    assert statuses[0].close == 179
    assert statuses[0].entry.matched is True
    assert statuses[0].snapshot[0].value is not None


def test_handles_empty_price_data() -> None:
    statuses = TechnicalService(_config(), StubFetcher({})).check_portfolio()
    assert statuses[0].error == "No price data available"


class AnyFetcher:
    """Returns the same frame for whichever tickers are requested."""

    def __init__(self, frame: pd.DataFrame, fail: bool = False) -> None:
        self.frame = frame
        self.fail = fail
        self.requested: list[list[str]] = []

    def fetch(self, tickers, start: date, end: date):
        self.requested.append(list(tickers))
        if self.fail:
            raise RuntimeError("network down")
        return {ticker: self.frame for ticker in tickers}


def _frame(periods: int = 260) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=periods)
    close = pd.Series([100.0 + i * 0.1 for i in range(periods)], index=idx, dtype=float)
    return pd.DataFrame(
        {
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": 1000.0,
        },
        index=idx,
    )


# --- check_portfolio extras ------------------------------------------------


def test_check_portfolio_unknown_ruleset() -> None:
    config = BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "missing"}],
            "rulesets": {"x": {}},
        }
    )
    statuses = TechnicalService(
        config, StubFetcher({"AAPL": _frame(20)})
    ).check_portfolio()
    assert "Unknown ruleset" in (statuses[0].entry.error or "")
    assert "Unknown ruleset" in (statuses[0].exit.error or "")


def test_check_portfolio_snapshot_error_is_captured() -> None:
    config = BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "x"}],
            "rulesets": {"x": {}},
            "technical_snapshot": {
                "expressions": [{"label": "Bad", "expression": "undefinedfunc(close)"}]
            },
        }
    )
    statuses = TechnicalService(
        config, StubFetcher({"AAPL": _frame(20)})
    ).check_portfolio()
    assert statuses[0].snapshot[0].error is not None
    assert statuses[0].avg_volume_20 is not None
    assert statuses[0].last_volume == 1000.0


# --- detail ----------------------------------------------------------------


def test_detail_returns_indicators() -> None:
    svc = TechnicalService(_config(), AnyFetcher(_frame(260)))
    status = svc.detail("AAPL", "us")
    assert status.market == "us"
    assert status.ticker == "AAPL"
    assert status.close is not None
    assert status.daily_change_pct is not None
    assert status.rsi14 is not None
    assert status.ema20 is not None
    assert status.last_volume == 1000.0


def test_detail_no_data_available() -> None:
    status = TechnicalService(_config(), StubFetcher({})).detail("AAPL", "us")
    assert status.error == "No price data available"
    assert status.close is None


def test_detail_fetch_failure_records_error() -> None:
    svc = TechnicalService(_config(), AnyFetcher(pd.DataFrame(), fail=True))
    status = svc.detail("AAPL", "us")
    assert status.error is not None
    assert "Price fetch failed" in status.error


# --- bars ------------------------------------------------------------------


def test_bars_returns_sorted_frame() -> None:
    idx = pd.date_range("2025-01-01", periods=10)[::-1]
    frame = pd.DataFrame(
        {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 1.0}, index=idx
    )
    market, ticker, bars = TechnicalService(_config(), AnyFetcher(frame)).bars(
        "AAPL", "us"
    )
    assert market == "us"
    assert ticker == "AAPL"
    assert bars is not None and bars.index.is_monotonic_increasing


def test_bars_no_data_returns_none_tuple() -> None:
    assert TechnicalService(_config(), StubFetcher({})).bars("AAPL", "us") == (
        None,
        None,
        None,
    )


def test_bars_fetch_failure_returns_none_tuple() -> None:
    svc = TechnicalService(_config(), AnyFetcher(pd.DataFrame(), fail=True))
    assert svc.bars("AAPL", "us") == (None, None, None)


# --- candidate market resolution ------------------------------------------


def test_candidate_markets() -> None:
    cm = TechnicalService(_config(), StubFetcher({}))._candidate_markets
    assert cm("NSE:RELIANCE", None) == ["india"]
    assert cm("BSE:500325", None) == ["india"]
    assert cm("NASDAQ:AAPL", None) == ["us"]
    assert cm("RELIANCE.NS", None) == ["india"]
    assert cm("TCS.BO", None) == ["india"]
    assert cm("AAPL", "us") == ["us"]
    assert cm("AAPL", "india") == ["india"]
    assert cm("AAPL", None) == ["us", "india"]


# --- rule-group / expression helpers --------------------------------------


def test_eval_group_empty_returns_none() -> None:
    from screener_bot.technical import _eval_group
    from screener_bot.config import RuleGroup

    assert _eval_group(RuleGroup(), pd.DataFrame()).matched is None


def test_eval_group_any_and_all_modes() -> None:
    from screener_bot.technical import _eval_group
    from screener_bot.config import RuleExpression, RuleGroup

    bars = _frame(60)
    any_group = RuleGroup(
        any=[
            RuleExpression(expression="close > 1000000"),
            RuleExpression(expression="close > 0"),
        ]
    )
    assert _eval_group(any_group, bars).matched is True

    all_group = RuleGroup(
        all=[
            RuleExpression(expression="close > 0"),
            RuleExpression(expression="close > 1000000"),
        ]
    )
    assert _eval_group(all_group, bars).matched is False


def test_eval_group_expression_error_yields_no_match() -> None:
    from screener_bot.technical import _eval_group
    from screener_bot.config import RuleExpression, RuleGroup

    group = RuleGroup(all=[RuleExpression(expression="undefinedfunc(close)")])
    status = _eval_group(group, _frame(20))
    assert status.matched is None
    assert status.error == "no expressions evaluated"
    assert status.results[0].error is not None


def test_safe_eval_and_last_value_edges() -> None:
    from screener_bot.technical import _last_value, _safe_eval

    bars = _frame(10)
    assert isinstance(_safe_eval("close", bars), float)
    assert _safe_eval("undefinedfunc(close)", bars) is None
    assert _last_value(pd.Series([], dtype=float)) is None
    assert _last_value(pd.Series([1.0, 2.0])) == 2.0


def test_last_value_returns_plain_objects() -> None:
    from screener_bot.technical import _last_value

    # Python objects (no numpy .item()) are returned as-is.
    assert _last_value(pd.Series(["a", "b"], dtype=object)) == "b"


def test_safe_eval_non_floatable_value_returns_none(monkeypatch) -> None:
    import screener_bot.technical as tech

    monkeypatch.setattr(tech, "_eval_expression", lambda expr, bars: object())
    assert tech._safe_eval("anything", pd.DataFrame()) is None


def test_detail_skips_indicators_that_error(monkeypatch) -> None:
    import screener_bot.technical as tech

    svc = TechnicalService(_config(), AnyFetcher(_frame(60)))

    def boom(expr, bars):
        raise ValueError("indicator failed")

    monkeypatch.setattr(tech, "_eval_expression", boom)
    status = svc.detail("AAPL", "us")
    assert status.close is not None  # close comes straight from the bars
    assert status.rsi14 is None  # every indicator was skipped
