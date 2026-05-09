from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import pandas as pd

from screener.backtester.data import build_price_fetcher, tv_to_yf
from screener.backtester.pine import evaluate, parse

from .config import BotConfig, PortfolioItem, RuleGroup


@dataclass
class ExpressionResult:
    label: str
    value: Any
    error: str | None = None


@dataclass
class RuleStatus:
    matched: bool | None
    results: list[ExpressionResult] = field(default_factory=list)
    error: str | None = None


@dataclass
class TechnicalStatus:
    item: PortfolioItem
    ticker: str
    close: float | None = None
    daily_change_pct: float | None = None
    snapshot: list[ExpressionResult] = field(default_factory=list)
    entry: RuleStatus = field(default_factory=lambda: RuleStatus(None))
    exit: RuleStatus = field(default_factory=lambda: RuleStatus(None))
    error: str | None = None


def _last_value(series: pd.Series) -> Any:
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    value = cleaned.iloc[-1]
    if hasattr(value, "item"):
        return value.item()
    return value


def _eval_expression(expression: str, bars: pd.DataFrame) -> Any:
    return _last_value(evaluate(parse(expression), bars))


def _eval_group(group: RuleGroup, bars: pd.DataFrame) -> RuleStatus:
    expressions = group.all if group.all is not None else group.any
    mode = "all" if group.all is not None else "any"
    if not expressions:
        return RuleStatus(None)

    results: list[ExpressionResult] = []
    bools: list[bool] = []
    for expr in expressions:
        try:
            value = _eval_expression(expr.expression, bars)
            bool_value = bool(value) if value is not None else False
            bools.append(bool_value)
            results.append(ExpressionResult(expr.expression, bool_value))
        except Exception as exc:  # expression errors should not abort the report
            results.append(ExpressionResult(expr.expression, None, str(exc)))
    if not bools:
        return RuleStatus(None, results, "no expressions evaluated")
    return RuleStatus(all(bools) if mode == "all" else any(bools), results)


class TechnicalService:
    def __init__(self, config: BotConfig, price_fetcher=None) -> None:
        self.config = config
        self.price_fetcher = price_fetcher or build_price_fetcher()

    def check_portfolio(self) -> list[TechnicalStatus]:
        end = date.today() + timedelta(days=1)
        start = end - timedelta(days=370)
        tickers = [tv_to_yf(item.symbol, item.market) for item in self.config.portfolio]
        frames = self.price_fetcher.fetch(tickers, start, end)

        statuses: list[TechnicalStatus] = []
        for item, ticker in zip(self.config.portfolio, tickers, strict=True):
            bars = frames.get(ticker)
            status = TechnicalStatus(item=item, ticker=ticker)
            if bars is None or bars.empty:
                status.error = "No price data available"
                statuses.append(status)
                continue

            bars = bars.sort_index()
            status.close = float(bars["close"].dropna().iloc[-1])
            if len(bars["close"].dropna()) >= 2:
                prev = float(bars["close"].dropna().iloc[-2])
                if prev:
                    status.daily_change_pct = ((status.close - prev) / prev) * 100

            for expr in self.config.technical_snapshot.expressions:
                try:
                    status.snapshot.append(
                        ExpressionResult(expr.label, _eval_expression(expr.expression, bars))
                    )
                except Exception as exc:
                    status.snapshot.append(ExpressionResult(expr.label, None, str(exc)))

            ruleset = self.config.rulesets.get(item.ruleset)
            if ruleset is None:
                status.entry = RuleStatus(None, error=f"Unknown ruleset {item.ruleset!r}")
                status.exit = RuleStatus(None, error=f"Unknown ruleset {item.ruleset!r}")
            else:
                status.entry = _eval_group(ruleset.entry, bars)
                status.exit = _eval_group(ruleset.exit, bars)

            statuses.append(status)
        return statuses
