from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import pandas as pd

from screener.backtester.data import build_price_fetcher, tv_to_yf
from screener.backtester.pine import evaluate, parse

from .config import BotConfig, PortfolioItem, RuleGroup
from .pricecache import CachedPriceFetcher


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
class DetailStatus:
    symbol: str
    market: str | None = None
    ticker: str | None = None
    close: float | None = None
    daily_change_pct: float | None = None
    rsi14: float | None = None
    ema20: float | None = None
    ema50: float | None = None
    ema200: float | None = None
    sma50: float | None = None
    sma200: float | None = None
    atr14: float | None = None
    high_52w: float | None = None
    low_52w: float | None = None
    avg_volume_20: float | None = None
    last_volume: float | None = None
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
    high_52w: float | None = None
    low_52w: float | None = None
    last_volume: float | None = None
    avg_volume_20: float | None = None
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


def _safe_eval(expression: str, bars: pd.DataFrame) -> float | None:
    try:
        value = _eval_expression(expression, bars)
    except Exception:  # individual indicator failure is non-fatal
        return None
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
        self.price_fetcher = price_fetcher or CachedPriceFetcher(build_price_fetcher())

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
                        ExpressionResult(
                            expr.label, _eval_expression(expr.expression, bars)
                        )
                    )
                except Exception as exc:
                    status.snapshot.append(ExpressionResult(expr.label, None, str(exc)))

            status.high_52w = _safe_eval("highest(high, 252)", bars)
            status.low_52w = _safe_eval("lowest(low, 252)", bars)
            status.avg_volume_20 = _safe_eval("sma(volume, 20)", bars)
            if "volume" in bars:
                volume = bars["volume"].dropna()
                if not volume.empty:
                    status.last_volume = float(volume.iloc[-1])

            ruleset = self.config.rulesets.get(item.ruleset)
            if ruleset is None:
                status.entry = RuleStatus(
                    None, error=f"Unknown ruleset {item.ruleset!r}"
                )
                status.exit = RuleStatus(
                    None, error=f"Unknown ruleset {item.ruleset!r}"
                )
            else:
                status.entry = _eval_group(ruleset.entry, bars)
                status.exit = _eval_group(ruleset.exit, bars)

            statuses.append(status)
        return statuses

    def _candidate_markets(self, symbol: str, market: str | None) -> list[str]:
        sym = symbol.strip().upper()
        if ":" in sym:
            exch = sym.split(":", 1)[0]
            return ["india"] if exch in {"NSE", "BSE"} else ["us"]
        if sym.endswith((".NS", ".BO")):
            return ["india"]
        if market in {"us", "india"}:
            return [market]
        return ["us", "india"]

    def detail(self, symbol: str, market: str | None = None) -> DetailStatus:
        status = DetailStatus(symbol=symbol)
        end = date.today() + timedelta(days=1)
        start = end - timedelta(days=370)

        bars = None
        for candidate in self._candidate_markets(symbol, market):
            ticker = tv_to_yf(symbol, candidate)
            try:
                frames = self.price_fetcher.fetch([ticker], start, end)
            except Exception as exc:  # network/data failures shouldn't crash the bot
                status.error = f"Price fetch failed: {exc}"
                continue
            candidate_bars = frames.get(ticker)
            if candidate_bars is not None and not candidate_bars.empty:
                status.market = candidate
                status.ticker = ticker
                bars = candidate_bars.sort_index()
                status.error = None
                break

        if bars is None:
            if status.error is None:
                status.error = "No price data available"
            return status

        closes = bars["close"].dropna()
        status.close = float(closes.iloc[-1])
        if len(closes) >= 2:
            prev = float(closes.iloc[-2])
            if prev:
                status.daily_change_pct = ((status.close - prev) / prev) * 100

        if "volume" in bars:
            volume = bars["volume"].dropna()
            if not volume.empty:
                status.last_volume = float(volume.iloc[-1])

        exprs = {
            "rsi14": "rsi(close, 14)",
            "ema20": "ema(close, 20)",
            "ema50": "ema(close, 50)",
            "ema200": "ema(close, 200)",
            "sma50": "sma(close, 50)",
            "sma200": "sma(close, 200)",
            "atr14": "atr(14)",
            "high_52w": "highest(high, 252)",
            "low_52w": "lowest(low, 252)",
            "avg_volume_20": "sma(volume, 20)",
        }
        for attr, expr in exprs.items():
            try:
                value = _eval_expression(expr, bars)
                if value is not None:
                    setattr(status, attr, float(value))
            except Exception:  # individual indicator failure is non-fatal
                continue
        return status

    def bars(
        self, symbol: str, market: str | None = None
    ) -> tuple[str | None, str | None, pd.DataFrame | None]:
        """Best-effort fetch of sorted OHLCV bars for charting.

        Returns ``(market, ticker, bars)`` or ``(None, None, None)`` when no
        data is available. Shares the price cache with ``detail``.
        """
        end = date.today() + timedelta(days=1)
        start = end - timedelta(days=370)
        for candidate in self._candidate_markets(symbol, market):
            ticker = tv_to_yf(symbol, candidate)
            try:
                frames = self.price_fetcher.fetch([ticker], start, end)
            except Exception:  # network/data failures shouldn't crash the bot
                continue
            candidate_bars = frames.get(ticker)
            if candidate_bars is not None and not candidate_bars.empty:
                return candidate, ticker, candidate_bars.sort_index()
        return None, None, None
