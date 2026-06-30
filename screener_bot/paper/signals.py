"""Strategy signal evaluation using the screener package.

Bridges the paper trading engine with the screener's strategy and price-data
infrastructure.  All heavy imports are deferred to method bodies so the module
can be imported cheaply.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class SignalResult:
    """Result of evaluating signals for a single ticker."""

    ticker: str
    entry_signal: bool = False
    exit_signal: bool = False
    close: float | None = None
    open_price: float | None = None
    exit_reason: str | None = None  # "stop", "trail", "target", "exit_signal"


@dataclass
class ScanResult:
    """Result of scanning a universe for entry signals."""

    candidates: list[str] = field(default_factory=list)
    prices: dict[str, dict[str, float]] = field(default_factory=dict)
    as_of: date = field(default_factory=date.today)


class SignalService:
    """Evaluates strategy signals using the screener package.

    Designed to be run synchronously inside ``asyncio.to_thread``.
    """

    def __init__(self, price_fetcher: object | None = None) -> None:
        from screener.backtester.data import build_price_fetcher

        self._fetcher = price_fetcher or build_price_fetcher()

    # ------------------------------------------------------------------
    # Entry signals
    # ------------------------------------------------------------------

    def scan_entry_signals(
        self,
        strategy_name: str,
        market: str,
        tickers: list[str] | None = None,
        as_of: date | None = None,
    ) -> ScanResult:
        """Scan a universe for entry signals.

        For ``rs_breakout``: uses ``scan_rs_breakouts()`` which returns a
        ranked candidate list.

        For expression-based strategies: evaluates the entry Pine expression
        on each ticker's bars.
        """
        target_date = as_of or date.today()

        if strategy_name == "rs_breakout":
            return self._scan_rs_breakout(market, tickers, target_date)
        return self._scan_expression(strategy_name, market, tickers, target_date)

    def _scan_rs_breakout(
        self, market: str, tickers: list[str] | None, as_of: date
    ) -> ScanResult:
        from screener.commands.rs_breakout import load_universe as load_tv_universe
        from screener.rs_breakout import (
            DEFAULT_BENCHMARKS,
            fetch_price_data,
            load_india_delivery_for_scan,
            scan_rs_breakouts,
        )

        # Resolve universe
        if tickers:
            universe = tickers
        else:
            try:
                universe = load_tv_universe(market, universe_limit=500)
            except Exception:
                logger.exception("TradingView universe load failed for %s", market)
                return ScanResult(as_of=as_of)

        if not universe:
            return ScanResult(as_of=as_of)

        benchmark = DEFAULT_BENCHMARKS.get(market, "SPY")
        try:
            bars_by_symbol, benchmark_bars = fetch_price_data(
                universe, market, as_of, self._fetcher, benchmark=benchmark
            )
        except Exception:
            logger.exception("price data fetch failed for rs_breakout scan")
            return ScanResult(as_of=as_of)

        # Delivery data for India
        delivery_panel = pd.DataFrame()
        if market == "india":
            try:
                delivery_panel = load_india_delivery_for_scan(universe, as_of)
            except Exception:
                logger.warning("delivery panel unavailable, scanning without it")

        try:
            result = scan_rs_breakouts(
                bars_by_symbol,
                benchmark_bars,
                as_of,
                delivery_panel=delivery_panel,
                benchmark_symbol=benchmark,
                require_delivery=False,  # Don't require delivery for paper trading
            )
        except Exception:
            logger.exception("rs_breakout scan failed")
            return ScanResult(as_of=as_of)

        # Build candidates + price map from full results (ranked by RS score)
        candidates: list[str] = []
        prices: dict[str, dict[str, float]] = {}
        for row in result.full:
            sym = row.symbol
            candidates.append(sym)
            prices[sym] = {"close": row.close, "open": row.close}

        # Also include relaxed bucket with lower priority
        for row in result.relaxed:
            if row.symbol not in prices:
                candidates.append(row.symbol)
                prices[row.symbol] = {"close": row.close, "open": row.close}

        return ScanResult(candidates=candidates, prices=prices, as_of=as_of)

    def _scan_expression(
        self, strategy_name: str, market: str, tickers: list[str] | None, as_of: date
    ) -> ScanResult:
        from screener.backtester.data import tv_to_yf
        from screener.backtester.pine import evaluate, parse
        from screener.strategies.expressions import resolve_strategy

        try:
            strat = resolve_strategy(strategy_name)
        except KeyError:
            logger.error("unknown strategy %r", strategy_name)
            return ScanResult(as_of=as_of)

        if not tickers:
            # Fall back to TradingView universe
            try:
                from screener.commands.rs_breakout import (
                    load_universe as load_tv_universe,
                )

                tickers = load_tv_universe(market, universe_limit=200)
            except Exception:
                logger.exception("universe load failed for expression strategy")
                return ScanResult(as_of=as_of)

        if not tickers:
            return ScanResult(as_of=as_of)

        entry_ast = parse(strat.entry)
        end = as_of + timedelta(days=1)
        start = as_of - timedelta(days=370)

        # Fetch bars for all tickers
        yf_map = {t: tv_to_yf(t, market) for t in tickers}
        try:
            frames = self._fetcher.fetch(list(yf_map.values()), start, end)
        except Exception:
            logger.exception("price fetch failed for expression scan")
            return ScanResult(as_of=as_of)

        candidates: list[str] = []
        prices: dict[str, dict[str, float]] = {}

        for tv_sym, yf_sym in yf_map.items():
            bars = frames.get(yf_sym)
            if bars is None or bars.empty:
                continue
            bars = bars.sort_index()
            try:
                signal_series = evaluate(entry_ast, bars)
                last_signal = signal_series.dropna().iloc[-1] if not signal_series.dropna().empty else False
                if bool(last_signal):
                    close = float(bars["close"].dropna().iloc[-1])
                    candidates.append(tv_sym)
                    prices[tv_sym] = {"close": close, "open": close}
            except Exception:
                logger.debug("expression eval failed for %s", tv_sym, exc_info=True)
                continue

        return ScanResult(candidates=candidates, prices=prices, as_of=as_of)

    # ------------------------------------------------------------------
    # Exit signals
    # ------------------------------------------------------------------

    def check_exit_signals(
        self,
        strategy_name: str,
        market: str,
        tickers: list[str],
        stop_prices: dict[str, float] | None = None,
        target_prices: dict[str, float] | None = None,
        trail_peaks: dict[str, float] | None = None,
        trailing_stop_pct: float | None = None,
    ) -> dict[str, SignalResult]:
        """Check exit conditions for held tickers.

        Priority: stop > trailing stop > take-profit > strategy exit signal.
        """
        from screener.backtester.data import tv_to_yf
        from screener.backtester.pine import evaluate, parse

        end = date.today() + timedelta(days=1)
        start = end - timedelta(days=370)

        yf_map = {t: tv_to_yf(t, market) for t in tickers}
        try:
            frames = self._fetcher.fetch(list(yf_map.values()), start, end)
        except Exception:
            logger.exception("price fetch failed for exit signal check")
            return {}

        # Resolve exit expression (if strategy has one)
        exit_ast = None
        try:
            from screener.strategies.expressions import resolve_strategy

            strat = resolve_strategy(strategy_name)
            if strat.exit:
                exit_ast = parse(strat.exit)
        except (KeyError, Exception):
            pass  # No exit expression — rely on stop/trail/target only

        results: dict[str, SignalResult] = {}
        stop_prices = stop_prices or {}
        target_prices = target_prices or {}
        trail_peaks = trail_peaks or {}

        for tv_sym, yf_sym in yf_map.items():
            result = SignalResult(ticker=tv_sym)
            bars = frames.get(yf_sym)
            if bars is None or bars.empty:
                results[tv_sym] = result
                continue

            bars = bars.sort_index()
            closes = bars["close"].dropna()
            if closes.empty:
                results[tv_sym] = result
                continue

            close = float(closes.iloc[-1])
            result.close = close

            if "open" in bars:
                opens = bars["open"].dropna()
                if not opens.empty:
                    result.open_price = float(opens.iloc[-1])

            # Priority: stop > trail > target > exit_signal
            if tv_sym in stop_prices and close <= stop_prices[tv_sym]:
                result.exit_signal = True
                result.exit_reason = "stop"
            elif (
                tv_sym in trail_peaks
                and trailing_stop_pct
                and trailing_stop_pct > 0
                and close <= trail_peaks[tv_sym] * (1 - trailing_stop_pct)
            ):
                result.exit_signal = True
                result.exit_reason = "trail"
            elif tv_sym in target_prices and close >= target_prices[tv_sym]:
                result.exit_signal = True
                result.exit_reason = "target"
            elif exit_ast is not None:
                try:
                    exit_series = evaluate(exit_ast, bars)
                    last = exit_series.dropna().iloc[-1] if not exit_series.dropna().empty else False
                    if bool(last):
                        result.exit_signal = True
                        result.exit_reason = "exit_signal"
                except Exception:
                    logger.debug("exit expr eval failed for %s", tv_sym, exc_info=True)

            results[tv_sym] = result

        return results

    # ------------------------------------------------------------------
    # Price fetching
    # ------------------------------------------------------------------

    def fetch_open_prices(
        self,
        tickers: list[str],
        market: str,
        target_date: date | None = None,
    ) -> dict[str, float]:
        """Fetch today's open prices for filling pending orders."""
        from screener.backtester.data import tv_to_yf

        target = target_date or date.today()
        # Fetch a small window to get today's bar
        start = target - timedelta(days=7)
        end = target + timedelta(days=1)

        yf_map = {t: tv_to_yf(t, market) for t in tickers}
        try:
            frames = self._fetcher.fetch(list(yf_map.values()), start, end)
        except Exception:
            logger.exception("price fetch failed for open prices")
            return {}

        prices: dict[str, float] = {}
        for tv_sym, yf_sym in yf_map.items():
            bars = frames.get(yf_sym)
            if bars is None or bars.empty:
                continue
            bars = bars.sort_index()
            if "open" in bars:
                opens = bars["open"].dropna()
                if not opens.empty:
                    prices[tv_sym] = float(opens.iloc[-1])
            elif "close" in bars:
                # Fallback to close if open not available
                closes = bars["close"].dropna()
                if not closes.empty:
                    prices[tv_sym] = float(closes.iloc[-1])

        return prices

    def fetch_close_prices(
        self,
        tickers: list[str],
        market: str,
    ) -> dict[str, float]:
        """Fetch latest close prices for mark-to-market valuation."""
        from screener.backtester.data import tv_to_yf

        end = date.today() + timedelta(days=1)
        start = end - timedelta(days=7)

        yf_map = {t: tv_to_yf(t, market) for t in tickers}
        try:
            frames = self._fetcher.fetch(list(yf_map.values()), start, end)
        except Exception:
            logger.exception("price fetch failed for close prices")
            return {}

        prices: dict[str, float] = {}
        for tv_sym, yf_sym in yf_map.items():
            bars = frames.get(yf_sym)
            if bars is None or bars.empty:
                continue
            bars = bars.sort_index()
            closes = bars["close"].dropna()
            if not closes.empty:
                prices[tv_sym] = float(closes.iloc[-1])

        return prices
