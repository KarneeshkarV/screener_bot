"""Core paper trading engine — two-phase daily cycle orchestration.

Coordinates the Turso store, signal service, and portfolio math to execute
the evening-signals → morning-fills paper trading loop.

All methods are synchronous; callers should wrap in ``asyncio.to_thread``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

from .portfolio import (
    TradeAction,
    check_stop_hit,
    check_trailing_stop_hit,
    compute_fill_price,
    compute_pnl,
    compute_shares,
    compute_slot_capital,
    days_between,
)
from .signals import SignalService
from .store import PaperStore

logger = logging.getLogger(__name__)


@dataclass
class DailyReport:
    """Summary of what happened in one daily cycle for a single portfolio."""

    portfolio_name: str
    market: str
    actions: list[TradeAction] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    open_count: int = 0
    total_slots: int = 0
    current_cash: float = 0.0
    portfolio_value: float = 0.0
    initial_capital: float = 0.0


class PaperTradingEngine:
    """Orchestrates the two-phase paper trading cycle."""

    def __init__(
        self,
        store: PaperStore | None = None,
        signal_service: SignalService | None = None,
    ) -> None:
        self._store = store or PaperStore()
        self._signals = signal_service or SignalService()

    # ------------------------------------------------------------------
    # Phase 1 — Evening signal evaluation
    # ------------------------------------------------------------------

    def run_evening_signals(
        self, portfolio_name: str | None = None
    ) -> list[DailyReport]:
        """Evaluate entry/exit signals for each enabled portfolio.

        Creates pending orders in Turso to be filled the next morning.
        """
        reports: list[DailyReport] = []
        for pf in self._get_target_portfolios(portfolio_name):
            report = self._make_report(pf)
            try:
                self._evening_for_portfolio(pf, report)
            except Exception as exc:
                logger.exception("evening signals failed for %s", pf["name"])
                report.errors.append(f"Evening signal evaluation failed: {exc}")
            reports.append(report)
        return reports

    def _evening_for_portfolio(self, pf: dict, report: DailyReport) -> None:
        portfolio_id = pf["id"]
        positions = self._store.fetch_positions(portfolio_id)
        report.open_count = len(positions)

        # Clear any stale pending orders from a previous incomplete cycle
        self._store.delete_pending_orders(portfolio_id)

        # 1. Check exit signals on open positions
        pending_sell_count = 0
        if positions:
            pending_sell_count = self._create_exit_orders(pf, positions)

        # 2. Check entry signals (only if free slots anticipated)
        free_slots = pf["slots"] - len(positions) + pending_sell_count
        if free_slots > 0:
            self._create_entry_orders(pf, positions, free_slots)

    def _create_exit_orders(self, pf: dict, positions: list[dict]) -> int:
        """Evaluate exit signals and create pending sell orders. Return count."""
        from .portfolio import compute_stop_price, compute_target_price

        portfolio_id = pf["id"]
        held_tickers = [p["ticker"] for p in positions]

        stop_prices: dict[str, float] = {}
        target_prices: dict[str, float] = {}
        trail_peaks: dict[str, float] = {}

        for pos in positions:
            entry = pos["entry_price"]
            if pf.get("stop_loss_pct"):
                sp = compute_stop_price(entry, pf["stop_loss_pct"])
                if sp is not None:
                    stop_prices[pos["ticker"]] = sp
            if pf.get("take_profit_pct"):
                tp = compute_target_price(entry, pf["take_profit_pct"])
                if tp is not None:
                    target_prices[pos["ticker"]] = tp
            trail_peaks[pos["ticker"]] = pos["peak_price"]

        exit_results = self._signals.check_exit_signals(
            strategy_name=pf["strategy"],
            market=pf["market"],
            tickers=held_tickers,
            stop_prices=stop_prices or None,
            target_prices=target_prices or None,
            trail_peaks=trail_peaks or None,
            trailing_stop_pct=pf.get("trailing_stop_pct"),
        )

        sell_count = 0
        for ticker, result in exit_results.items():
            if result.exit_signal and result.close is not None:
                self._store.insert_pending_order(
                    portfolio_id=portfolio_id,
                    ticker=ticker,
                    side="sell",
                    reason=result.exit_reason or "exit_signal",
                    signal_price=result.close,
                    signal_date=date.today().isoformat(),
                )
                sell_count += 1
                logger.info(
                    "paper[%s] pending SELL %s reason=%s close=%.2f",
                    pf["name"], ticker, result.exit_reason, result.close,
                )
        return sell_count

    def _create_entry_orders(
        self, pf: dict, positions: list[dict], free_slots: int
    ) -> None:
        """Evaluate entry signals and create pending buy orders."""
        portfolio_id = pf["id"]

        tickers_list: list[str] | None = None
        if pf.get("tickers"):
            tickers_list = [t.strip() for t in pf["tickers"].split(",") if t.strip()]

        scan = self._signals.scan_entry_signals(
            strategy_name=pf["strategy"],
            market=pf["market"],
            tickers=tickers_list,
        )

        # Filter out already-held tickers
        held_set = {p["ticker"] for p in positions}
        candidates = [t for t in scan.candidates if t not in held_set]

        for ticker in candidates[:free_slots]:
            price_info = scan.prices.get(ticker, {})
            signal_price = price_info.get("close", 0.0)
            if signal_price > 0:
                self._store.insert_pending_order(
                    portfolio_id=portfolio_id,
                    ticker=ticker,
                    side="buy",
                    reason="entry_signal",
                    signal_price=signal_price,
                    signal_date=date.today().isoformat(),
                )
                logger.info(
                    "paper[%s] pending BUY %s close=%.2f",
                    pf["name"], ticker, signal_price,
                )

    # ------------------------------------------------------------------
    # Phase 2 — Morning fill execution
    # ------------------------------------------------------------------

    def run_morning_fills(
        self, portfolio_name: str | None = None
    ) -> list[DailyReport]:
        """Fill pending orders at today's open prices."""
        reports: list[DailyReport] = []
        for pf in self._get_target_portfolios(portfolio_name):
            report = self._make_report(pf)
            try:
                self._morning_for_portfolio(pf, report)
            except Exception as exc:
                logger.exception("morning fills failed for %s", pf["name"])
                report.errors.append(f"Morning fill execution failed: {exc}")
            reports.append(report)
        return reports

    def _morning_for_portfolio(self, pf: dict, report: DailyReport) -> None:
        portfolio_id = pf["id"]
        pending = self._store.fetch_pending_orders(portfolio_id)

        if not pending:
            self._check_gap_stops(pf, report)
            self._update_peaks(pf)
            report.current_cash = pf["current_cash"]
            report.open_count = len(self._store.fetch_positions(portfolio_id))
            return

        # Fetch open prices for all tickers in pending orders
        all_tickers = list({o["ticker"] for o in pending})
        open_prices = self._signals.fetch_open_prices(all_tickers, pf["market"])

        cash = pf["current_cash"]
        positions = self._store.fetch_positions(portfolio_id)

        # Process sells first (frees cash for buys)
        for order in pending:
            if order["side"] != "sell":
                continue
            action = self._fill_sell(pf, order, positions, open_prices, cash)
            if action:
                cash += compute_fill_price(
                    action.price, "sell", pf["slippage_bps"]
                ) * action.shares
                report.actions.append(action)
                # Refresh positions after each sell
                positions = self._store.fetch_positions(portfolio_id)

        # Process buys
        for order in pending:
            if order["side"] != "buy":
                continue
            if len(positions) >= pf["slots"]:
                break

            action = self._fill_buy(pf, order, positions, open_prices, cash)
            if action:
                cost = compute_fill_price(
                    action.price, "buy", pf["slippage_bps"]
                ) * action.shares
                cash -= cost
                report.actions.append(action)
                positions = self._store.fetch_positions(portfolio_id)

        # Persist cash
        self._store.update_portfolio_cash(portfolio_id, cash)
        report.current_cash = cash

        # Clear processed pending orders
        self._store.delete_pending_orders(portfolio_id)

        # Check gap-through stops on remaining positions
        self._check_gap_stops(pf, report)

        # Update peak prices for trailing stops
        self._update_peaks(pf)

        report.open_count = len(self._store.fetch_positions(portfolio_id))

    def _fill_sell(
        self,
        pf: dict,
        order: dict,
        positions: list[dict],
        open_prices: dict[str, float],
        cash: float,
    ) -> TradeAction | None:
        ticker = order["ticker"]
        open_px = open_prices.get(ticker)
        if open_px is None:
            logger.warning("paper[%s] no open price for %s, skipping sell", pf["name"], ticker)
            return None

        pos = next((p for p in positions if p["ticker"] == ticker), None)
        if pos is None:
            return None

        pnl_abs, ret_pct = compute_pnl(
            pos["entry_price"], open_px, pos["shares"], pf["slippage_bps"]
        )
        held = days_between(pos["entry_date"], date.today().isoformat())

        self._store.insert_trade(
            portfolio_id=pf["id"],
            ticker=ticker,
            entry_date=pos["entry_date"],
            entry_price=pos["entry_price"],
            exit_date=date.today().isoformat(),
            exit_price=open_px,
            shares=pos["shares"],
            pnl=pnl_abs,
            return_pct=ret_pct,
            exit_reason=order["reason"],
            days_held=held,
        )
        self._store.delete_position(pf["id"], ticker)

        logger.info(
            "paper[%s] SELL %s @ %.2f pnl=%.2f (%.2f%%) reason=%s",
            pf["name"], ticker, open_px, pnl_abs, ret_pct * 100, order["reason"],
        )
        return TradeAction(
            portfolio_name=pf["name"],
            ticker=ticker,
            side="sell",
            price=open_px,
            shares=pos["shares"],
            reason=order["reason"],
            pnl=pnl_abs,
            return_pct=ret_pct,
            days_held=held,
        )

    def _fill_buy(
        self,
        pf: dict,
        order: dict,
        positions: list[dict],
        open_prices: dict[str, float],
        cash: float,
    ) -> TradeAction | None:
        ticker = order["ticker"]
        open_px = open_prices.get(ticker)
        if open_px is None:
            logger.warning("paper[%s] no open price for %s, skipping buy", pf["name"], ticker)
            return None

        slot_cap = compute_slot_capital(cash, pf["slots"], len(positions))
        if slot_cap <= 0:
            return None

        shares = compute_shares(slot_cap, open_px, pf["slippage_bps"])
        if shares <= 0:
            return None

        fill_px = compute_fill_price(open_px, "buy", pf["slippage_bps"])
        cost = fill_px * shares
        if cost > cash:
            return None

        self._store.insert_position(
            portfolio_id=pf["id"],
            ticker=ticker,
            entry_date=date.today().isoformat(),
            entry_price=open_px,
            shares=shares,
            slot_capital=slot_cap,
            peak_price=open_px,
        )

        logger.info(
            "paper[%s] BUY %s @ %.2f shares=%.2f cost=%.2f",
            pf["name"], ticker, open_px, shares, cost,
        )
        return TradeAction(
            portfolio_name=pf["name"],
            ticker=ticker,
            side="buy",
            price=open_px,
            shares=shares,
            reason="entry_signal",
        )

    # ------------------------------------------------------------------
    # Gap-through stop checks
    # ------------------------------------------------------------------

    def _check_gap_stops(self, pf: dict, report: DailyReport) -> None:
        """Check if open price gaps through stop for any position."""
        portfolio_id = pf["id"]
        positions = self._store.fetch_positions(portfolio_id)
        if not positions:
            return

        tickers = [p["ticker"] for p in positions]
        open_prices = self._signals.fetch_open_prices(tickers, pf["market"])
        cash = report.current_cash or pf["current_cash"]

        for pos in positions:
            ticker = pos["ticker"]
            open_px = open_prices.get(ticker)
            if open_px is None:
                continue

            if check_stop_hit(open_px, pos["entry_price"], pf.get("stop_loss_pct")):
                reason = "stop"
            elif check_trailing_stop_hit(
                open_px, pos["peak_price"], pf.get("trailing_stop_pct")
            ):
                reason = "trail"
            else:
                continue

            fill_px = compute_fill_price(open_px, "sell", pf["slippage_bps"])
            pnl_abs, ret_pct = compute_pnl(
                pos["entry_price"], open_px, pos["shares"], pf["slippage_bps"]
            )
            held = days_between(pos["entry_date"], date.today().isoformat())

            self._store.insert_trade(
                portfolio_id=portfolio_id,
                ticker=ticker,
                entry_date=pos["entry_date"],
                entry_price=pos["entry_price"],
                exit_date=date.today().isoformat(),
                exit_price=open_px,
                shares=pos["shares"],
                pnl=pnl_abs,
                return_pct=ret_pct,
                exit_reason=reason,
                days_held=held,
            )
            self._store.delete_position(portfolio_id, ticker)
            cash += fill_px * pos["shares"]

            logger.info(
                "paper[%s] GAP-STOP %s @ %.2f reason=%s pnl=%.2f",
                pf["name"], ticker, open_px, reason, pnl_abs,
            )
            report.actions.append(
                TradeAction(
                    portfolio_name=pf["name"],
                    ticker=ticker,
                    side="sell",
                    price=open_px,
                    shares=pos["shares"],
                    reason=reason,
                    pnl=pnl_abs,
                    return_pct=ret_pct,
                    days_held=held,
                )
            )

        self._store.update_portfolio_cash(pf["id"], cash)
        report.current_cash = cash

    # ------------------------------------------------------------------
    # Peak price tracking
    # ------------------------------------------------------------------

    def _update_peaks(self, pf: dict) -> None:
        """Update peak prices for trailing stop tracking."""
        positions = self._store.fetch_positions(pf["id"])
        if not positions:
            return

        tickers = [p["ticker"] for p in positions]
        close_prices = self._signals.fetch_close_prices(tickers, pf["market"])

        for pos in positions:
            price = close_prices.get(pos["ticker"])
            if price and price > pos["peak_price"]:
                self._store.update_peak_price(pos["id"], price)

    # ------------------------------------------------------------------
    # Status / metrics (on-demand)
    # ------------------------------------------------------------------

    def get_portfolio_status(self, portfolio_name: str) -> dict | None:
        """Portfolio status including unrealized P&L."""
        pf = self._store.fetch_portfolio_by_name(portfolio_name)
        if pf is None:
            return None

        positions = self._store.fetch_positions(pf["id"])
        current_prices: dict[str, float] = {}
        if positions:
            tickers = [p["ticker"] for p in positions]
            current_prices = self._signals.fetch_close_prices(tickers, pf["market"])

        total_unrealized = 0.0
        position_details: list[dict] = []
        for pos in positions:
            current_px = current_prices.get(pos["ticker"])
            unrealized_pnl = 0.0
            unrealized_pct = 0.0
            if current_px:
                unrealized_pnl = (current_px - pos["entry_price"]) * pos["shares"]
                if pos["entry_price"] > 0:
                    unrealized_pct = (
                        (current_px - pos["entry_price"]) / pos["entry_price"]
                    ) * 100
            total_unrealized += unrealized_pnl
            position_details.append(
                {
                    **pos,
                    "current_price": current_px,
                    "unrealized_pnl": unrealized_pnl,
                    "unrealized_pct": unrealized_pct,
                    "days_held": days_between(
                        pos["entry_date"], date.today().isoformat()
                    ),
                }
            )

        positions_value = sum(
            (p.get("current_price") or 0) * p.get("shares", 0) for p in position_details
        )
        portfolio_value = pf["current_cash"] + positions_value
        total_return_pct = (portfolio_value / pf["initial_capital"] - 1) * 100

        return {
            "portfolio": pf,
            "positions": position_details,
            "total_unrealized_pnl": total_unrealized,
            "portfolio_value": portfolio_value,
            "total_return_pct": total_return_pct,
        }

    def get_all_portfolios_status(self) -> list[dict]:
        """Status for all portfolios."""
        statuses: list[dict] = []
        for pf in self._store.fetch_portfolios():
            status = self.get_portfolio_status(pf["name"])
            if status:
                statuses.append(status)
        return statuses

    def compute_metrics(self, portfolio_name: str) -> dict:
        """On-demand performance metrics from trade history."""
        pf = self._store.fetch_portfolio_by_name(portfolio_name)
        if pf is None:
            return {}
        trades = self._store.fetch_all_trades(pf["id"])
        if not trades:
            return {"trade_count": 0, "hit_rate": 0.0, "total_pnl": 0.0}

        winners = [t for t in trades if t["pnl"] > 0]
        losers = [t for t in trades if t["pnl"] <= 0]
        total_pnl = sum(t["pnl"] for t in trades)
        avg_return = sum(t["return_pct"] for t in trades) / len(trades)
        gross_profit = sum(t["pnl"] for t in winners) if winners else 0.0
        gross_loss = abs(sum(t["pnl"] for t in losers)) if losers else 0.0

        return {
            "trade_count": len(trades),
            "hit_rate": len(winners) / len(trades) * 100,
            "total_pnl": total_pnl,
            "avg_return_pct": avg_return * 100,
            "best_trade": max(t["return_pct"] for t in trades) * 100,
            "worst_trade": min(t["return_pct"] for t in trades) * 100,
            "profit_factor": (
                gross_profit / gross_loss if gross_loss > 0 else float("inf")
            ),
            "avg_days_held": sum(t["days_held"] for t in trades) / len(trades),
            "winning_trades": len(winners),
            "losing_trades": len(losers),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_target_portfolios(self, name: str | None) -> list[dict]:
        if name:
            pf = self._store.fetch_portfolio_by_name(name)
            if pf and pf.get("enabled"):
                return [pf]
            return []
        return [p for p in self._store.fetch_portfolios() if p.get("enabled")]

    def _make_report(self, pf: dict) -> DailyReport:
        return DailyReport(
            portfolio_name=pf["name"],
            market=pf["market"],
            total_slots=pf["slots"],
            current_cash=pf["current_cash"],
            initial_capital=pf["initial_capital"],
        )
