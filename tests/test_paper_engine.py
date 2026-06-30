"""Tests for paper.engine — mocks store + signals, validates orchestration."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from screener_bot.paper.engine import DailyReport, PaperTradingEngine
from screener_bot.paper.portfolio import TradeAction
from screener_bot.paper.signals import ScanResult, SignalResult


def _make_portfolio(**overrides) -> dict:
    base = {
        "id": 1,
        "name": "test-pf",
        "market": "india",
        "strategy": "rs_breakout",
        "enabled": True,
        "initial_capital": 100_000,
        "current_cash": 100_000,
        "slots": 5,
        "stop_loss_pct": 0.08,
        "take_profit_pct": 0.15,
        "trailing_stop_pct": 0.10,
        "slippage_bps": 10,
        "tickers": None,
    }
    base.update(overrides)
    return base


def _make_position(**overrides) -> dict:
    base = {
        "id": 1,
        "portfolio_id": 1,
        "ticker": "TCS",
        "entry_date": "2024-06-01",
        "entry_price": 3500.0,
        "shares": 10.0,
        "slot_capital": 20_000.0,
        "peak_price": 3600.0,
    }
    base.update(overrides)
    return base


class TestEveningSignals:
    def test_creates_buy_orders(self) -> None:
        store = MagicMock()
        signals = MagicMock()

        pf = _make_portfolio()
        store.fetch_portfolios.return_value = [pf]
        store.fetch_portfolio_by_name.return_value = pf
        store.fetch_positions.return_value = []
        store.delete_pending_orders.return_value = 0
        store.fetch_pending_orders.return_value = []
        store.insert_pending_order.return_value = {}

        signals.scan_entry_signals.return_value = ScanResult(
            candidates=["RELIANCE", "TCS", "INFY"],
            prices={
                "RELIANCE": {"close": 2800.0},
                "TCS": {"close": 3500.0},
                "INFY": {"close": 1600.0},
            },
            as_of=date.today(),
        )

        engine = PaperTradingEngine(store=store, signal_service=signals)
        reports = engine.run_evening_signals()

        assert len(reports) == 1
        assert reports[0].portfolio_name == "test-pf"
        # Should create buy orders for all 3 candidates (5 free slots)
        assert store.insert_pending_order.call_count == 3

    def test_creates_sell_orders_for_exits(self) -> None:
        store = MagicMock()
        signals = MagicMock()

        pf = _make_portfolio()
        store.fetch_portfolios.return_value = [pf]
        store.fetch_positions.return_value = [
            _make_position(ticker="TCS"),
        ]
        store.delete_pending_orders.return_value = 0
        store.fetch_pending_orders.return_value = [
            {"side": "sell", "ticker": "TCS"},
        ]
        store.insert_pending_order.return_value = {}

        signals.check_exit_signals.return_value = {
            "TCS": SignalResult(
                ticker="TCS", exit_signal=True, close=3200.0,
                exit_reason="stop",
            ),
        }
        signals.scan_entry_signals.return_value = ScanResult(as_of=date.today())

        engine = PaperTradingEngine(store=store, signal_service=signals)
        engine.run_evening_signals()

        # Should create at least one sell pending order
        calls = store.insert_pending_order.call_args_list
        sell_calls = [c for c in calls if c.kwargs.get("side") == "sell"]
        assert len(sell_calls) >= 1

    def test_skips_disabled_portfolio(self) -> None:
        store = MagicMock()
        signals = MagicMock()

        pf = _make_portfolio(enabled=False)
        store.fetch_portfolios.return_value = [pf]

        engine = PaperTradingEngine(store=store, signal_service=signals)
        reports = engine.run_evening_signals()

        assert len(reports) == 0


class TestMorningFills:
    def test_fills_buy_orders(self) -> None:
        store = MagicMock()
        signals = MagicMock()

        pf = _make_portfolio()
        store.fetch_portfolios.return_value = [pf]
        store.fetch_positions.return_value = []
        store.fetch_pending_orders.return_value = [
            {
                "id": 1, "portfolio_id": 1, "ticker": "RELIANCE",
                "side": "buy", "reason": "entry_signal",
                "signal_price": 2800.0, "signal_date": "2024-06-01",
            },
        ]
        store.insert_position.return_value = _make_position(
            ticker="RELIANCE", entry_price=2810.0
        )
        store.delete_pending_orders.return_value = 1

        signals.fetch_open_prices.return_value = {"RELIANCE": 2810.0}
        signals.fetch_close_prices.return_value = {"RELIANCE": 2810.0}

        engine = PaperTradingEngine(store=store, signal_service=signals)
        reports = engine.run_morning_fills()

        assert len(reports) == 1
        buys = [a for a in reports[0].actions if a.side == "buy"]
        assert len(buys) == 1
        assert buys[0].ticker == "RELIANCE"

    def test_no_pending_orders(self) -> None:
        store = MagicMock()
        signals = MagicMock()

        pf = _make_portfolio()
        store.fetch_portfolios.return_value = [pf]
        store.fetch_positions.return_value = []
        store.fetch_pending_orders.return_value = []
        signals.fetch_open_prices.return_value = {}
        signals.fetch_close_prices.return_value = {}

        engine = PaperTradingEngine(store=store, signal_service=signals)
        reports = engine.run_morning_fills()

        assert len(reports) == 1
        assert reports[0].actions == []


class TestMetrics:
    def test_compute_metrics(self) -> None:
        store = MagicMock()
        signals = MagicMock()

        pf = _make_portfolio()
        store.fetch_portfolio_by_name.return_value = pf
        store.fetch_all_trades.return_value = [
            {"pnl": 500, "return_pct": 0.05, "days_held": 10},
            {"pnl": -200, "return_pct": -0.02, "days_held": 5},
            {"pnl": 300, "return_pct": 0.03, "days_held": 7},
        ]

        engine = PaperTradingEngine(store=store, signal_service=signals)
        metrics = engine.compute_metrics("test-pf")

        assert metrics["trade_count"] == 3
        assert metrics["winning_trades"] == 2
        assert metrics["losing_trades"] == 1
        assert metrics["hit_rate"] == pytest.approx(66.67, abs=0.1)
        assert metrics["total_pnl"] == pytest.approx(600)

    def test_no_trades(self) -> None:
        store = MagicMock()
        signals = MagicMock()

        store.fetch_portfolio_by_name.return_value = _make_portfolio()
        store.fetch_all_trades.return_value = []

        engine = PaperTradingEngine(store=store, signal_service=signals)
        metrics = engine.compute_metrics("test-pf")
        assert metrics["trade_count"] == 0

    def test_portfolio_not_found(self) -> None:
        store = MagicMock()
        signals = MagicMock()

        store.fetch_portfolio_by_name.return_value = None

        engine = PaperTradingEngine(store=store, signal_service=signals)
        assert engine.compute_metrics("nonexistent") == {}


class TestPortfolioStatus:
    def test_with_positions(self) -> None:
        store = MagicMock()
        signals = MagicMock()

        pf = _make_portfolio()
        store.fetch_portfolio_by_name.return_value = pf
        store.fetch_positions.return_value = [
            _make_position(ticker="TCS", entry_price=3500, shares=10),
        ]
        signals.fetch_close_prices.return_value = {"TCS": 3600.0}

        engine = PaperTradingEngine(store=store, signal_service=signals)
        status = engine.get_portfolio_status("test-pf")

        assert status is not None
        assert status["portfolio"]["name"] == "test-pf"
        assert len(status["positions"]) == 1
        assert status["positions"][0]["current_price"] == 3600.0
        assert status["total_unrealized_pnl"] == pytest.approx(1000.0)  # (3600-3500)*10

    def test_not_found(self) -> None:
        store = MagicMock()
        signals = MagicMock()

        store.fetch_portfolio_by_name.return_value = None

        engine = PaperTradingEngine(store=store, signal_service=signals)
        assert engine.get_portfolio_status("nope") is None
