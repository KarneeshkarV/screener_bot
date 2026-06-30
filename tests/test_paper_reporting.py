"""Tests for paper.reporting — Telegram HTML formatting."""

from __future__ import annotations

from datetime import date

from screener_bot.paper.engine import DailyReport
from screener_bot.paper.portfolio import TradeAction
from screener_bot.paper.reporting import (
    format_daily_report,
    format_metrics,
    format_portfolios_list,
    format_portfolio_status,
    format_trades,
)


class TestFormatDailyReport:
    def test_empty_reports(self) -> None:
        text = format_daily_report([])
        assert "No enabled portfolios" in text

    def test_with_actions(self) -> None:
        report = DailyReport(
            portfolio_name="india-momentum",
            market="india",
            total_slots=5,
            current_cash=900_000,
            initial_capital=1_000_000,
        )
        report.actions.append(
            TradeAction(
                portfolio_name="india-momentum",
                ticker="TCS",
                side="buy",
                price=3500,
                shares=10,
                reason="entry_signal",
            )
        )
        text = format_daily_report([report])
        assert "india-momentum" in text
        assert "BUY" in text
        assert "TCS" in text
        assert "🇮🇳" in text

    def test_sell_with_pnl(self) -> None:
        report = DailyReport(
            portfolio_name="us-test",
            market="us",
            total_slots=10,
            current_cash=90_000,
            initial_capital=100_000,
        )
        report.actions.append(
            TradeAction(
                portfolio_name="us-test",
                ticker="AAPL",
                side="sell",
                price=180,
                shares=50,
                reason="stop",
                pnl=-250.0,
                return_pct=-0.028,
                days_held=5,
            )
        )
        text = format_daily_report([report])
        assert "STOP" in text
        assert "AAPL" in text
        assert "🇺🇸" in text


class TestFormatPortfolioStatus:
    def test_with_positions(self) -> None:
        status = {
            "portfolio": {
                "name": "india-momentum",
                "market": "india",
                "strategy": "rs_breakout",
                "enabled": True,
                "initial_capital": 1_000_000,
                "current_cash": 800_000,
                "slots": 5,
            },
            "positions": [
                {
                    "ticker": "TCS",
                    "entry_price": 3500,
                    "current_price": 3600,
                    "unrealized_pct": 2.86,
                    "days_held": 10,
                },
            ],
            "total_unrealized_pnl": 1000,
            "portfolio_value": 836_000,
            "total_return_pct": -16.4,
        }
        text = format_portfolio_status(status)
        assert "india-momentum" in text
        assert "TCS" in text
        assert "Enabled" in text


class TestFormatPortfoliosList:
    def test_no_portfolios(self) -> None:
        text = format_portfolios_list([])
        assert "None configured" in text

    def test_with_portfolios(self) -> None:
        statuses = [
            {
                "portfolio": {
                    "name": "pf1",
                    "market": "india",
                    "enabled": True,
                    "initial_capital": 100_000,
                    "slots": 5,
                },
                "positions": [],
                "portfolio_value": 102_000,
                "total_return_pct": 2.0,
            },
        ]
        text = format_portfolios_list(statuses)
        assert "pf1" in text
        assert "✅" in text


class TestFormatTrades:
    def test_no_trades(self) -> None:
        text = format_trades([], "test", "india")
        assert "No trades" in text

    def test_with_trades(self) -> None:
        trades = [
            {
                "ticker": "TCS",
                "entry_price": 3500,
                "exit_price": 3700,
                "return_pct": 0.054,
                "pnl": 1900,
                "exit_reason": "target",
                "days_held": 14,
            },
        ]
        text = format_trades(trades, "test", "india")
        assert "TCS" in text
        assert "target" in text


class TestFormatMetrics:
    def test_no_trades(self) -> None:
        text = format_metrics({"trade_count": 0}, "test", "india")
        assert "No trades" in text

    def test_with_metrics(self) -> None:
        metrics = {
            "trade_count": 10,
            "hit_rate": 60.0,
            "total_pnl": 5000,
            "avg_return_pct": 2.5,
            "best_trade": 15.0,
            "worst_trade": -8.0,
            "profit_factor": 1.8,
            "avg_days_held": 12.3,
            "winning_trades": 6,
            "losing_trades": 4,
        }
        text = format_metrics(metrics, "test", "india")
        assert "60.0%" in text
        assert "1.80" in text
        assert "10" in text
