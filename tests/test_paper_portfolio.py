"""Tests for paper.portfolio — pure computation, no mocking needed."""

from __future__ import annotations

import pytest

from screener_bot.paper.portfolio import (
    TradeAction,
    check_stop_hit,
    check_target_hit,
    check_trailing_stop_hit,
    compute_fill_price,
    compute_pnl,
    compute_shares,
    compute_slot_capital,
    compute_stop_price,
    compute_target_price,
    compute_trailing_stop,
    days_between,
)


# ---------------------------------------------------------------------------
# compute_slot_capital
# ---------------------------------------------------------------------------


class TestSlotCapital:
    def test_equal_weight(self) -> None:
        assert compute_slot_capital(100_000, 5, 0) == pytest.approx(20_000)

    def test_partial_fill(self) -> None:
        # 3 of 5 slots used → 2 free → 50k / 2 = 25k
        assert compute_slot_capital(50_000, 5, 3) == pytest.approx(25_000)

    def test_fully_invested(self) -> None:
        assert compute_slot_capital(50_000, 5, 5) == 0.0

    def test_over_invested(self) -> None:
        assert compute_slot_capital(50_000, 5, 6) == 0.0


# ---------------------------------------------------------------------------
# compute_shares
# ---------------------------------------------------------------------------


class TestShares:
    def test_basic(self) -> None:
        # 20k / (100 * 1.001) ≈ 199.80
        result = compute_shares(20_000, 100.0, 10)
        assert result == pytest.approx(20_000 / (100 * 1.001))

    def test_zero_capital(self) -> None:
        assert compute_shares(0, 100, 10) == 0.0

    def test_zero_price(self) -> None:
        assert compute_shares(20_000, 0, 10) == 0.0

    def test_negative_capital(self) -> None:
        assert compute_shares(-1000, 100, 10) == 0.0


# ---------------------------------------------------------------------------
# compute_fill_price
# ---------------------------------------------------------------------------


class TestFillPrice:
    def test_buy_slippage(self) -> None:
        # Buy at 100 with 10bps → 100.10
        assert compute_fill_price(100, "buy", 10) == pytest.approx(100.10)

    def test_sell_slippage(self) -> None:
        # Sell at 100 with 10bps → 99.90
        assert compute_fill_price(100, "sell", 10) == pytest.approx(99.90)

    def test_zero_slippage(self) -> None:
        assert compute_fill_price(100, "buy", 0) == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Stop / Target / Trailing
# ---------------------------------------------------------------------------


class TestStopPrice:
    def test_with_stop(self) -> None:
        assert compute_stop_price(100, 0.08) == pytest.approx(92.0)

    def test_none(self) -> None:
        assert compute_stop_price(100, None) is None

    def test_zero(self) -> None:
        assert compute_stop_price(100, 0) is None

    def test_negative(self) -> None:
        assert compute_stop_price(100, -0.05) is None


class TestTargetPrice:
    def test_with_target(self) -> None:
        assert compute_target_price(100, 0.15) == pytest.approx(115.0)

    def test_none(self) -> None:
        assert compute_target_price(100, None) is None


class TestTrailingStop:
    def test_basic(self) -> None:
        assert compute_trailing_stop(120, 0.10) == pytest.approx(108.0)

    def test_none(self) -> None:
        assert compute_trailing_stop(120, None) is None


class TestCheckHits:
    def test_stop_hit(self) -> None:
        assert check_stop_hit(91.0, 100.0, 0.08) is True

    def test_stop_exact(self) -> None:
        assert check_stop_hit(92.0, 100.0, 0.08) is True

    def test_stop_not_hit(self) -> None:
        assert check_stop_hit(93.0, 100.0, 0.08) is False

    def test_stop_none(self) -> None:
        assert check_stop_hit(50.0, 100.0, None) is False

    def test_target_hit(self) -> None:
        assert check_target_hit(116.0, 100.0, 0.15) is True

    def test_target_not_hit(self) -> None:
        assert check_target_hit(114.0, 100.0, 0.15) is False

    def test_trail_hit(self) -> None:
        assert check_trailing_stop_hit(107.0, 120.0, 0.10) is True

    def test_trail_not_hit(self) -> None:
        assert check_trailing_stop_hit(109.0, 120.0, 0.10) is False


# ---------------------------------------------------------------------------
# compute_pnl
# ---------------------------------------------------------------------------


class TestPnl:
    def test_profit(self) -> None:
        # Buy 100 @ 100, sell @ 110, 10bps slip each way
        pnl, ret = compute_pnl(100.0, 110.0, 100, 10)
        buy_cost = 100.10 * 100  # = 10010
        sell_val = 109.89 * 100  # = 10989 (110 * 0.999 = 109.89)
        expected_pnl = sell_val - buy_cost
        assert pnl == pytest.approx(expected_pnl, abs=0.1)
        assert ret == pytest.approx(expected_pnl / buy_cost, abs=1e-5)

    def test_loss(self) -> None:
        pnl, ret = compute_pnl(100.0, 90.0, 100, 10)
        assert pnl < 0
        assert ret < 0

    def test_zero_shares(self) -> None:
        pnl, ret = compute_pnl(100, 110, 0, 10)
        assert pnl == 0.0
        assert ret == 0.0


# ---------------------------------------------------------------------------
# days_between
# ---------------------------------------------------------------------------


class TestDaysBetween:
    def test_same_day(self) -> None:
        assert days_between("2024-01-15", "2024-01-15") == 0

    def test_one_week(self) -> None:
        assert days_between("2024-01-01", "2024-01-08") == 7

    def test_cross_month(self) -> None:
        assert days_between("2024-01-28", "2024-02-04") == 7


# ---------------------------------------------------------------------------
# TradeAction
# ---------------------------------------------------------------------------


class TestTradeAction:
    def test_buy_action(self) -> None:
        a = TradeAction(
            portfolio_name="test",
            ticker="AAPL",
            side="buy",
            price=150.0,
            shares=10,
            reason="entry_signal",
        )
        assert a.pnl is None
        assert a.return_pct is None

    def test_sell_action(self) -> None:
        a = TradeAction(
            portfolio_name="test",
            ticker="AAPL",
            side="sell",
            price=160.0,
            shares=10,
            reason="stop",
            pnl=95.0,
            return_pct=0.063,
            days_held=12,
        )
        assert a.pnl == 95.0
        assert a.days_held == 12
