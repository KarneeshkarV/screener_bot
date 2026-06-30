"""Pure computation for paper trading position sizing and P&L.

No database calls, no network calls. Fully testable with synthetic data.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date as dt_date

logger = logging.getLogger(__name__)


@dataclass
class TradeAction:
    """A completed trade action for reporting."""

    portfolio_name: str
    ticker: str
    side: str  # "buy" or "sell"
    price: float
    shares: float
    reason: str  # "entry_signal", "stop", "trail", "target", "exit_signal"
    pnl: float | None = None  # Only for sells
    return_pct: float | None = None  # Only for sells
    days_held: int | None = None  # Only for sells


def compute_slot_capital(current_cash: float, slots: int, open_positions: int) -> float:
    """Equal-weight slot capital = cash / free_slots."""
    free_slots = slots - open_positions
    if free_slots <= 0:
        return 0.0
    return current_cash / free_slots


def compute_shares(slot_capital: float, price: float, slippage_bps: float) -> float:
    """Compute shares: slot_capital / (price * (1 + slippage))."""
    if slot_capital <= 0 or price <= 0:
        return 0.0
    slippage_factor = 1 + slippage_bps / 10_000
    return slot_capital / (price * slippage_factor)


def compute_fill_price(price: float, side: str, slippage_bps: float) -> float:
    """Apply slippage to a fill price. Buy = higher, sell = lower."""
    factor = slippage_bps / 10_000
    if side == "buy":
        return price * (1 + factor)
    return price * (1 - factor)


def compute_stop_price(entry_price: float, stop_loss_pct: float | None) -> float | None:
    """Compute stop-loss price from entry and percentage."""
    if stop_loss_pct is None or stop_loss_pct <= 0:
        return None
    return entry_price * (1 - stop_loss_pct)


def compute_target_price(
    entry_price: float, take_profit_pct: float | None
) -> float | None:
    """Compute take-profit price from entry and percentage."""
    if take_profit_pct is None or take_profit_pct <= 0:
        return None
    return entry_price * (1 + take_profit_pct)


def compute_trailing_stop(
    peak_price: float, trailing_stop_pct: float | None
) -> float | None:
    """Compute trailing stop from peak price and percentage."""
    if trailing_stop_pct is None or trailing_stop_pct <= 0:
        return None
    return peak_price * (1 - trailing_stop_pct)


def check_stop_hit(
    price: float, entry_price: float, stop_loss_pct: float | None
) -> bool:
    """True if price is at or below the stop-loss level."""
    stop = compute_stop_price(entry_price, stop_loss_pct)
    return stop is not None and price <= stop


def check_target_hit(
    price: float, entry_price: float, take_profit_pct: float | None
) -> bool:
    """True if price is at or above the take-profit level."""
    target = compute_target_price(entry_price, take_profit_pct)
    return target is not None and price >= target


def check_trailing_stop_hit(
    price: float, peak_price: float, trailing_stop_pct: float | None
) -> bool:
    """True if price is at or below the trailing stop level."""
    trail = compute_trailing_stop(peak_price, trailing_stop_pct)
    return trail is not None and price <= trail


def compute_pnl(
    entry_price: float,
    exit_price: float,
    shares: float,
    slippage_bps: float,
) -> tuple[float, float]:
    """Compute P&L with slippage on both sides.

    Returns ``(pnl_absolute, return_fraction)``.
    """
    buy_fill = compute_fill_price(entry_price, "buy", slippage_bps)
    sell_fill = compute_fill_price(exit_price, "sell", slippage_bps)
    entry_cost = buy_fill * shares
    exit_value = sell_fill * shares
    pnl = exit_value - entry_cost
    return_frac = pnl / entry_cost if entry_cost > 0 else 0.0
    return pnl, return_frac


def days_between(entry_date: str, exit_date: str) -> int:
    """Calendar days between two ISO date strings."""
    entry = dt_date.fromisoformat(entry_date)
    exit_ = dt_date.fromisoformat(exit_date)
    return (exit_ - entry).days
