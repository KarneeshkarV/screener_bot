"""Turso/libSQL storage for paper trading state.

Follows the same patterns as ``portfolio_store`` — module-level functions
that accept a ``_Client``, plus a ``PaperStore`` facade class that opens a
fresh Turso connection per operation.
"""

from __future__ import annotations

import logging
from typing import Any

from ..portfolio_store import _Client, connect

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_PORTFOLIO_TABLE = "paper_portfolio"
_POSITION_TABLE = "paper_position"
_PENDING_TABLE = "paper_pending_order"
_TRADE_TABLE = "paper_trade"


def ensure_paper_tables(client: _Client) -> None:
    """Create all paper trading tables if they don't exist."""
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_PORTFOLIO_TABLE} (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL UNIQUE,
            market          TEXT NOT NULL,
            strategy        TEXT NOT NULL,
            enabled         INTEGER NOT NULL DEFAULT 1,
            initial_capital REAL NOT NULL DEFAULT 1000000,
            current_cash    REAL NOT NULL,
            slots           INTEGER NOT NULL DEFAULT 5,
            stop_loss_pct   REAL,
            take_profit_pct REAL,
            trailing_stop_pct REAL,
            slippage_bps    REAL NOT NULL DEFAULT 10,
            tickers         TEXT,
            created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        )
        """
    )
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_POSITION_TABLE} (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id    INTEGER NOT NULL,
            ticker          TEXT NOT NULL,
            entry_date      TEXT NOT NULL,
            entry_price     REAL NOT NULL,
            shares          REAL NOT NULL,
            slot_capital    REAL NOT NULL,
            peak_price      REAL NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            UNIQUE(portfolio_id, ticker)
        )
        """
    )
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_PENDING_TABLE} (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id    INTEGER NOT NULL,
            ticker          TEXT NOT NULL,
            side            TEXT NOT NULL,
            reason          TEXT NOT NULL,
            signal_price    REAL NOT NULL,
            signal_date     TEXT NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        )
        """
    )
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_TRADE_TABLE} (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id    INTEGER NOT NULL,
            ticker          TEXT NOT NULL,
            entry_date      TEXT NOT NULL,
            entry_price     REAL NOT NULL,
            exit_date       TEXT NOT NULL,
            exit_price      REAL NOT NULL,
            shares          REAL NOT NULL,
            pnl             REAL NOT NULL,
            return_pct      REAL NOT NULL,
            exit_reason     TEXT NOT NULL,
            days_held       INTEGER NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        )
        """
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _opt_float(value: object) -> float | None:
    return float(value) if value is not None else None


def _portfolio_row(row: tuple) -> dict[str, Any]:
    return {
        "id": int(row[0]),
        "name": str(row[1]),
        "market": str(row[2]),
        "strategy": str(row[3]),
        "enabled": bool(row[4]),
        "initial_capital": float(row[5]),
        "current_cash": float(row[6]),
        "slots": int(row[7]),
        "stop_loss_pct": _opt_float(row[8]),
        "take_profit_pct": _opt_float(row[9]),
        "trailing_stop_pct": _opt_float(row[10]),
        "slippage_bps": float(row[11]),
        "tickers": str(row[12]) if row[12] is not None else None,
    }


_PORTFOLIO_COLS = (
    "id, name, market, strategy, enabled, initial_capital, current_cash, "
    "slots, stop_loss_pct, take_profit_pct, trailing_stop_pct, slippage_bps, tickers"
)


def _position_row(row: tuple) -> dict[str, Any]:
    return {
        "id": int(row[0]),
        "portfolio_id": int(row[1]),
        "ticker": str(row[2]),
        "entry_date": str(row[3]),
        "entry_price": float(row[4]),
        "shares": float(row[5]),
        "slot_capital": float(row[6]),
        "peak_price": float(row[7]),
    }


_POSITION_COLS = "id, portfolio_id, ticker, entry_date, entry_price, shares, slot_capital, peak_price"


def _pending_row(row: tuple) -> dict[str, Any]:
    return {
        "id": int(row[0]),
        "portfolio_id": int(row[1]),
        "ticker": str(row[2]),
        "side": str(row[3]),
        "reason": str(row[4]),
        "signal_price": float(row[5]),
        "signal_date": str(row[6]),
    }


_PENDING_COLS = "id, portfolio_id, ticker, side, reason, signal_price, signal_date"


def _trade_row(row: tuple) -> dict[str, Any]:
    return {
        "id": int(row[0]),
        "portfolio_id": int(row[1]),
        "ticker": str(row[2]),
        "entry_date": str(row[3]),
        "entry_price": float(row[4]),
        "exit_date": str(row[5]),
        "exit_price": float(row[6]),
        "shares": float(row[7]),
        "pnl": float(row[8]),
        "return_pct": float(row[9]),
        "exit_reason": str(row[10]),
        "days_held": int(row[11]),
    }


_TRADE_COLS = (
    "id, portfolio_id, ticker, entry_date, entry_price, exit_date, exit_price, "
    "shares, pnl, return_pct, exit_reason, days_held"
)


# ---------------------------------------------------------------------------
# Portfolio CRUD
# ---------------------------------------------------------------------------


def upsert_portfolio(
    client: _Client,
    *,
    name: str,
    market: str,
    strategy: str,
    enabled: bool = True,
    initial_capital: float = 1_000_000,
    current_cash: float | None = None,
    slots: int = 5,
    stop_loss_pct: float | None = None,
    take_profit_pct: float | None = None,
    trailing_stop_pct: float | None = None,
    slippage_bps: float = 10,
    tickers: str | None = None,
) -> dict[str, Any]:
    """Insert or update a named paper portfolio; returns the saved row."""
    ensure_paper_tables(client)
    cash = current_cash if current_cash is not None else initial_capital
    client.execute(
        f"""
        INSERT INTO {_PORTFOLIO_TABLE}
            (name, market, strategy, enabled, initial_capital, current_cash,
             slots, stop_loss_pct, take_profit_pct, trailing_stop_pct,
             slippage_bps, tickers)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            market = excluded.market,
            strategy = excluded.strategy,
            enabled = excluded.enabled,
            initial_capital = excluded.initial_capital,
            slots = excluded.slots,
            stop_loss_pct = excluded.stop_loss_pct,
            take_profit_pct = excluded.take_profit_pct,
            trailing_stop_pct = excluded.trailing_stop_pct,
            slippage_bps = excluded.slippage_bps,
            tickers = excluded.tickers
        """,
        [
            name,
            market,
            strategy,
            int(enabled),
            initial_capital,
            cash,
            slots,
            stop_loss_pct,
            take_profit_pct,
            trailing_stop_pct,
            slippage_bps,
            tickers,
        ],
    )
    rows = client.execute(
        f"SELECT {_PORTFOLIO_COLS} FROM {_PORTFOLIO_TABLE} WHERE name = ?",
        [name],
    ).rows
    return _portfolio_row(rows[0])


def fetch_portfolios(client: _Client) -> list[dict[str, Any]]:
    ensure_paper_tables(client)
    rows = client.execute(
        f"SELECT {_PORTFOLIO_COLS} FROM {_PORTFOLIO_TABLE} ORDER BY id"
    ).rows
    return [_portfolio_row(r) for r in rows]


def fetch_portfolio_by_name(client: _Client, name: str) -> dict[str, Any] | None:
    ensure_paper_tables(client)
    rows = client.execute(
        f"SELECT {_PORTFOLIO_COLS} FROM {_PORTFOLIO_TABLE} WHERE name = ?",
        [name],
    ).rows
    return _portfolio_row(rows[0]) if rows else None


def update_portfolio_enabled(client: _Client, name: str, enabled: bool) -> int:
    ensure_paper_tables(client)
    rows = client.execute(
        f"SELECT COUNT(*) FROM {_PORTFOLIO_TABLE} WHERE name = ?", [name]
    ).rows
    count = int(rows[0][0])
    if count:
        client.execute(
            f"UPDATE {_PORTFOLIO_TABLE} SET enabled = ? WHERE name = ?",
            [int(enabled), name],
        )
    return count


def update_portfolio_cash(client: _Client, portfolio_id: int, cash: float) -> None:
    ensure_paper_tables(client)
    client.execute(
        f"UPDATE {_PORTFOLIO_TABLE} SET current_cash = ? WHERE id = ?",
        [cash, portfolio_id],
    )


def reset_portfolio(client: _Client, portfolio_id: int, initial_capital: float) -> None:
    """Reset a portfolio: restore cash, delete positions/orders/trades."""
    ensure_paper_tables(client)
    client.execute(
        f"UPDATE {_PORTFOLIO_TABLE} SET current_cash = ? WHERE id = ?",
        [initial_capital, portfolio_id],
    )
    client.execute(
        f"DELETE FROM {_POSITION_TABLE} WHERE portfolio_id = ?", [portfolio_id]
    )
    client.execute(
        f"DELETE FROM {_PENDING_TABLE} WHERE portfolio_id = ?", [portfolio_id]
    )
    client.execute(f"DELETE FROM {_TRADE_TABLE} WHERE portfolio_id = ?", [portfolio_id])


# ---------------------------------------------------------------------------
# Position CRUD
# ---------------------------------------------------------------------------


def fetch_positions(client: _Client, portfolio_id: int) -> list[dict[str, Any]]:
    ensure_paper_tables(client)
    rows = client.execute(
        f"SELECT {_POSITION_COLS} FROM {_POSITION_TABLE} WHERE portfolio_id = ? ORDER BY id",
        [portfolio_id],
    ).rows
    return [_position_row(r) for r in rows]


def insert_position(
    client: _Client,
    *,
    portfolio_id: int,
    ticker: str,
    entry_date: str,
    entry_price: float,
    shares: float,
    slot_capital: float,
    peak_price: float,
) -> dict[str, Any]:
    ensure_paper_tables(client)
    client.execute(
        f"""
        INSERT INTO {_POSITION_TABLE}
            (portfolio_id, ticker, entry_date, entry_price, shares, slot_capital, peak_price)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            portfolio_id,
            ticker,
            entry_date,
            entry_price,
            shares,
            slot_capital,
            peak_price,
        ],
    )
    rows = client.execute(
        f"SELECT {_POSITION_COLS} FROM {_POSITION_TABLE} "
        "WHERE portfolio_id = ? AND ticker = ?",
        [portfolio_id, ticker],
    ).rows
    return _position_row(rows[0])


def delete_position(
    client: _Client, portfolio_id: int, ticker: str
) -> dict[str, Any] | None:
    """Delete and return the position (for closing)."""
    ensure_paper_tables(client)
    rows = client.execute(
        f"SELECT {_POSITION_COLS} FROM {_POSITION_TABLE} "
        "WHERE portfolio_id = ? AND ticker = ?",
        [portfolio_id, ticker],
    ).rows
    if not rows:
        return None
    pos = _position_row(rows[0])
    client.execute(
        f"DELETE FROM {_POSITION_TABLE} WHERE portfolio_id = ? AND ticker = ?",
        [portfolio_id, ticker],
    )
    return pos


def update_peak_price(client: _Client, position_id: int, peak_price: float) -> None:
    ensure_paper_tables(client)
    client.execute(
        f"UPDATE {_POSITION_TABLE} SET peak_price = ? WHERE id = ?",
        [peak_price, position_id],
    )


# ---------------------------------------------------------------------------
# Pending Order CRUD
# ---------------------------------------------------------------------------


def insert_pending_order(
    client: _Client,
    *,
    portfolio_id: int,
    ticker: str,
    side: str,
    reason: str,
    signal_price: float,
    signal_date: str,
) -> dict[str, Any]:
    ensure_paper_tables(client)
    client.execute(
        f"""
        INSERT INTO {_PENDING_TABLE}
            (portfolio_id, ticker, side, reason, signal_price, signal_date)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [portfolio_id, ticker, side, reason, signal_price, signal_date],
    )
    rows = client.execute(
        f"SELECT {_PENDING_COLS} FROM {_PENDING_TABLE} "
        "WHERE portfolio_id = ? AND ticker = ? AND side = ? ORDER BY id DESC LIMIT 1",
        [portfolio_id, ticker, side],
    ).rows
    return _pending_row(rows[0])


def fetch_pending_orders(client: _Client, portfolio_id: int) -> list[dict[str, Any]]:
    ensure_paper_tables(client)
    rows = client.execute(
        f"SELECT {_PENDING_COLS} FROM {_PENDING_TABLE} WHERE portfolio_id = ? ORDER BY id",
        [portfolio_id],
    ).rows
    return [_pending_row(r) for r in rows]


def delete_pending_orders(client: _Client, portfolio_id: int) -> int:
    """Delete all pending orders for a portfolio. Return count."""
    ensure_paper_tables(client)
    rows = client.execute(
        f"SELECT COUNT(*) FROM {_PENDING_TABLE} WHERE portfolio_id = ?",
        [portfolio_id],
    ).rows
    count = int(rows[0][0])
    if count:
        client.execute(
            f"DELETE FROM {_PENDING_TABLE} WHERE portfolio_id = ?", [portfolio_id]
        )
    return count


# ---------------------------------------------------------------------------
# Trade History
# ---------------------------------------------------------------------------


def insert_trade(
    client: _Client,
    *,
    portfolio_id: int,
    ticker: str,
    entry_date: str,
    entry_price: float,
    exit_date: str,
    exit_price: float,
    shares: float,
    pnl: float,
    return_pct: float,
    exit_reason: str,
    days_held: int,
) -> dict[str, Any]:
    ensure_paper_tables(client)
    client.execute(
        f"""
        INSERT INTO {_TRADE_TABLE}
            (portfolio_id, ticker, entry_date, entry_price, exit_date, exit_price,
             shares, pnl, return_pct, exit_reason, days_held)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            portfolio_id,
            ticker,
            entry_date,
            entry_price,
            exit_date,
            exit_price,
            shares,
            pnl,
            return_pct,
            exit_reason,
            days_held,
        ],
    )
    rows = client.execute(
        f"SELECT {_TRADE_COLS} FROM {_TRADE_TABLE} "
        "WHERE portfolio_id = ? ORDER BY id DESC LIMIT 1",
        [portfolio_id],
    ).rows
    return _trade_row(rows[0])


def fetch_trades(
    client: _Client, portfolio_id: int, limit: int = 50
) -> list[dict[str, Any]]:
    ensure_paper_tables(client)
    rows = client.execute(
        f"SELECT {_TRADE_COLS} FROM {_TRADE_TABLE} "
        "WHERE portfolio_id = ? ORDER BY id DESC LIMIT ?",
        [portfolio_id, limit],
    ).rows
    return [_trade_row(r) for r in rows]


def fetch_all_trades(client: _Client, portfolio_id: int) -> list[dict[str, Any]]:
    ensure_paper_tables(client)
    rows = client.execute(
        f"SELECT {_TRADE_COLS} FROM {_TRADE_TABLE} WHERE portfolio_id = ? ORDER BY id",
        [portfolio_id],
    ).rows
    return [_trade_row(r) for r in rows]


# ---------------------------------------------------------------------------
# Facade
# ---------------------------------------------------------------------------


class PaperStore:
    """Synchronous CRUD facade that opens a fresh Turso client per operation.

    Handlers run these methods via ``asyncio.to_thread`` to keep blocking
    network I/O off the event loop.
    """

    def _client(self) -> _Client:
        client = connect()
        if client is None:
            raise RuntimeError(
                "Turso is not configured. Set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN."
            )
        return client

    # -- Portfolio --

    def upsert_portfolio(self, **kwargs: Any) -> dict[str, Any]:
        client = self._client()
        try:
            return upsert_portfolio(client, **kwargs)
        finally:
            client.close()

    def fetch_portfolios(self) -> list[dict[str, Any]]:
        client = self._client()
        try:
            return fetch_portfolios(client)
        finally:
            client.close()

    def fetch_portfolio_by_name(self, name: str) -> dict[str, Any] | None:
        client = self._client()
        try:
            return fetch_portfolio_by_name(client, name)
        finally:
            client.close()

    def update_portfolio_enabled(self, name: str, enabled: bool) -> int:
        client = self._client()
        try:
            return update_portfolio_enabled(client, name, enabled)
        finally:
            client.close()

    def update_portfolio_cash(self, portfolio_id: int, cash: float) -> None:
        client = self._client()
        try:
            update_portfolio_cash(client, portfolio_id, cash)
        finally:
            client.close()

    def reset_portfolio(self, portfolio_id: int, initial_capital: float) -> None:
        client = self._client()
        try:
            reset_portfolio(client, portfolio_id, initial_capital)
        finally:
            client.close()

    # -- Positions --

    def fetch_positions(self, portfolio_id: int) -> list[dict[str, Any]]:
        client = self._client()
        try:
            return fetch_positions(client, portfolio_id)
        finally:
            client.close()

    def insert_position(self, **kwargs: Any) -> dict[str, Any]:
        client = self._client()
        try:
            return insert_position(client, **kwargs)
        finally:
            client.close()

    def delete_position(self, portfolio_id: int, ticker: str) -> dict[str, Any] | None:
        client = self._client()
        try:
            return delete_position(client, portfolio_id, ticker)
        finally:
            client.close()

    def update_peak_price(self, position_id: int, peak_price: float) -> None:
        client = self._client()
        try:
            update_peak_price(client, position_id, peak_price)
        finally:
            client.close()

    # -- Pending Orders --

    def insert_pending_order(self, **kwargs: Any) -> dict[str, Any]:
        client = self._client()
        try:
            return insert_pending_order(client, **kwargs)
        finally:
            client.close()

    def fetch_pending_orders(self, portfolio_id: int) -> list[dict[str, Any]]:
        client = self._client()
        try:
            return fetch_pending_orders(client, portfolio_id)
        finally:
            client.close()

    def delete_pending_orders(self, portfolio_id: int) -> int:
        client = self._client()
        try:
            return delete_pending_orders(client, portfolio_id)
        finally:
            client.close()

    # -- Trades --

    def insert_trade(self, **kwargs: Any) -> dict[str, Any]:
        client = self._client()
        try:
            return insert_trade(client, **kwargs)
        finally:
            client.close()

    def fetch_trades(self, portfolio_id: int, limit: int = 50) -> list[dict[str, Any]]:
        client = self._client()
        try:
            return fetch_trades(client, portfolio_id, limit)
        finally:
            client.close()

    def fetch_all_trades(self, portfolio_id: int) -> list[dict[str, Any]]:
        client = self._client()
        try:
            return fetch_all_trades(client, portfolio_id)
        finally:
            client.close()
