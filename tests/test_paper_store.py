"""Tests for paper.store — uses a real in-memory SQLite client stub."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest


class _Row:
    """Minimal result-set wrapper matching libsql_client."""
    def __init__(self, rows: list[tuple]) -> None:
        self.rows = rows


class FakeClient:
    """In-memory SQLite client that mimics the libsql _Client protocol."""

    def __init__(self) -> None:
        import sqlite3
        self._conn = sqlite3.connect(":memory:")
        self._conn.execute("PRAGMA journal_mode=WAL")

    def execute(self, sql: str, params: list | None = None) -> _Row:
        cur = self._conn.execute(sql, params or [])
        self._conn.commit()
        return _Row(cur.fetchall())

    def close(self) -> None:
        self._conn.close()


@pytest.fixture()
def client() -> FakeClient:
    return FakeClient()


@pytest.fixture()
def _ensure_tables(client: FakeClient):
    from screener_bot.paper.store import ensure_paper_tables
    ensure_paper_tables(client)


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------


class TestEnsureTables:
    def test_creates_tables(self, client: FakeClient) -> None:
        from screener_bot.paper.store import ensure_paper_tables
        ensure_paper_tables(client)
        rows = client.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).rows
        names = {r[0] for r in rows}
        assert "paper_portfolio" in names
        assert "paper_position" in names
        assert "paper_pending_order" in names
        assert "paper_trade" in names

    def test_idempotent(self, client: FakeClient) -> None:
        from screener_bot.paper.store import ensure_paper_tables
        ensure_paper_tables(client)
        ensure_paper_tables(client)  # should not raise


# ---------------------------------------------------------------------------
# Portfolio CRUD
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_ensure_tables")
class TestPortfolioCrud:
    def test_upsert_and_fetch(self, client: FakeClient) -> None:
        from screener_bot.paper.store import fetch_portfolios, upsert_portfolio
        pf = upsert_portfolio(
            client, name="test", market="india", strategy="rs_breakout",
            initial_capital=100_000, slots=5,
        )
        assert pf["name"] == "test"
        assert pf["market"] == "india"
        assert pf["current_cash"] == 100_000

        all_pfs = fetch_portfolios(client)
        assert len(all_pfs) == 1

    def test_upsert_updates(self, client: FakeClient) -> None:
        from screener_bot.paper.store import fetch_portfolio_by_name, upsert_portfolio
        upsert_portfolio(
            client, name="test", market="india", strategy="rs_breakout",
            initial_capital=100_000, slots=5,
        )
        upsert_portfolio(
            client, name="test", market="us", strategy="ema_trend",
            initial_capital=200_000, slots=10,
        )
        pf = fetch_portfolio_by_name(client, "test")
        assert pf is not None
        assert pf["market"] == "us"
        assert pf["strategy"] == "ema_trend"
        assert pf["slots"] == 10
        # cash should not be overwritten on upsert
        assert pf["current_cash"] == 100_000

    def test_fetch_nonexistent(self, client: FakeClient) -> None:
        from screener_bot.paper.store import fetch_portfolio_by_name
        assert fetch_portfolio_by_name(client, "nope") is None

    def test_update_enabled(self, client: FakeClient) -> None:
        from screener_bot.paper.store import (
            fetch_portfolio_by_name,
            update_portfolio_enabled,
            upsert_portfolio,
        )
        upsert_portfolio(
            client, name="test", market="india", strategy="rs_breakout",
        )
        update_portfolio_enabled(client, "test", False)
        pf = fetch_portfolio_by_name(client, "test")
        assert pf is not None
        assert pf["enabled"] is False

    def test_update_cash(self, client: FakeClient) -> None:
        from screener_bot.paper.store import (
            fetch_portfolio_by_name,
            update_portfolio_cash,
            upsert_portfolio,
        )
        pf = upsert_portfolio(
            client, name="test", market="india", strategy="rs_breakout",
            initial_capital=100_000,
        )
        update_portfolio_cash(client, pf["id"], 95_000)
        updated = fetch_portfolio_by_name(client, "test")
        assert updated is not None
        assert updated["current_cash"] == pytest.approx(95_000)

    def test_reset(self, client: FakeClient) -> None:
        from screener_bot.paper.store import (
            fetch_portfolio_by_name,
            fetch_positions,
            insert_position,
            reset_portfolio,
            upsert_portfolio,
        )
        pf = upsert_portfolio(
            client, name="test", market="india", strategy="rs_breakout",
            initial_capital=100_000,
        )
        insert_position(
            client, portfolio_id=pf["id"], ticker="TCS",
            entry_date="2024-01-01", entry_price=3500, shares=10,
            slot_capital=20_000, peak_price=3500,
        )
        reset_portfolio(client, pf["id"], 100_000)
        updated = fetch_portfolio_by_name(client, "test")
        assert updated is not None
        assert updated["current_cash"] == 100_000
        assert fetch_positions(client, pf["id"]) == []


# ---------------------------------------------------------------------------
# Position CRUD
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_ensure_tables")
class TestPositionCrud:
    def _make_portfolio(self, client: FakeClient) -> dict:
        from screener_bot.paper.store import upsert_portfolio
        return upsert_portfolio(
            client, name="test", market="india", strategy="rs_breakout",
            initial_capital=100_000,
        )

    def test_insert_and_fetch(self, client: FakeClient) -> None:
        from screener_bot.paper.store import fetch_positions, insert_position
        pf = self._make_portfolio(client)
        pos = insert_position(
            client, portfolio_id=pf["id"], ticker="RELIANCE",
            entry_date="2024-06-01", entry_price=2900, shares=5,
            slot_capital=20_000, peak_price=2900,
        )
        assert pos["ticker"] == "RELIANCE"
        assert pos["shares"] == 5
        all_pos = fetch_positions(client, pf["id"])
        assert len(all_pos) == 1

    def test_delete_position(self, client: FakeClient) -> None:
        from screener_bot.paper.store import (
            delete_position,
            fetch_positions,
            insert_position,
        )
        pf = self._make_portfolio(client)
        insert_position(
            client, portfolio_id=pf["id"], ticker="RELIANCE",
            entry_date="2024-06-01", entry_price=2900, shares=5,
            slot_capital=20_000, peak_price=2900,
        )
        deleted = delete_position(client, pf["id"], "RELIANCE")
        assert deleted is not None
        assert deleted["ticker"] == "RELIANCE"
        assert fetch_positions(client, pf["id"]) == []

    def test_delete_nonexistent(self, client: FakeClient) -> None:
        from screener_bot.paper.store import delete_position
        pf = self._make_portfolio(client)
        assert delete_position(client, pf["id"], "NOPE") is None

    def test_update_peak(self, client: FakeClient) -> None:
        from screener_bot.paper.store import (
            fetch_positions,
            insert_position,
            update_peak_price,
        )
        pf = self._make_portfolio(client)
        pos = insert_position(
            client, portfolio_id=pf["id"], ticker="TCS",
            entry_date="2024-01-01", entry_price=3500, shares=10,
            slot_capital=20_000, peak_price=3500,
        )
        update_peak_price(client, pos["id"], 3800.0)
        updated = fetch_positions(client, pf["id"])
        assert updated[0]["peak_price"] == pytest.approx(3800.0)

    def test_unique_constraint(self, client: FakeClient) -> None:
        from screener_bot.paper.store import insert_position
        pf = self._make_portfolio(client)
        insert_position(
            client, portfolio_id=pf["id"], ticker="TCS",
            entry_date="2024-01-01", entry_price=3500, shares=10,
            slot_capital=20_000, peak_price=3500,
        )
        with pytest.raises(Exception):
            insert_position(
                client, portfolio_id=pf["id"], ticker="TCS",
                entry_date="2024-01-02", entry_price=3600, shares=5,
                slot_capital=20_000, peak_price=3600,
            )


# ---------------------------------------------------------------------------
# Pending Orders
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_ensure_tables")
class TestPendingOrders:
    def _make_portfolio(self, client: FakeClient) -> dict:
        from screener_bot.paper.store import upsert_portfolio
        return upsert_portfolio(
            client, name="test", market="india", strategy="rs_breakout",
            initial_capital=100_000,
        )

    def test_insert_and_fetch(self, client: FakeClient) -> None:
        from screener_bot.paper.store import fetch_pending_orders, insert_pending_order
        pf = self._make_portfolio(client)
        order = insert_pending_order(
            client, portfolio_id=pf["id"], ticker="TCS",
            side="buy", reason="entry_signal",
            signal_price=3500, signal_date="2024-06-01",
        )
        assert order["side"] == "buy"
        assert order["ticker"] == "TCS"
        all_orders = fetch_pending_orders(client, pf["id"])
        assert len(all_orders) == 1

    def test_delete_all(self, client: FakeClient) -> None:
        from screener_bot.paper.store import (
            delete_pending_orders,
            fetch_pending_orders,
            insert_pending_order,
        )
        pf = self._make_portfolio(client)
        insert_pending_order(
            client, portfolio_id=pf["id"], ticker="TCS",
            side="buy", reason="entry_signal",
            signal_price=3500, signal_date="2024-06-01",
        )
        insert_pending_order(
            client, portfolio_id=pf["id"], ticker="RELIANCE",
            side="sell", reason="stop",
            signal_price=2800, signal_date="2024-06-01",
        )
        count = delete_pending_orders(client, pf["id"])
        assert count == 2
        assert fetch_pending_orders(client, pf["id"]) == []


# ---------------------------------------------------------------------------
# Trades
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_ensure_tables")
class TestTrades:
    def _make_portfolio(self, client: FakeClient) -> dict:
        from screener_bot.paper.store import upsert_portfolio
        return upsert_portfolio(
            client, name="test", market="india", strategy="rs_breakout",
            initial_capital=100_000,
        )

    def test_insert_and_fetch(self, client: FakeClient) -> None:
        from screener_bot.paper.store import fetch_trades, insert_trade
        pf = self._make_portfolio(client)
        trade = insert_trade(
            client, portfolio_id=pf["id"], ticker="TCS",
            entry_date="2024-01-01", entry_price=3500,
            exit_date="2024-01-15", exit_price=3700,
            shares=10, pnl=1900, return_pct=0.054,
            exit_reason="target", days_held=14,
        )
        assert trade["ticker"] == "TCS"
        assert trade["pnl"] == pytest.approx(1900)
        all_trades = fetch_trades(client, pf["id"])
        assert len(all_trades) == 1

    def test_fetch_limit(self, client: FakeClient) -> None:
        from screener_bot.paper.store import fetch_trades, insert_trade
        pf = self._make_portfolio(client)
        for i in range(5):
            insert_trade(
                client, portfolio_id=pf["id"], ticker=f"STOCK{i}",
                entry_date="2024-01-01", entry_price=100,
                exit_date="2024-01-10", exit_price=110,
                shares=10, pnl=100, return_pct=0.10,
                exit_reason="target", days_held=9,
            )
        assert len(fetch_trades(client, pf["id"], limit=3)) == 3

    def test_fetch_all(self, client: FakeClient) -> None:
        from screener_bot.paper.store import fetch_all_trades, insert_trade
        pf = self._make_portfolio(client)
        for i in range(5):
            insert_trade(
                client, portfolio_id=pf["id"], ticker=f"STOCK{i}",
                entry_date="2024-01-01", entry_price=100,
                exit_date="2024-01-10", exit_price=110,
                shares=10, pnl=100, return_pct=0.10,
                exit_reason="target", days_held=9,
            )
        assert len(fetch_all_trades(client, pf["id"])) == 5


# ---------------------------------------------------------------------------
# PaperStore facade
# ---------------------------------------------------------------------------


class TestPaperStoreFacade:
    def test_fetch_portfolios_delegates(self) -> None:
        from screener_bot.paper.store import PaperStore

        fake = FakeClient()
        from screener_bot.paper.store import ensure_paper_tables
        ensure_paper_tables(fake)

        with patch("screener_bot.paper.store.connect", return_value=fake):
            store = PaperStore()
            result = store.fetch_portfolios()
            assert result == []
