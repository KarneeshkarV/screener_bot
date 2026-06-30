from __future__ import annotations

import pandas as pd

import sys
import types

from screener_bot.config import PortfolioItem
from screener_bot.ownership import (
    OwnershipService,
    OwnershipStatus,
    _as_float,
    _india_name,
    fetch_india_shareholding,
)


def test_india_shareholding_deltas(monkeypatch) -> None:
    class FakeStock:
        def __init__(self, name, scraper):
            self.name = name

        def shareholding_quarterly(self):
            return [
                {"date": "Dec 2025", "promoters": "50", "fiis": "10", "diis": "5"},
                {"date": "Mar 2026", "promoters": "51.5", "fiis": "9.5", "diis": "6"},
            ]

    import types
    import sys

    monkeypatch.setitem(
        sys.modules, "openscreener", types.SimpleNamespace(Stock=FakeStock)
    )
    status = fetch_india_shareholding("NSE:RELIANCE")
    assert status.promoter_pct_latest == 51.5
    assert status.promoter_change == 1.5
    assert status.fii_change == -0.5
    assert status.dii_change == 1.0


def test_india_shareholding_retries_missing_section(monkeypatch) -> None:
    class FakeStock:
        calls = 0

        def __init__(self, name, scraper):
            self.name = name

        def shareholding_quarterly(self):
            FakeStock.calls += 1
            if FakeStock.calls == 1:
                raise ValueError("Could not find section with id 'shareholding'.")
            return [
                {"date": "Dec 2025", "promoters": "50", "fiis": "10", "diis": "5"},
                {"date": "Mar 2026", "promoters": "51.5", "fiis": "9.5", "diis": "6"},
            ]

    import sys
    import types

    monkeypatch.setattr("screener_bot.ownership.time.sleep", lambda _: None)
    monkeypatch.setitem(
        sys.modules, "openscreener", types.SimpleNamespace(Stock=FakeStock)
    )
    status = fetch_india_shareholding("NSE:RELIANCE")
    assert FakeStock.calls == 2
    assert status.error is None
    assert status.promoter_pct_latest == 51.5


def test_us_uses_yfinance_insiders(monkeypatch) -> None:
    def fake_fetch(universe, market):
        return pd.DataFrame(
            [{"name": "AAPL", "yf_net_shares_6m": 1200, "yf_net_pct_6m": 0.4}]
        )

    monkeypatch.setattr("screener_bot.ownership.fetch_yfinance_insiders", fake_fetch)
    items = [PortfolioItem(symbol="AAPL", market="us", ruleset="x")]
    statuses = OwnershipService().check_portfolio(items)
    assert statuses["AAPL"].yf_net_shares_6m == 1200
    assert statuses["AAPL"].promoter_pct_latest is None


def test_as_float_handles_none_nan_and_bad_values() -> None:
    assert _as_float(None) is None
    assert _as_float(float("nan")) is None
    assert _as_float("3.5") == 3.5
    assert _as_float("not-a-number") is None
    assert _as_float(5) == 5.0


def test_india_name_strips_suffix() -> None:
    assert "." not in _india_name("NSE:RELIANCE")


def test_india_no_data_after_retries(monkeypatch) -> None:
    class FailStock:
        def __init__(self, name, scraper):
            raise RuntimeError("scrape failed")

    monkeypatch.setattr("screener_bot.ownership.time.sleep", lambda _: None)
    monkeypatch.setitem(
        sys.modules, "openscreener", types.SimpleNamespace(Stock=FailStock)
    )
    status = fetch_india_shareholding("NSE:X")
    assert status.error is not None
    assert status.promoter_pct_latest is None


def test_india_not_enough_rows(monkeypatch) -> None:
    class OneRowStock:
        def __init__(self, name, scraper):
            pass

        def shareholding_quarterly(self):
            return [{"date": "Mar 2026", "promoters": "50"}]

    monkeypatch.setitem(
        sys.modules, "openscreener", types.SimpleNamespace(Stock=OneRowStock)
    )
    status = fetch_india_shareholding("NSE:X")
    assert status.error == "Not enough quarterly shareholding data"


def test_us_without_insider_data(monkeypatch) -> None:
    monkeypatch.setattr(
        "screener_bot.ownership.fetch_yfinance_insiders", lambda u, m: pd.DataFrame()
    )
    items = [PortfolioItem(symbol="AAPL", market="us", ruleset="x")]
    out = OwnershipService().check_portfolio(items)
    assert out["AAPL"].error == "No yfinance insider data available"


def test_us_insider_fetch_raises(monkeypatch) -> None:
    def boom(universe, market):
        raise RuntimeError("network")

    monkeypatch.setattr("screener_bot.ownership.fetch_yfinance_insiders", boom)
    items = [PortfolioItem(symbol="AAPL", market="us", ruleset="x")]
    out = OwnershipService().check_portfolio(items)
    assert out["AAPL"].error == "No yfinance insider data available"


def test_check_portfolio_empty_list() -> None:
    assert OwnershipService().check_portfolio([]) == {}


def test_check_portfolio_parallel_india_fetches_resolve_all() -> None:
    import threading

    barrier = threading.Barrier(4, timeout=5)
    seen_threads: set[str] = set()
    lock = threading.Lock()

    def stub_fetcher(symbol: str) -> OwnershipStatus:
        # All four fetches must be in flight at once for the barrier to
        # release; a sequential implementation would deadlock (timeout).
        barrier.wait()
        with lock:
            seen_threads.add(threading.current_thread().name)
        return OwnershipStatus(
            symbol=symbol, market="india", promoter_pct_latest=float(len(symbol))
        )

    items = [
        PortfolioItem(symbol=f"NSE:S{i}", market="india", ruleset="x") for i in range(4)
    ]
    out = OwnershipService(india_fetcher=stub_fetcher).check_portfolio(items)
    assert list(out) == [item.symbol for item in items]
    for item in items:
        status = out[item.symbol]
        assert status.symbol == item.symbol
        assert status.promoter_pct_latest == float(len(item.symbol))
    assert len(seen_threads) == 4


def test_check_portfolio_worker_cap_and_order(monkeypatch) -> None:
    import threading
    import time

    active = 0
    peak = 0
    lock = threading.Lock()

    def stub_fetcher(symbol: str) -> OwnershipStatus:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.01)
        with lock:
            active -= 1
        return OwnershipStatus(symbol=symbol, market="india")

    items = [
        PortfolioItem(symbol=f"NSE:S{i}", market="india", ruleset="x")
        for i in range(10)
    ]
    out = OwnershipService(india_fetcher=stub_fetcher, max_workers=2).check_portfolio(
        items
    )
    assert list(out) == [item.symbol for item in items]
    assert all(out[item.symbol].error is None for item in items)
    assert peak <= 2


def test_check_portfolio_mixed_markets_aggregation(monkeypatch) -> None:
    def stub_fetcher(symbol: str) -> OwnershipStatus:
        return OwnershipStatus(symbol=symbol, market="india", promoter_pct_latest=42.0)

    def fake_fetch(universe, market):
        return pd.DataFrame(
            [{"name": "AAPL", "yf_net_shares_6m": 100, "yf_net_pct_6m": 0.1}]
        )

    monkeypatch.setattr("screener_bot.ownership.fetch_yfinance_insiders", fake_fetch)
    items = [
        PortfolioItem(symbol="NSE:RELIANCE", market="india", ruleset="x"),
        PortfolioItem(symbol="AAPL", market="us", ruleset="x"),
        PortfolioItem(symbol="NSE:TCS", market="india", ruleset="x"),
    ]
    out = OwnershipService(india_fetcher=stub_fetcher).check_portfolio(items)
    assert set(out) == {"NSE:RELIANCE", "AAPL", "NSE:TCS"}
    assert out["NSE:RELIANCE"].promoter_pct_latest == 42.0
    assert out["NSE:TCS"].promoter_pct_latest == 42.0
    assert out["AAPL"].yf_net_shares_6m == 100


def test_check_portfolio_india_item(monkeypatch) -> None:
    class FakeStock:
        def __init__(self, name, scraper):
            pass

        def shareholding_quarterly(self):
            return [
                {"date": "Dec 2025", "promoters": "50", "fiis": "10", "diis": "5"},
                {"date": "Mar 2026", "promoters": "51", "fiis": "9", "diis": "6"},
            ]

    monkeypatch.setitem(
        sys.modules, "openscreener", types.SimpleNamespace(Stock=FakeStock)
    )
    items = [PortfolioItem(symbol="NSE:RELIANCE", market="india", ruleset="x")]
    out = OwnershipService().check_portfolio(items)
    assert out["NSE:RELIANCE"].promoter_pct_latest == 51
