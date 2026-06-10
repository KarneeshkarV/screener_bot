from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import time

import pandas as pd

from screener.backtester.data import tv_to_yf
from screener.insiders import fetch_yfinance_insiders

from .config import PortfolioItem


@dataclass
class OwnershipStatus:
    symbol: str
    market: str
    promoter_pct_latest: float | None = None
    promoter_change: float | None = None
    fii_pct_latest: float | None = None
    fii_change: float | None = None
    dii_pct_latest: float | None = None
    dii_change: float | None = None
    latest_quarter: str | None = None
    yf_net_shares_6m: float | None = None
    yf_net_pct_6m: float | None = None
    error: str | None = None


def _as_float(value) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _india_name(symbol: str) -> str:
    yf_symbol = tv_to_yf(symbol, "india")
    return yf_symbol.removesuffix(".NS").removesuffix(".BO")


def fetch_india_shareholding(symbol: str) -> OwnershipStatus:
    name = _india_name(symbol)
    status = OwnershipStatus(symbol=symbol, market="india")
    rows = None
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            from openscreener import Stock
            from screener.insiders import _HttpScraper

            rows = Stock(name, scraper=_HttpScraper()).shareholding_quarterly()
            break
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))

    if rows is None:
        status.error = (
            str(last_error) if last_error else "No shareholding data available"
        )
        return status

    if not rows or len(rows) < 2:
        status.error = "Not enough quarterly shareholding data"
        return status

    latest, previous = rows[-1], rows[-2]
    status.latest_quarter = latest.get("date")
    fields = (
        ("promoters", "promoter_pct_latest", "promoter_change"),
        ("fiis", "fii_pct_latest", "fii_change"),
        ("diis", "dii_pct_latest", "dii_change"),
    )
    for source, latest_attr, change_attr in fields:
        latest_value = _as_float(latest.get(source))
        previous_value = _as_float(previous.get(source))
        setattr(status, latest_attr, latest_value)
        if latest_value is not None and previous_value is not None:
            setattr(status, change_attr, latest_value - previous_value)
    return status


_MAX_FETCH_WORKERS = 4


class OwnershipService:
    def __init__(
        self,
        india_fetcher: Callable[[str], OwnershipStatus] = fetch_india_shareholding,
        max_workers: int = _MAX_FETCH_WORKERS,
    ) -> None:
        self._india_fetcher = india_fetcher
        self._max_workers = max(1, max_workers)

    def check_portfolio(self, items: list[PortfolioItem]) -> dict[str, OwnershipStatus]:
        out: dict[str, OwnershipStatus] = {}
        india_items = [item for item in items if item.market == "india"]
        if india_items:
            # Each fetch is independent (fresh _HttpScraper per call, no shared
            # session) and keeps its own retry loop, so fan out across a small
            # pool instead of paying the retry sleeps sequentially. pool.map
            # preserves input order, so aggregation order is unchanged.
            workers = min(self._max_workers, len(india_items))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                statuses = list(
                    pool.map(self._india_fetcher, [item.symbol for item in india_items])
                )
            for item, status in zip(india_items, statuses):
                out[item.symbol] = status

        us_items = [item for item in items if item.market == "us"]
        if us_items:
            universe = pd.DataFrame(
                {
                    "name": [item.symbol for item in us_items],
                    "ticker": [item.symbol for item in us_items],
                }
            )
            try:
                insiders = fetch_yfinance_insiders(universe, "us")
            except Exception:
                insiders = pd.DataFrame()
            by_name = (
                {str(row.get("name")): row for _, row in insiders.iterrows()}
                if not insiders.empty
                else {}
            )
            for item in us_items:
                row = by_name.get(item.symbol)
                status = OwnershipStatus(symbol=item.symbol, market="us")
                if row is None:
                    status.error = "No yfinance insider data available"
                else:
                    status.yf_net_shares_6m = _as_float(row.get("yf_net_shares_6m"))
                    status.yf_net_pct_6m = _as_float(row.get("yf_net_pct_6m"))
                out[item.symbol] = status
        return out
