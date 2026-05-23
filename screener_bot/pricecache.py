from __future__ import annotations

import time as _time
from dataclasses import dataclass
from datetime import date
from typing import Protocol

import pandas as pd


class PriceFetcher(Protocol):
    def fetch(
        self, tickers: list[str], start: date, end: date
    ) -> dict[str, pd.DataFrame]: ...


@dataclass
class _Entry:
    expires_at: float
    start: date
    end: date
    frame: pd.DataFrame | None


class CachedPriceFetcher:
    """Wrap a price fetcher with a per-ticker TTL cache.

    The portfolio check, on-demand ``/stock`` lookups, and the hourly alert
    engine all request the same tickers over the same trailing date range
    within a short window. Caching avoids hammering the upstream data source
    (and the event loop) with redundant fetches.
    """

    def __init__(self, fetcher: PriceFetcher, ttl_seconds: int = 1800) -> None:
        self._fetcher = fetcher
        self._ttl = ttl_seconds
        self._cache: dict[str, _Entry] = {}

    def fetch(
        self, tickers: list[str], start: date, end: date
    ) -> dict[str, pd.DataFrame]:
        now = _time.monotonic()
        result: dict[str, pd.DataFrame] = {}
        missing: list[str] = []
        for ticker in tickers:
            entry = self._cache.get(ticker)
            if (
                entry is not None
                and entry.expires_at > now
                and entry.start == start
                and entry.end == end
                and entry.frame is not None
            ):
                result[ticker] = entry.frame
            else:
                missing.append(ticker)

        if missing:
            fetched = self._fetcher.fetch(missing, start, end)
            for ticker in missing:
                frame = fetched.get(ticker)
                if frame is not None:
                    result[ticker] = frame
                # Only cache successful, non-empty fetches so transient
                # failures are retried on the next request.
                if frame is not None and not frame.empty:
                    self._cache[ticker] = _Entry(now + self._ttl, start, end, frame)
        return result
