from __future__ import annotations

from datetime import date

import pandas as pd

from screener_bot.pricecache import CachedPriceFetcher


class _CountingFetcher:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def fetch(self, tickers, start, end):
        self.calls.append(list(tickers))
        return {t: pd.DataFrame({"close": [1.0]}) for t in tickers}


def test_caches_within_ttl_and_fetches_only_missing() -> None:
    start, end = date(2025, 1, 1), date(2025, 1, 2)
    inner = _CountingFetcher()
    cache = CachedPriceFetcher(inner, ttl_seconds=300)

    first = cache.fetch(["AAPL", "MSFT"], start, end)
    assert set(first) == {"AAPL", "MSFT"}
    assert inner.calls == [["AAPL", "MSFT"]]

    # AAPL is cached; only NVDA should hit the underlying fetcher.
    cache.fetch(["AAPL", "NVDA"], start, end)
    assert inner.calls[-1] == ["NVDA"]


def test_empty_frames_are_not_cached() -> None:
    start, end = date(2025, 1, 1), date(2025, 1, 2)

    class _EmptyFetcher:
        def __init__(self) -> None:
            self.calls = 0

        def fetch(self, tickers, start, end):
            self.calls += 1
            return {t: pd.DataFrame() for t in tickers}

    inner = _EmptyFetcher()
    cache = CachedPriceFetcher(inner, ttl_seconds=300)
    cache.fetch(["AAPL"], start, end)
    cache.fetch(["AAPL"], start, end)
    assert inner.calls == 2  # transient empties are retried, not cached
