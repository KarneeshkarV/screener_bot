from __future__ import annotations

import pandas as pd

from screener_bot.charts import render_price_chart


def _ohlcv(periods: int, *, volume: bool = True) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=periods)
    close = pd.Series([100.0 + i for i in range(periods)], index=idx, dtype=float)
    data = {
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
    }
    if volume:
        data["volume"] = 1000.0
    return pd.DataFrame(data, index=idx)


def test_renders_png_with_volume_and_emas() -> None:
    png = render_price_chart(_ohlcv(60), "NSE:NMDC")
    assert isinstance(png, bytes)
    assert png[:4] == b"\x89PNG"  # PNG magic number


def test_renders_all_ema_spans_for_long_history() -> None:
    png = render_price_chart(_ohlcv(260), "AAPL")
    assert isinstance(png, bytes) and png[:4] == b"\x89PNG"


def test_short_history_without_volume_still_renders() -> None:
    png = render_price_chart(_ohlcv(5, volume=False), "X")
    assert isinstance(png, bytes) and png[:4] == b"\x89PNG"


def test_missing_ohlc_columns_returns_none() -> None:
    assert render_price_chart(pd.DataFrame({"close": [1.0, 2.0, 3.0]}), "X") is None


def test_all_nan_rows_drop_to_empty_returns_none() -> None:
    idx = pd.date_range("2025-01-01", periods=3)
    df = pd.DataFrame(
        {"open": [None] * 3, "high": [None] * 3, "low": [None] * 3, "close": [None] * 3},
        index=idx,
    )
    assert render_price_chart(df, "X") is None
