from __future__ import annotations

import threading
from io import BytesIO

import matplotlib

matplotlib.use("Agg")  # headless rendering for a server worker

import mplfinance as mpf  # noqa: E402  (must follow backend selection)
import pandas as pd  # noqa: E402


# mplfinance drives pyplot's global state, which is not thread-safe; charts are
# rendered in a thread pool, so serialize the actual plotting.
_PLOT_LOCK = threading.Lock()


_RENAME = {
    "open": "Open",
    "high": "High",
    "low": "Low",
    "close": "Close",
    "volume": "Volume",
}

_EMA_SPANS = ((20, "#1f77b4"), (50, "#ff7f0e"), (200, "#9467bd"))


def render_price_chart(
    bars: pd.DataFrame, symbol: str, lookback: int = 180
) -> bytes | None:
    """Render a candlestick chart with EMA overlays and a volume panel.

    Returns PNG bytes, or None if the bars lack the columns needed to draw.
    """
    df = bars.rename(columns=_RENAME)
    if not all(col in df.columns for col in ("Open", "High", "Low", "Close")):
        return None

    keep = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in df.columns]
    df = df[keep].dropna(subset=["Open", "High", "Low", "Close"])
    if df.empty:
        return None

    df.index = pd.to_datetime(df.index)
    df = df.sort_index().tail(lookback)

    has_volume = bool("Volume" in df.columns and df["Volume"].notna().any())
    close = df["Close"]
    addplots = [
        mpf.make_addplot(
            close.ewm(span=span, adjust=False).mean(), color=color, width=1.0
        )
        for span, color in _EMA_SPANS
        if len(close) >= span
    ]

    plot_kwargs: dict = dict(
        type="candle",
        style="yahoo",
        volume=has_volume,
        title=symbol.split(":")[-1],
        figsize=(9, 6),
        tight_layout=True,
    )
    if addplots:
        plot_kwargs["addplot"] = addplots

    buf = BytesIO()
    with _PLOT_LOCK:
        mpf.plot(
            df, savefig=dict(fname=buf, dpi=120, bbox_inches="tight"), **plot_kwargs
        )
    buf.seek(0)
    return buf.getvalue()
