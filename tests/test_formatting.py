from __future__ import annotations

from screener_bot.config import PortfolioItem
from screener_bot.formatting import (
    SAFE_LIMIT,
    TELEGRAM_LIMIT,
    _currency,
    _fmt_delta,
    _fmt_number,
    _fmt_pnl,
    _fmt_rule,
    _rsi_zone,
    _signal,
    _split_long_line,
    _split_pre_block,
    _vs,
    format_detail_report,
    format_portfolio_report,
    split_messages,
)
from screener_bot.ownership import OwnershipStatus
from screener_bot.technical import (
    DetailStatus,
    ExpressionResult,
    RuleStatus,
    TechnicalStatus,
)


# --- small formatting helpers ---------------------------------------------


def test_fmt_number_variants() -> None:
    assert _fmt_number(None) == "n/a"
    assert _fmt_number(True) == "yes"
    assert _fmt_number(False) == "no"
    assert _fmt_number(3.14159) == "3.14"
    assert _fmt_number(10, 0) == "10"
    assert _fmt_number("text") == "text"


def test_fmt_delta_variants() -> None:
    assert _fmt_delta(None) == "n/a"
    assert _fmt_delta(1.5) == "+1.50 pp"
    assert _fmt_delta(-2.0) == "-2.00 pp"


def test_fmt_rule_variants() -> None:
    assert _fmt_rule(None) == "n/a"
    assert _fmt_rule(True) == "yes"
    assert _fmt_rule(False) == "no"


def test_currency() -> None:
    assert _currency("india") == "₹"
    assert _currency("us") == "$"


def test_fmt_pnl_gain_and_loss() -> None:
    assert _fmt_pnl(110.0, 100.0, "us") == "+$10.00 (+10.00%)"
    assert _fmt_pnl(90.0, 100.0, "india") == "-₹10.00 (-10.00%)"


def test_signal_variants() -> None:
    assert _signal(True) == "✅ Yes"
    assert _signal(False) == "❌ No"
    assert _signal(None) == "—"


def test_rsi_zone_all_bands() -> None:
    assert "overbought" in _rsi_zone(75)
    assert "bullish" in _rsi_zone(60)
    assert "oversold" in _rsi_zone(20)
    assert "bearish" in _rsi_zone(40)
    assert "neutral" in _rsi_zone(50)


def test_vs_variants() -> None:
    assert _vs(100.0, None) == "n/a"
    assert "▲" in _vs(110.0, 100.0)
    assert "▼" in _vs(90.0, 100.0)


# --- portfolio report ------------------------------------------------------


def _tech(
    *,
    symbol: str = "AAPL",
    market: str = "us",
    avg_price: float | None = None,
    error: str | None = None,
    close: float = 100.0,
) -> TechnicalStatus:
    item = PortfolioItem(symbol=symbol, market=market, avg_price=avg_price, ruleset="x")
    status = TechnicalStatus(item=item, ticker=symbol)
    if error:
        status.error = error
    else:
        status.close = close
        status.daily_change_pct = 1.23
    status.entry = RuleStatus(True)
    status.exit = RuleStatus(False)
    return status


def test_portfolio_report_us_with_pnl_snapshot_and_insiders() -> None:
    status = _tech(symbol="AAPL", market="us", avg_price=90.0, close=100.0)
    status.snapshot = [
        ExpressionResult("RSI", 55.0),
        ExpressionResult("Broken", None, "boom"),
    ]
    owner = OwnershipStatus(
        symbol="AAPL", market="us", yf_net_shares_6m=1000.0, yf_net_pct_6m=0.5
    )

    report = format_portfolio_report([status], {"AAPL": owner})

    assert "📊 Portfolio Check" in report
    assert "AAPL" in report
    assert "Avg Cost" in report
    assert "P&amp;L" in report  # ampersand HTML-escaped
    assert "RSI" in report
    assert "Insiders 6m" in report


def test_portfolio_report_us_owner_error_note() -> None:
    owner = OwnershipStatus(symbol="AAPL", market="us", error="no data")
    report = format_portfolio_report([_tech()], {"AAPL": owner})
    assert "Note: no data" in report


def test_portfolio_report_india_with_quarter_and_error() -> None:
    status = _tech(symbol="NSE:RELIANCE", market="india", close=2500.0)
    owner = OwnershipStatus(
        symbol="NSE:RELIANCE",
        market="india",
        promoter_pct_latest=50.0,
        promoter_change=1.0,
        fii_pct_latest=10.0,
        fii_change=-0.5,
        dii_pct_latest=5.0,
        dii_change=0.2,
        latest_quarter="Mar 2026",
        error="partial",
    )
    report = format_portfolio_report([status], {"NSE:RELIANCE": owner})
    assert "RELIANCE" in report
    assert "Shareholding" in report
    assert "Mar 2026" in report
    assert "Promoter" in report
    assert "Note: partial" in report


def test_portfolio_report_india_without_quarter_or_error() -> None:
    status = _tech(symbol="NSE:TCS", market="india", close=4000.0)
    owner = OwnershipStatus(symbol="NSE:TCS", market="india", promoter_pct_latest=70.0)
    report = format_portfolio_report([status], {"NSE:TCS": owner})
    assert "Shareholding" in report
    assert "Note:" not in report


def test_portfolio_report_with_error_status_and_no_owner() -> None:
    status = _tech(symbol="AAPL", market="us", error="No price data available")
    report = format_portfolio_report([status], {})
    assert "No price data available" in report


# --- detail report ---------------------------------------------------------


def test_detail_report_error_without_close() -> None:
    report = format_detail_report(
        DetailStatus(symbol="AAPL", error="No price data available")
    )
    assert "No price data available" in report


def test_detail_report_full_with_golden_cross() -> None:
    status = DetailStatus(
        symbol="NSE:NMDC",
        market="india",
        close=100.0,
        daily_change_pct=2.0,
        rsi14=72.0,
        ema20=95.0,
        ema50=90.0,
        ema200=80.0,
        sma50=85.0,
        sma200=70.0,
        atr14=3.0,
        high_52w=120.0,
        low_52w=60.0,
        avg_volume_20=1000.0,
        last_volume=2000.0,
    )
    report = format_detail_report(status)
    assert "NMDC" in report
    assert "RSI 14" in report
    assert "overbought" in report
    assert "golden cross" in report
    assert "ATR 14" in report
    assert "52w range" in report
    assert "Volume" in report


def test_detail_report_death_cross_and_oversold() -> None:
    status = DetailStatus(
        symbol="AAPL", market="us", close=50.0, sma50=40.0, sma200=60.0, rsi14=25.0
    )
    report = format_detail_report(status)
    assert "death cross" in report
    assert "oversold" in report


def test_detail_report_unknown_market_minimal() -> None:
    report = format_detail_report(DetailStatus(symbol="X", close=10.0))
    assert report.startswith("<b>📈 X</b>")


# --- message splitting -----------------------------------------------------


def test_split_messages_returns_single_when_short() -> None:
    assert split_messages("hello", limit=100) == ["hello"]


def test_split_messages_breaks_many_lines() -> None:
    messages = split_messages("x\n" * 5000, limit=100)
    assert len(messages) > 1
    assert all(len(message) <= 100 for message in messages)


def test_split_messages_splits_single_overlong_line() -> None:
    messages = split_messages("a" * 250, limit=100)
    assert all(len(message) <= 100 for message in messages)
    assert "".join(messages) == "a" * 250


def test_split_messages_keeps_pre_tags_balanced() -> None:
    text = "<b>R</b>\n<pre>" + "\n".join(f"S{i} 1 2" for i in range(50)) + "</pre>"
    messages = split_messages(text, limit=100)
    assert len(messages) > 1
    assert all(m.count("<pre>") == m.count("</pre>") for m in messages)


def test_split_messages_handles_unclosed_pre_at_end() -> None:
    text = "<pre>" + "\n".join("a" * 30 for _ in range(20))
    messages = split_messages(text, limit=80)
    assert len(messages) >= 1


def test_split_messages_default_limit_stays_below_telegram_cap() -> None:
    text = "\n".join("<b>row</b> " + "y" * 90 for _ in range(200))
    messages = split_messages(text)
    assert len(messages) > 1
    assert all(len(message) <= SAFE_LIMIT for message in messages)
    assert all(len(message) < TELEGRAM_LIMIT for message in messages)


def test_split_messages_clamps_oversized_limit() -> None:
    # A caller-supplied limit above the safe cap must not produce messages
    # that Telegram would reject once HTML overhead is counted.
    messages = split_messages("x" * 20000, limit=TELEGRAM_LIMIT * 2)
    assert all(len(message) <= SAFE_LIMIT for message in messages)
    assert "".join(messages) == "x" * 20000


def test_split_long_line() -> None:
    assert _split_long_line("abcdef", 2) == ["ab", "cd", "ef"]


def test_split_pre_block_small_returns_as_is() -> None:
    assert _split_pre_block("<pre>hi</pre>", 100) == ["<pre>hi</pre>"]


def test_split_pre_block_unwrapped_falls_back_to_line_split() -> None:
    block = "x" * 300
    assert _split_pre_block(block, 100) == _split_long_line(block, 100)


def test_split_pre_block_tiny_limit_falls_back() -> None:
    block = "<pre>aaa\nbbb</pre>"
    assert _split_pre_block(block, 5) == _split_long_line(block, 5)


def test_split_pre_block_wraps_long_inner_line() -> None:
    block = "<pre>" + "a" * 300 + "</pre>"
    out = _split_pre_block(block, 50)
    assert len(out) > 1
    assert all(m.startswith("<pre>") and m.endswith("</pre>") for m in out)
