"""Telegram HTML formatting for paper trading reports.

Mirrors the style used by the existing ``formatting`` module.
"""

from __future__ import annotations

from html import escape

from .engine import DailyReport
from .portfolio import TradeAction


def _currency(market: str) -> str:
    return "₹" if market == "india" else "$"


def _fmt_money(value: float, market: str) -> str:
    cur = _currency(market)
    if market == "india":
        # Indian-style formatting: ₹10,23,450
        abs_val = abs(value)
        if abs_val < 1000:
            formatted = f"{abs_val:,.2f}"
        else:
            # Split into last 3 digits and rest grouped by 2
            s = f"{abs_val:.2f}"
            integer_part, decimal = s.split(".")
            if len(integer_part) <= 3:
                formatted = f"{integer_part}.{decimal}"
            else:
                last3 = integer_part[-3:]
                rest = integer_part[:-3]
                groups = []
                while rest:
                    groups.append(rest[-2:])
                    rest = rest[:-2]
                groups.reverse()
                formatted = f"{','.join(groups)},{last3}.{decimal}"
        sign = "-" if value < 0 else ""
        return f"{sign}{cur}{formatted}"
    return f"{cur}{value:,.2f}"


def _fmt_pct(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}%"


def _action_emoji(action: TradeAction) -> str:
    if action.side == "buy":
        return "🟢 BUY"
    reason_map = {
        "stop": "🔴 STOP",
        "trail": "🟡 TRAIL",
        "target": "🎯 TARGET",
        "exit_signal": "🔴 EXIT",
    }
    return reason_map.get(action.reason, "🔴 SELL")


def format_daily_report(reports: list[DailyReport]) -> str:
    """Format the daily paper trading summary as Telegram HTML."""
    from datetime import date

    if not reports:
        return "📊 <b>Paper Trading</b> — No enabled portfolios."

    lines = [
        f"📊 <b>Paper Trading Daily Report — {date.today().isoformat()}</b>",
        "",
    ]

    for report in reports:
        market_flag = "🇮🇳" if report.market == "india" else "🇺🇸"
        cur = _currency(report.market)

        lines.append(f"━━━ {market_flag} <b>{escape(report.portfolio_name)}</b> ━━━")

        total_return = 0.0
        if report.initial_capital > 0:
            total_return = (
                (report.portfolio_value / report.initial_capital - 1) * 100
                if report.portfolio_value > 0
                else (report.current_cash / report.initial_capital - 1) * 100
            )

        lines.append(
            f"💰 Capital: {_fmt_money(report.current_cash, report.market)} "
            f"({_fmt_pct(total_return)})"
        )
        lines.append(f"📈 Open: {report.open_count}/{report.total_slots} slots")
        lines.append("")

        if report.actions:
            lines.append("<b>Today's actions:</b>")
            for action in report.actions:
                emoji = _action_emoji(action)
                line = f"  {emoji} {escape(action.ticker)} @ {cur}{action.price:.2f}"
                if action.pnl is not None and action.return_pct is not None:
                    line += f" → {_fmt_pct(action.return_pct * 100)}"
                    if action.days_held is not None:
                        line += f" ({action.days_held}d)"
                lines.append(line)
        else:
            lines.append("No actions today.")

        if report.errors:
            lines.append("")
            for error in report.errors:
                lines.append(f"⚠️ {escape(error)}")

        lines.append("")

    return "\n".join(lines)


def format_portfolio_status(status: dict) -> str:
    """Format portfolio status for /paper_status command."""
    pf = status["portfolio"]
    market = pf["market"]
    cur = _currency(market)
    market_flag = "🇮🇳" if market == "india" else "🇺🇸"

    enabled_label = "✅ Enabled" if pf["enabled"] else "❌ Disabled"

    lines = [
        f"{market_flag} <b>{escape(pf['name'])}</b> — {enabled_label}",
        f"Strategy: {escape(pf['strategy'])}",
        f"💰 Portfolio Value: {_fmt_money(status['portfolio_value'], market)} "
        f"({_fmt_pct(status['total_return_pct'])})",
        f"💵 Cash: {_fmt_money(pf['current_cash'], market)}",
        f"📈 Positions: {len(status['positions'])}/{pf['slots']} slots",
        "",
    ]

    if status["positions"]:
        lines.append("<b>Open Positions:</b>")
        for pos in status["positions"]:
            ticker = escape(pos["ticker"])
            entry_px = pos["entry_price"]
            current_px = pos.get("current_price")
            unrealized_pct = pos.get("unrealized_pct", 0.0)
            days = pos.get("days_held", 0)

            if current_px:
                lines.append(
                    f"  {ticker}  {cur}{current_px:.2f}  "
                    f"{_fmt_pct(unrealized_pct)}  ({days}d)"
                )
            else:
                lines.append(f"  {ticker}  entry {cur}{entry_px:.2f}  ({days}d)")

        unrealized = status["total_unrealized_pnl"]
        lines.append(
            f"\nUnrealized P&L: {_fmt_money(unrealized, market)}"
        )
    else:
        lines.append("No open positions.")

    return "\n".join(lines)


def format_portfolios_list(statuses: list[dict]) -> str:
    """Format list of all paper portfolios for /paper_portfolios."""
    if not statuses:
        return "📊 <b>Paper Portfolios</b> — None configured."

    lines = ["📊 <b>Paper Portfolios</b>", ""]

    for status in statuses:
        pf = status["portfolio"]
        market_flag = "🇮🇳" if pf["market"] == "india" else "🇺🇸"
        enabled = "✅" if pf["enabled"] else "❌"
        value = _fmt_money(status["portfolio_value"], pf["market"])
        ret = _fmt_pct(status["total_return_pct"])
        pos_count = len(status["positions"])

        lines.append(
            f"{enabled} {market_flag} <b>{escape(pf['name'])}</b>  "
            f"{value} ({ret})  "
            f"{pos_count}/{pf['slots']} slots"
        )

    return "\n".join(lines)


def format_trades(trades: list[dict], portfolio_name: str, market: str) -> str:
    """Format recent trades for /paper_trades."""
    cur = _currency(market)

    if not trades:
        return f"📊 <b>{escape(portfolio_name)}</b> — No trades yet."

    lines = [f"📊 <b>Recent Trades — {escape(portfolio_name)}</b>", ""]

    for t in trades:
        pnl_pct = t["return_pct"] * 100
        emoji = "🟢" if t["pnl"] > 0 else "🔴"
        lines.append(
            f"{emoji} {escape(t['ticker'])}  "
            f"{cur}{t['entry_price']:.2f} → {cur}{t['exit_price']:.2f}  "
            f"{_fmt_pct(pnl_pct)}  "
            f"{t['exit_reason']}  {t['days_held']}d"
        )

    return "\n".join(lines)


def format_metrics(metrics: dict, portfolio_name: str, market: str) -> str:
    """Format performance metrics."""
    if not metrics or metrics.get("trade_count", 0) == 0:
        return f"📊 <b>{escape(portfolio_name)}</b> — No trades for metrics."

    cur = _currency(market)
    lines = [
        f"📊 <b>Performance — {escape(portfolio_name)}</b>",
        "",
        f"Trades: {metrics['trade_count']} "
        f"({metrics['winning_trades']}W / {metrics['losing_trades']}L)",
        f"Hit Rate: {metrics['hit_rate']:.1f}%",
        f"Total P&L: {_fmt_money(metrics['total_pnl'], market)}",
        f"Avg Return: {_fmt_pct(metrics['avg_return_pct'])}",
        f"Best Trade: {_fmt_pct(metrics['best_trade'])}",
        f"Worst Trade: {_fmt_pct(metrics['worst_trade'])}",
        f"Profit Factor: {metrics['profit_factor']:.2f}",
        f"Avg Days Held: {metrics['avg_days_held']:.1f}",
    ]
    return "\n".join(lines)


def format_weekly_report(statuses: list[dict], weekly_trades: dict[str, list[dict]]) -> str:
    """Format the weekly paper trading summary."""
    from datetime import date, timedelta

    week_start = date.today() - timedelta(days=7)

    if not statuses:
        return "📊 <b>Paper Trading Weekly Report</b> — No portfolios."

    lines = [
        f"📊 <b>Paper Trading Weekly Report — Week of {week_start.isoformat()}</b>",
        "",
    ]

    for status in statuses:
        pf = status["portfolio"]
        name = pf["name"]
        market_flag = "🇮🇳" if pf["market"] == "india" else "🇺🇸"

        trades = weekly_trades.get(name, [])
        week_pnl = sum(t["pnl"] for t in trades)
        week_return = (week_pnl / pf["initial_capital"]) * 100 if pf["initial_capital"] > 0 else 0
        total_return = status["total_return_pct"]

        buys = [t for t in trades if t.get("exit_reason") == "entry_signal"]
        sells = [t for t in trades]
        winners = [t for t in trades if t["pnl"] > 0]
        win_rate = (len(winners) / len(trades) * 100) if trades else 0

        lines.append(f"━━━ {market_flag} <b>{escape(name)}</b> ━━━")
        lines.append(
            f"Week: {_fmt_pct(week_return)} | Total: {_fmt_pct(total_return)}"
        )
        lines.append(
            f"Trades: {len(trades)} | Win rate: {win_rate:.0f}%"
        )

        if trades:
            best = max(trades, key=lambda t: t["return_pct"])
            worst = min(trades, key=lambda t: t["return_pct"])
            lines.append(
                f"Best: {escape(best['ticker'])} {_fmt_pct(best['return_pct'] * 100)} | "
                f"Worst: {escape(worst['ticker'])} {_fmt_pct(worst['return_pct'] * 100)}"
            )
        lines.append("")

    return "\n".join(lines)
