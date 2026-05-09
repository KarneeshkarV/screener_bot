from __future__ import annotations

from collections.abc import Iterable
from html import escape
from typing import Any

from .ownership import OwnershipStatus
from .technical import TechnicalStatus


TELEGRAM_LIMIT = 4096
SAFE_LIMIT = 3800


def _fmt_number(value: Any, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (int, float)):
        return f"{value:.{digits}f}"
    return str(value)


def _fmt_delta(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.2f} pp"


def _fmt_rule(value: bool | None) -> str:
    if value is None:
        return "n/a"
    return "yes" if value else "no"


def format_portfolio_report(
    technical: Iterable[TechnicalStatus],
    ownership: dict[str, OwnershipStatus],
) -> str:
    lines = ["<b>Portfolio Check</b>"]
    for status in technical:
        item = status.item
        lines.append("")
        lines.append(f"<b>{escape(item.symbol)}</b> ({escape(item.market)})")
        if status.error:
            lines.append(f"Price: {escape(status.error)}")
        else:
            daily = (
                f" ({status.daily_change_pct:+.2f}%)"
                if status.daily_change_pct is not None
                else ""
            )
            lines.append(f"Close: {_fmt_number(status.close)}{daily}")

        snapshot = ", ".join(
            f"{escape(result.label)}: {escape(_fmt_number(result.value))}"
            if not result.error
            else f"{escape(result.label)}: n/a"
            for result in status.snapshot
        )
        if snapshot:
            lines.append(snapshot)
        lines.append(
            f"Entry: {_fmt_rule(status.entry.matched)} | Exit: {_fmt_rule(status.exit.matched)}"
        )

        owner = ownership.get(item.symbol)
        if owner is None:
            lines.append("Ownership: n/a")
        elif owner.market == "india":
            parts = [
                f"Promoter {_fmt_number(owner.promoter_pct_latest)}% ({_fmt_delta(owner.promoter_change)})",
                f"FII {_fmt_number(owner.fii_pct_latest)}% ({_fmt_delta(owner.fii_change)})",
                f"DII {_fmt_number(owner.dii_pct_latest)}% ({_fmt_delta(owner.dii_change)})",
            ]
            quarter = f" {escape(owner.latest_quarter)}" if owner.latest_quarter else ""
            lines.append(f"Shareholding{quarter}: " + "; ".join(parts))
            if owner.error:
                lines.append(f"Shareholding note: {escape(owner.error)}")
        else:
            lines.append(
                "Insiders 6m: "
                f"net shares {_fmt_number(owner.yf_net_shares_6m, 0)}, "
                f"net {_fmt_number(owner.yf_net_pct_6m)}%"
            )
            if owner.error:
                lines.append(f"Insider note: {escape(owner.error)}")
    return "\n".join(lines)


def split_messages(text: str, limit: int = SAFE_LIMIT) -> list[str]:
    if len(text) <= limit:
        return [text]
    messages: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in text.splitlines():
        needed = len(line) + 1
        if current and current_len + needed > limit:
            messages.append("\n".join(current))
            current = []
            current_len = 0
        if needed > limit:
            for start in range(0, len(line), limit):
                messages.append(line[start : start + limit])
            continue
        current.append(line)
        current_len += needed
    if current:
        messages.append("\n".join(current))
    return messages
