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


def _currency(market: str) -> str:
    return "₹" if market == "india" else "$"


def _fmt_pnl(close: float, avg_price: float, market: str) -> str:
    cur = _currency(market)
    diff = close - avg_price
    pct = (diff / avg_price) * 100
    sign = "+" if diff >= 0 else "-"
    return f"{sign}{cur}{abs(diff):.2f} ({sign}{abs(pct):.2f}%)"


def _signal(matched: bool | None) -> str:
    if matched is True:
        return "✅ Yes"
    if matched is False:
        return "❌ No"
    return "—"


def format_portfolio_report(
    technical: Iterable[TechnicalStatus],
    ownership: dict[str, OwnershipStatus],
) -> str:
    lines = ["<b>📊 Portfolio Check</b>"]
    divider = "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄"

    for status in technical:
        item = status.item
        cur = _currency(item.market)
        flag = "🇮🇳" if item.market == "india" else "🇺🇸"
        symbol = item.symbol.split(":")[-1]

        lines.append("")
        lines.append(divider)
        lines.append(f"{flag} <b>{escape(symbol)}</b>")

        if status.error:
            lines.append(f"  <b>Price:</b> {escape(status.error)}")
        else:
            daily = (
                f"  ({status.daily_change_pct:+.2f}%)"
                if status.daily_change_pct is not None
                else ""
            )
            lines.append(f"  <b>Price:</b> {cur}{_fmt_number(status.close)}{daily}")

            if item.avg_price is not None and status.close is not None:
                lines.append(
                    f"  <b>Avg Cost:</b> {cur}{_fmt_number(item.avg_price)}  "
                    f"<b>P&amp;L:</b> {escape(_fmt_pnl(status.close, item.avg_price, item.market))}"
                )

        if status.snapshot:
            parts = []
            for result in status.snapshot:
                val = "n/a" if result.error else _fmt_number(result.value)
                parts.append(f"{escape(result.label)}: <b>{escape(val)}</b>")
            lines.append("  " + "  ·  ".join(parts))

        lines.append(
            f"  <b>Entry:</b> {_signal(status.entry.matched)}    "
            f"<b>Exit:</b> {_signal(status.exit.matched)}"
        )

        owner = ownership.get(item.symbol)
        if owner is not None:
            if owner.market == "india":
                quarter = f" <i>{escape(owner.latest_quarter)}</i>" if owner.latest_quarter else ""
                lines.append(f"  <b>Shareholding{quarter}:</b>")
                lines.append(
                    f"    Promoter <b>{_fmt_number(owner.promoter_pct_latest)}%</b> ({_fmt_delta(owner.promoter_change)})"
                    f"  ·  FII <b>{_fmt_number(owner.fii_pct_latest)}%</b> ({_fmt_delta(owner.fii_change)})"
                    f"  ·  DII <b>{_fmt_number(owner.dii_pct_latest)}%</b> ({_fmt_delta(owner.dii_change)})"
                )
                if owner.error:
                    lines.append(f"  <i>Note: {escape(owner.error)}</i>")
            else:
                lines.append(
                    f"  <b>Insiders 6m:</b> "
                    f"net <b>{_fmt_number(owner.yf_net_shares_6m, 0)}</b> shares  "
                    f"(<b>{_fmt_number(owner.yf_net_pct_6m)}%</b>)"
                )
                if owner.error:
                    lines.append(f"  <i>Note: {escape(owner.error)}</i>")

    return "\n".join(lines)


def split_messages(text: str, limit: int = SAFE_LIMIT) -> list[str]:
    if len(text) <= limit:
        return [text]
    messages: list[str] = []
    current: list[str] = []
    current_len = 0

    def flush_current() -> None:
        nonlocal current, current_len
        if current:
            messages.append("\n".join(current))
            current = []
            current_len = 0

    in_pre = False
    pre_lines: list[str] = []
    for line in text.splitlines():
        if in_pre:
            pre_lines.append(line)
            if "</pre>" in line:
                flush_current()
                messages.extend(_split_pre_block("\n".join(pre_lines), limit))
                pre_lines = []
                in_pre = False
            continue
        if "<pre>" in line and "</pre>" not in line:
            flush_current()
            pre_lines = [line]
            in_pre = True
            continue

        needed = len(line) + 1
        if current and current_len + needed > limit:
            flush_current()
        if needed > limit:
            flush_current()
            messages.extend(_split_long_line(line, limit))
            continue
        current.append(line)
        current_len += needed
    if pre_lines:
        flush_current()
        messages.extend(_split_pre_block("\n".join(pre_lines), limit))
    flush_current()
    return messages


def _split_long_line(line: str, limit: int) -> list[str]:
    return [line[start : start + limit] for start in range(0, len(line), limit)]


def _split_pre_block(block: str, limit: int) -> list[str]:
    if len(block) <= limit:
        return [block]
    prefix = "<pre>"
    suffix = "</pre>"
    if not block.startswith(prefix) or not block.endswith(suffix):
        return _split_long_line(block, limit)

    body = block[len(prefix) : -len(suffix)]
    chunk_limit = limit - len(prefix) - len(suffix)
    if chunk_limit <= 0:
        return _split_long_line(block, limit)

    messages: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in body.splitlines():
        needed = len(line) + 1
        if current and current_len + needed > chunk_limit:
            messages.append(prefix + "\n".join(current) + suffix)
            current = []
            current_len = 0
        if needed > chunk_limit:
            for part in _split_long_line(line, chunk_limit):
                messages.append(prefix + part + suffix)
            continue
        current.append(line)
        current_len += needed
    if current:
        messages.append(prefix + "\n".join(current) + suffix)
    return messages
