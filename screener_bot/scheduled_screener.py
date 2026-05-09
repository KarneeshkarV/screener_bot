from __future__ import annotations

import asyncio
import csv
from dataclasses import dataclass
from datetime import datetime
from html import escape
from io import StringIO
from pathlib import Path

from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from .config import BotConfig, ScreenerCommandConfig
from .formatting import split_messages


@dataclass
class CommandResult:
    config: ScreenerCommandConfig
    returncode: int | None
    stdout: str
    stderr: str
    timed_out: bool = False


class ScheduledScreenerService:
    def __init__(self, config: BotConfig) -> None:
        self.config = config

    def chat_ids(self) -> list[int]:
        configured = self.config.scheduled_screener.chat_ids
        return configured or self.config.telegram.allowed_chat_ids

    async def run(self, query: str | None = None) -> str:
        cfg = self.config.scheduled_screener
        commands = _matching_commands(cfg.commands, query)
        if not commands:
            if query:
                return (
                    f"<b>Screener Job</b>\n"
                    f"No screener command matched <code>{escape(query)}</code>."
                )
            return "<b>Screener Job</b>\nNo screener commands configured."

        results = []
        for command in commands:
            results.append(await self._run_command(command))
        return self._format_report(results, show_all=bool(query))

    async def _run_command(self, command: ScreenerCommandConfig) -> CommandResult:
        cfg = self.config.scheduled_screener
        configured_cwd = Path(cfg.working_directory)
        cwd = configured_cwd if configured_cwd.exists() else None
        try:
            proc = await asyncio.create_subprocess_exec(
                *command.command,
                cwd=cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout_raw, stderr_raw = await asyncio.wait_for(
                    proc.communicate(), timeout=cfg.timeout_seconds
                )
            except asyncio.TimeoutError:
                proc.kill()
                stdout_raw, stderr_raw = await proc.communicate()
                return CommandResult(
                    command,
                    proc.returncode,
                    stdout_raw.decode(errors="replace"),
                    stderr_raw.decode(errors="replace"),
                    timed_out=True,
                )
            return CommandResult(
                command,
                proc.returncode,
                stdout_raw.decode(errors="replace"),
                stderr_raw.decode(errors="replace"),
            )
        except Exception as exc:
            return CommandResult(command, None, "", str(exc))

    def _format_report(self, results: list[CommandResult], show_all: bool = False) -> str:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        lines = [f"<b>Screener Job</b> <i>{escape(timestamp)}</i>"]
        for result in results:
            status = "timed out" if result.timed_out else f"exit {result.returncode}"
            if result.returncode == 0 and not result.timed_out:
                status = "ok"
            lines.append("")
            lines.append(f"<b>{escape(result.config.label)}</b> ({escape(status)})")
            output = result.stdout.strip()
            error = result.stderr.strip()
            if output:
                lines.extend(_format_output(result.config.label, output, show_all=show_all))
            if error:
                filtered_error = _filter_stderr(error, success=result.returncode == 0)
                if filtered_error:
                    lines.append(
                        f"<i>stderr:</i>\n<pre>{escape(_truncate(filtered_error))}</pre>"
                    )
            if not output and not error:
                lines.append("<i>No output.</i>")
        return "\n".join(lines)


def _truncate(value: str, limit: int = 650) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 32].rstrip() + "\n... output truncated ..."


def _matching_commands(
    commands: list[ScreenerCommandConfig], query: str | None
) -> list[ScreenerCommandConfig]:
    if not query or not query.strip():
        return commands
    terms = query.lower().split()
    return [
        command
        for command in commands
        if all(term in command.label.lower() for term in terms)
    ]


def _format_output(label: str, output: str, show_all: bool = False) -> list[str]:
    rows = _parse_csv_rows(output)
    if not rows:
        return [f"<pre>{escape(_truncate(output))}</pre>"]
    limit = len(rows) if show_all else None
    if "ema" in label.lower():
        return _format_ema_rows(rows, limit=limit or 12)
    if "holding" in label.lower() or "insider" in label.lower() or "promoter" in label.lower():
        return _format_holding_rows(rows, limit=limit or 10)
    return _format_generic_rows(rows, limit=limit or 10)


def _parse_csv_rows(output: str) -> list[dict[str, str]]:
    try:
        csv_text = _extract_csv_text(output)
        if not csv_text:
            return []
        reader = csv.DictReader(StringIO(csv_text))
        if not reader.fieldnames or len(reader.fieldnames) < 2:
            return []
        return [{key: value for key, value in row.items()} for row in reader]
    except csv.Error:
        return []


def _extract_csv_text(output: str) -> str:
    lines = output.splitlines()
    for index, line in enumerate(lines):
        lowered = line.lower()
        if "," not in line:
            continue
        if (
            lowered.startswith("ticker,")
            or lowered.startswith("name,")
            or "promoter_change" in lowered
            or "yf_net_shares_6m" in lowered
        ):
            return "\n".join(lines[index:])
    return ""


def _format_ema_rows(rows: list[dict[str, str]], limit: int = 12) -> list[str]:
    lines = ["<pre>Symbol        Close    Chg%   Score"]
    for row in rows[:limit]:
        symbol = _clip(row.get("name") or row.get("ticker") or "-", 12)
        close = _fmt_float(row.get("close"), 2)
        change = _fmt_pct(row.get("change"))
        score = _fmt_float(row.get("setup_score"), 1)
        lines.append(f"{symbol:<12} {close:>8} {change:>7} {score:>7}")
    lines.append("</pre>")
    lines.extend(_extra_count(rows, limit))
    return lines


def _format_holding_rows(rows: list[dict[str, str]], limit: int = 10) -> list[str]:
    if rows and "promoter_change" in rows[0]:
        lines = ["<pre>Symbol        Prom%   ChgPP   FII%   DII%"]
        for row in rows[:limit]:
            symbol = _clip(row.get("name") or "-", 12)
            promoter = _fmt_float(row.get("promoter_pct_latest"), 2)
            change = _fmt_signed(row.get("promoter_change"), 2)
            fii = _fmt_float(row.get("fii_pct_latest"), 2)
            dii = _fmt_float(row.get("dii_pct_latest"), 2)
            lines.append(f"{symbol:<12} {promoter:>6} {change:>7} {fii:>6} {dii:>6}")
    else:
        lines = ["<pre>Symbol        Net Shrs  Net%  Buys Sells"]
        for row in rows[:limit]:
            symbol = _clip(row.get("name") or "-", 12)
            shares = _fmt_shares(row.get("yf_net_shares_6m"))
            pct = _fmt_fraction_pct(row.get("yf_net_pct_6m"))
            buys = _fmt_int(row.get("yf_buy_trans_6m"))
            sells = _fmt_int(row.get("yf_sell_trans_6m"))
            lines.append(f"{symbol:<12} {shares:>8} {pct:>6} {buys:>5} {sells:>5}")
    lines.append("</pre>")
    lines.extend(_extra_count(rows, limit))
    return lines


def _format_generic_rows(rows: list[dict[str, str]], limit: int = 10) -> list[str]:
    keys = list(rows[0].keys())[:4]
    lines = ["<pre>" + "  ".join(_clip(key, 12) for key in keys)]
    for row in rows[:limit]:
        lines.append("  ".join(_clip(row.get(key) or "-", 12) for key in keys))
    lines.append("</pre>")
    lines.extend(_extra_count(rows, limit))
    return lines


def _extra_count(rows: list[dict[str, str]], limit: int) -> list[str]:
    remaining = len(rows) - limit
    if remaining <= 0:
        return []
    return [f"<i>+{remaining} more rows</i>"]


def _clip(value: str, limit: int) -> str:
    value = str(value)
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "…"


def _to_float(value: str | None) -> float | None:
    if value in {None, "", "nan", "NaN", "None"}:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _fmt_float(value: str | None, digits: int) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    return f"{number:.{digits}f}"


def _fmt_signed(value: str | None, digits: int) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    return f"{number:+.{digits}f}"


def _fmt_pct(value: str | None) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    return f"{number:+.2f}%"


def _fmt_fraction_pct(value: str | None) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    return f"{number * 100:+.2f}%"


def _fmt_int(value: str | None) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    return f"{int(number)}"


def _fmt_shares(value: str | None) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    sign = "+" if number >= 0 else "-"
    magnitude = abs(number)
    if magnitude >= 1_000_000:
        return f"{sign}{magnitude / 1_000_000:.1f}M"
    if magnitude >= 1_000:
        return f"{sign}{magnitude / 1_000:.1f}K"
    return f"{sign}{magnitude:.0f}"


def _filter_stderr(error: str, success: bool) -> str:
    lines = [line for line in error.splitlines() if line.strip()]
    if success:
        lines = [
            line
            for line in lines
            if "HTTP Error 404" not in line and "Quote not found" not in line
        ]
    return "\n".join(lines)


async def send_screener_report(
    context: ContextTypes.DEFAULT_TYPE,
    service: ScheduledScreenerService,
    chat_ids: list[int] | None = None,
    query: str | None = None,
) -> None:
    report = await service.run(query=query)
    targets = chat_ids or service.chat_ids()
    for chat_id in targets:
        for message in split_messages(report):
            await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
            )
