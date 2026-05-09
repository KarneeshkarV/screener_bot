from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from html import escape
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

    async def run(self) -> str:
        cfg = self.config.scheduled_screener
        if not cfg.commands:
            return "<b>Screener Job</b>\nNo screener commands configured."

        results = []
        for command in cfg.commands:
            results.append(await self._run_command(command))
        return self._format_report(results)

    async def _run_command(self, command: ScreenerCommandConfig) -> CommandResult:
        cfg = self.config.scheduled_screener
        cwd = Path(cfg.working_directory)
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

    def _format_report(self, results: list[CommandResult]) -> str:
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
                lines.append(f"<pre>{escape(_truncate(output))}</pre>")
            if error:
                lines.append(f"<i>stderr:</i>\n<pre>{escape(_truncate(error))}</pre>")
            if not output and not error:
                lines.append("<i>No output.</i>")
        return "\n".join(lines)


def _truncate(value: str, limit: int = 650) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 32].rstrip() + "\n... output truncated ..."


async def send_screener_report(
    context: ContextTypes.DEFAULT_TYPE,
    service: ScheduledScreenerService,
    chat_ids: list[int] | None = None,
) -> None:
    report = await service.run()
    targets = chat_ids or service.chat_ids()
    for chat_id in targets:
        for message in split_messages(report):
            await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
            )
