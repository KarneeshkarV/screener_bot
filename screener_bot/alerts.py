from __future__ import annotations

import json
import logging
from datetime import datetime
from html import escape
from pathlib import Path

from .config import BotConfig
from .technical import TechnicalService, TechnicalStatus


class AlertService:
    """Detect notable per-holding changes and report only what changed.

    Each run computes a small set of boolean conditions per holding and diffs
    them against the previous run persisted on disk. A holding seen for the
    first time is recorded as a silent baseline so the first run never spams.
    """

    def __init__(
        self,
        config: BotConfig,
        technical_service: TechnicalService | None = None,
        state_path: Path | str | None = None,
    ) -> None:
        self.config = config
        self.technical_service = technical_service or TechnicalService(config)
        self.state_path = (
            Path(state_path)
            if state_path is not None
            else Path.home() / ".screener_bot" / "alert_state.json"
        )

    def chat_ids(self) -> list[int]:
        return self.config.alerts.chat_ids or self.config.telegram.allowed_chat_ids

    def evaluate(self) -> str | None:
        """Run a check and return a report of changes, or None if nothing changed."""
        statuses = self.technical_service.check_portfolio()
        prev = self._load_state()
        new_state: dict[str, dict] = {}
        sections: list[str] = []

        for status in statuses:
            symbol = status.item.symbol
            if status.error or status.close is None:
                # Preserve the last known baseline; never alert on a data gap.
                if symbol in prev:
                    new_state[symbol] = prev[symbol]
                continue

            cur = self._compute_flags(status)
            new_state[symbol] = cur
            old = prev.get(symbol)
            if old is None:
                continue  # silent baseline for a newly seen holding

            lines = self._diff(status, old, cur)
            if lines:
                sections.append(self._format_holding(status, lines))

        self._save_state(new_state)

        if not sections:
            return None
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        return "\n".join(
            [f"<b>🔔 Alerts</b> <i>{escape(timestamp)}</i>", "", *sections]
        )

    def _compute_flags(self, status: TechnicalStatus) -> dict:
        alerts = self.config.alerts
        close = status.close
        high = status.high_52w
        low = status.low_52w
        near_threshold = (
            high * (1 - alerts.near_high_pct / 100) if high is not None else None
        )
        at_high = bool(high is not None and close is not None and close >= high * 0.999)
        at_low = bool(low is not None and close is not None and close <= low * 1.001)
        near_high = bool(
            near_threshold is not None and close is not None and close >= near_threshold
        )
        vol_spike = bool(
            status.last_volume is not None
            and status.avg_volume_20
            and status.last_volume
            >= status.avg_volume_20 * alerts.volume_spike_multiple
        )
        return {
            "entry": status.entry.matched,
            "exit": status.exit.matched,
            "at_high": at_high,
            "at_low": at_low,
            "near_high": near_high,
            "vol_spike": vol_spike,
        }

    def _diff(self, status: TechnicalStatus, old: dict, cur: dict) -> list[str]:
        lines: list[str] = []

        if (
            cur["exit"] is not None
            and old.get("exit") is not None
            and cur["exit"] != old.get("exit")
        ):
            lines.append(
                "⚠️ Exit signal triggered" if cur["exit"] else "Exit signal cleared"
            )
        if (
            cur["entry"] is not None
            and old.get("entry") is not None
            and cur["entry"] != old.get("entry")
        ):
            lines.append(
                "🟢 Entry signal triggered" if cur["entry"] else "Entry signal cleared"
            )

        cur_symbol = self._currency(status)
        if cur["at_high"] and not old.get("at_high"):
            high = status.high_52w or 0.0
            lines.append(f"🚀 New 52-week high ({cur_symbol}{high:.2f})")
        elif cur["near_high"] and not old.get("near_high"):
            pct = self.config.alerts.near_high_pct
            extra = ""
            if status.high_52w and status.close is not None:
                gap = (status.high_52w - status.close) / status.high_52w * 100
                extra = f" (−{gap:.1f}% away)"
            lines.append(f"📈 Within {pct:g}% of 52-week high{extra}")

        if cur["at_low"] and not old.get("at_low"):
            low = status.low_52w or 0.0
            lines.append(f"🔻 New 52-week low ({cur_symbol}{low:.2f})")

        if cur["vol_spike"] and not old.get("vol_spike"):
            rel = status.last_volume / status.avg_volume_20
            lines.append(f"🔊 Volume spike: {rel:.1f}× 20d avg")

        return lines

    def _format_holding(self, status: TechnicalStatus, lines: list[str]) -> str:
        item = status.item
        flag = "🇮🇳" if item.market == "india" else "🇺🇸"
        symbol = item.symbol.split(":")[-1]
        cur = self._currency(status)
        daily = (
            f" ({status.daily_change_pct:+.2f}%)"
            if status.daily_change_pct is not None
            else ""
        )
        head = f"{flag} <b>{escape(symbol)}</b> {cur}{status.close:.2f}{daily}"
        return "\n".join([head, *(f"  {line}" for line in lines)])

    @staticmethod
    def _currency(status: TechnicalStatus) -> str:
        return "₹" if status.item.market == "india" else "$"

    def _load_state(self) -> dict[str, dict]:
        try:
            with self.state_path.open("r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}
        if not isinstance(raw, dict):
            return {}
        return {
            symbol: flags
            for symbol, flags in raw.items()
            if isinstance(symbol, str) and isinstance(flags, dict)
        }

    def _save_state(self, state: dict[str, dict]) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            with self.state_path.open("w", encoding="utf-8") as fh:
                json.dump(state, fh, indent=2, sort_keys=True)
        except OSError:
            logging.warning("could not persist alert state to %s", self.state_path)
