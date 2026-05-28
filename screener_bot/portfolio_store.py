"""Portfolio storage backed by Turso/libSQL."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)

TABLE_NAME = "bot_portfolio"


class _Client(Protocol):
    def execute(self, stmt: str, args: list[object] | None = None): ...

    def close(self) -> None: ...


def _load_env_file(path: Path = Path(".env")) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _env_value(name: str) -> str | None:
    return os.environ.get(name) or _load_env_file().get(name)


def _database_url() -> str | None:
    url = _env_value("TURSO_DATABASE_URL")
    if url and url.startswith("libsql://"):
        return url.replace("libsql://", "https://", 1)
    return url


def connect() -> _Client | None:
    url = _database_url()
    token = _env_value("TURSO_AUTH_TOKEN")
    if not url or not token:
        return None

    from libsql_client import create_client_sync  # type: ignore[import-untyped]

    return create_client_sync(url, auth_token=token)


def ensure_portfolio_table(client: _Client) -> None:
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            market TEXT NOT NULL,
            avg_price REAL,
            ruleset TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            UNIQUE(symbol, market)
        )
        """
    )


def fetch_portfolio(client: _Client) -> list[dict[str, Any]]:
    ensure_portfolio_table(client)
    rows = client.execute(
        f"SELECT symbol, market, avg_price, ruleset FROM {TABLE_NAME} ORDER BY id"
    ).rows
    return [
        {
            "symbol": str(row[0]),
            "market": str(row[1]),
            "avg_price": float(row[2]) if row[2] is not None else None,
            "ruleset": str(row[3]),
        }
        for row in rows
    ]


def seed_portfolio(client: _Client, items: list[dict[str, Any]]) -> int:
    """Insert items into the portfolio table; skips duplicates. Returns inserted count."""
    ensure_portfolio_table(client)
    inserted = 0
    for item in items:
        client.execute(
            f"""
            INSERT OR IGNORE INTO {TABLE_NAME} (symbol, market, avg_price, ruleset)
            VALUES (?, ?, ?, ?)
            """,
            [
                item["symbol"],
                item["market"],
                item.get("avg_price"),
                item["ruleset"],
            ],
        )
        inserted += 1
    return inserted


def portfolio_is_empty(client: _Client) -> bool:
    ensure_portfolio_table(client)
    rows = client.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").rows
    return int(rows[0][0]) == 0
