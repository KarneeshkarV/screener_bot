"""Portfolio storage backed by Turso/libSQL."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)

TABLE_NAME = "bot_portfolio"

# Ruleset assigned to holdings created via bot commands; matches
# config.DEFAULT_RULESETS.
DEFAULT_RULESET = "swing_momentum"


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
            stop_loss REAL,
            ruleset TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            UNIQUE(symbol, market)
        )
        """
    )
    # Migration: add stop_loss to tables created before the column existed.
    columns = {str(row[1]) for row in client.execute(f"PRAGMA table_info({TABLE_NAME})").rows}
    if "stop_loss" not in columns:
        client.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN stop_loss REAL")


def fetch_portfolio(client: _Client) -> list[dict[str, Any]]:
    ensure_portfolio_table(client)
    rows = client.execute(
        f"SELECT symbol, market, avg_price, stop_loss, ruleset FROM {TABLE_NAME} ORDER BY id"
    ).rows
    return [
        {
            "symbol": str(row[0]),
            "market": str(row[1]),
            "avg_price": float(row[2]) if row[2] is not None else None,
            "stop_loss": float(row[3]) if row[3] is not None else None,
            "ruleset": str(row[4]),
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
            INSERT OR IGNORE INTO {TABLE_NAME} (symbol, market, avg_price, stop_loss, ruleset)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                item["symbol"],
                item["market"],
                item.get("avg_price"),
                item.get("stop_loss"),
                item["ruleset"],
            ],
        )
        inserted += 1
    return inserted


def portfolio_is_empty(client: _Client) -> bool:
    ensure_portfolio_table(client)
    rows = client.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").rows
    return int(rows[0][0]) == 0


def upsert_holding(
    client: _Client,
    symbol: str,
    market: str,
    avg_price: float,
    stop_loss: float | None = None,
    ruleset: str = DEFAULT_RULESET,
) -> dict[str, Any]:
    """Insert or update a holding; returns the saved row.

    When ``stop_loss`` is omitted an existing stop is kept; the ruleset of an
    existing row is never changed.
    """
    ensure_portfolio_table(client)
    client.execute(
        f"""
        INSERT INTO {TABLE_NAME} (symbol, market, avg_price, stop_loss, ruleset)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(symbol, market) DO UPDATE SET
            avg_price = excluded.avg_price,
            stop_loss = COALESCE(excluded.stop_loss, stop_loss)
        """,
        [symbol, market, avg_price, stop_loss, ruleset],
    )
    rows = client.execute(
        f"SELECT symbol, market, avg_price, stop_loss, ruleset FROM {TABLE_NAME} "
        "WHERE symbol = ? AND market = ?",
        [symbol, market],
    ).rows
    row = rows[0]
    return {
        "symbol": str(row[0]),
        "market": str(row[1]),
        "avg_price": float(row[2]) if row[2] is not None else None,
        "stop_loss": float(row[3]) if row[3] is not None else None,
        "ruleset": str(row[4]),
    }


def delete_holding(client: _Client, symbol: str) -> int:
    """Delete all holdings for ``symbol``; returns the number of rows removed."""
    ensure_portfolio_table(client)
    rows = client.execute(
        f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE symbol = ?", [symbol]
    ).rows
    count = int(rows[0][0])
    if count:
        client.execute(f"DELETE FROM {TABLE_NAME} WHERE symbol = ?", [symbol])
    return count


def update_stop_loss(client: _Client, symbol: str, stop_loss: float) -> int:
    """Set the stop-loss for ``symbol``; returns the number of rows updated."""
    ensure_portfolio_table(client)
    rows = client.execute(
        f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE symbol = ?", [symbol]
    ).rows
    count = int(rows[0][0])
    if count:
        client.execute(
            f"UPDATE {TABLE_NAME} SET stop_loss = ? WHERE symbol = ?",
            [stop_loss, symbol],
        )
    return count


class PortfolioRepo:
    """Synchronous CRUD facade that opens a fresh Turso client per operation.

    Handlers run these methods via ``asyncio.to_thread`` to keep blocking
    network I/O off the event loop.
    """

    def _client(self) -> _Client:
        client = connect()
        if client is None:
            raise RuntimeError(
                "Turso is not configured. Set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN."
            )
        return client

    def upsert(
        self,
        symbol: str,
        market: str,
        avg_price: float,
        stop_loss: float | None = None,
        ruleset: str = DEFAULT_RULESET,
    ) -> dict[str, Any]:
        client = self._client()
        try:
            return upsert_holding(client, symbol, market, avg_price, stop_loss, ruleset)
        finally:
            client.close()

    def remove(self, symbol: str) -> int:
        client = self._client()
        try:
            return delete_holding(client, symbol)
        finally:
            client.close()

    def set_stop(self, symbol: str, stop_loss: float) -> int:
        client = self._client()
        try:
            return update_stop_loss(client, symbol, stop_loss)
        finally:
            client.close()
