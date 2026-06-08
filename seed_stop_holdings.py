"""One-off: insert AMD/MU/NET (with stop-losses) into the Turso portfolio.

Idempotent — uses INSERT OR IGNORE via portfolio_store.seed_portfolio, and the
stop_loss column migration runs inside ensure_portfolio_table. Safe to re-run.
"""

from __future__ import annotations

from screener_bot import portfolio_store

NEW_HOLDINGS = [
    {"symbol": "AMD", "market": "us", "avg_price": 485.00, "stop_loss": 447.00, "ruleset": "swing_momentum"},
    {"symbol": "MU", "market": "us", "avg_price": 941.92, "stop_loss": 860.00, "ruleset": "swing_momentum"},
    {"symbol": "NET", "market": "us", "avg_price": 252.92, "stop_loss": 230.00, "ruleset": "swing_momentum"},
]


def main() -> None:
    client = portfolio_store.connect()
    if client is None:
        raise SystemExit("Turso not configured (set TURSO_DATABASE_URL / TURSO_AUTH_TOKEN).")
    try:
        before = {(r["symbol"], r["market"]) for r in portfolio_store.fetch_portfolio(client)}
        portfolio_store.seed_portfolio(client, NEW_HOLDINGS)
        after = portfolio_store.fetch_portfolio(client)
        added = [r for r in after if (r["symbol"], r["market"]) not in before]
        print(f"Portfolio now has {len(after)} rows; newly inserted: {len(added)}")
        for r in after:
            if r["symbol"] in {"AMD", "MU", "NET"}:
                print(f"  {r['symbol']:5} avg={r['avg_price']} stop={r['stop_loss']} ruleset={r['ruleset']}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
