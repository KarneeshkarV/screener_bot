# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
uv sync --all-groups       # install all deps including dev
uv run python -m screener_bot   # run the bot locally

uv run pytest              # run all tests
uv run pytest tests/test_alerts.py   # run a single test file
uv run pytest -k "test_name"         # run a single test by name

uv run ruff check $(git ls-files '*.py')          # lint
uv run ruff format --check $(git ls-files '*.py') # format check
uv run ruff format $(git ls-files '*.py')         # auto-format
uv run mypy                # type-check screener_bot/ (strict, with suppressions)
```

## Architecture

The bot is a `python-telegram-bot` (v21+) polling bot that wraps a sibling `screener` package.

**Entry point**: `main.py` → `screener_bot/__main__.py` → `build_application()` in `bot.py`.

**Configuration** (`config.py`):
- `EnvSettings` (pydantic-settings): reads `.env` for `TELEGRAM_BOT_TOKEN`, `TELEGRAM_ALLOWED_CHAT_IDS`, `TURSO_DATABASE_URL`, `TURSO_AUTH_TOKEN`, `LOG_LEVEL`.
- `BotConfig` (pydantic): portfolio items, rulesets, scheduled screener times, alert thresholds, timezone. All defaults live as constants (`DEFAULT_*`) at the bottom of `config.py`; there is no `config/bot.yaml` in the current implementation.
- Portfolio items are fetched at startup from Turso/libSQL via `portfolio_store.py` (table `bot_portfolio`). No YAML file is used.

**Services** (all injected into `build_application`):
- `TechnicalService` (`technical.py`): fetches ~370 days of OHLCV from yfinance via the `screener` package, evaluates Pine-like expressions (RSI, EMA, ATR, etc.). Uses `CachedPriceFetcher` (30-min TTL) to avoid redundant fetches across `/check_portfolio`, `/stock`, and the hourly alert job.
- `AlertService` (`alerts.py`): diff-based alert engine. On each run it computes boolean flags per holding (entry/exit signal, 52w high/low, volume spike), diffs against `~/.screener_bot/alert_state.json`, and only reports what changed. First run is always a silent baseline.
- `ScheduledScreenerService` (`scheduled_screener.py`): spawns the external `screener` CLI as subprocesses, parses CSV output, diffs symbol lists against `~/.screener_bot/screener_snapshots.json` to report added/removed entries.
- `OwnershipService` (`ownership.py`): per-holding ownership/promoter data.

**Scheduled jobs** (registered in `bot.py::_post_init`):
- Daily portfolio report at `06:00` local time (configurable timezone, default `Asia/Kolkata`).
- Screener jobs at configured times (default `16:00` and `02:30`).
- Hourly alert check (interval configurable via `alerts.interval_minutes`).

**Authorization**: `_guard()` in `bot.py` checks every command against `config.telegram.allowed_chat_ids`. The chat ID list comes from `TELEGRAM_ALLOWED_CHAT_IDS` in `.env`.

**Message splitting**: Telegram has a 4096-char limit; `formatting.py::split_messages` handles this.

## Key Design Constraints

- All CPU-bound work (price fetches, indicator eval, alert diff) runs in `asyncio.to_thread()` to avoid blocking the event loop.
- Data gaps never produce spurious alerts — missing price data preserves the previous baseline rather than clearing it.
- `CachedPriceFetcher` is shared between `TechnicalService` and `AlertService` (passed via constructor) so the portfolio check and the alert job don't double-fetch.
- The `screener` package is a git dependency (`github.com/KarneeshkarV/screener`); its `backtester.data` and `backtester.pine` modules provide the price fetcher and expression evaluator respectively.
