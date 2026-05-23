## Screener Bot

Telegram polling bot for on-demand portfolio checks using the sibling
`../screener` package.

### Setup

```bash
uv sync
cp .env.example .env
```

Edit `.env` with `TELEGRAM_BOT_TOKEN` and update `config/bot.yaml` with your
Telegram chat ID and holdings.

Run locally:

```bash
uv run python -m screener_bot
```

### Commands

- `/start`
- `/help`
- `/status`
- `/run` — run screener changes now (`/run india ema` for one screener)
- `/run_all` — run all screeners and show the full lists
- `/check_portfolio` — check every configured holding
- `/stock SYMBOL [us|india]` — detailed technicals plus a candlestick chart
  (EMA 20/50/200 overlay + volume)
- `/alerts` — run a change check now

Only chat IDs listed in `telegram.allowed_chat_ids` can use the bot.

### Alerts

The bot runs a change-based check on every holding on a recurring interval
(default hourly) and only messages you when something changed since the last
check:

- entry/exit ruleset signal flips,
- a new 52-week high or low,
- price moving within `near_high_pct`% of the 52-week high,
- a volume spike above `volume_spike_multiple`× the 20-day average.

The first run records a silent baseline, and data gaps never produce spurious
alerts. State is persisted to `~/.screener_bot/alert_state.json`. Configure it
under `alerts:` in `config/bot.yaml`:

```yaml
alerts:
  enabled: true
  interval_minutes: 60
  near_high_pct: 15
  volume_spike_multiple: 2.0
  # chat_ids: []   # defaults to telegram.allowed_chat_ids
```

### Deployment

`render.yaml` defines a Render worker that starts polling with:

```bash
uv run python -m screener_bot
```

For deployment, replace the local editable screener source in `pyproject.toml`
with:

```toml
[tool.uv.sources]
screener = { git = "https://github.com/KarneeshkarV/screener.git" }
```
