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
- `/check_portfolio`

Only chat IDs listed in `telegram.allowed_chat_ids` can use the bot.

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
