# syntax=docker/dockerfile:1

# uv + Python 3.12 (Debian slim). git is required because the `screener`
# dependency is installed from a GitHub repo by `uv sync`.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# git: needed for the screener git dependency.
# ca-certificates: needed for outbound HTTPS (Telegram / Yahoo / FMP).
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Don't buffer stdout/stderr so logs show up live in Coolify.
ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

# Install dependencies first, against the lockfile, for layer caching.
# Source is copied afterwards so code changes don't bust the dep layer.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Now copy the application source and config.
# README.md is required: pyproject.toml declares `readme = "README.md"`,
# so the hatchling build below fails without it.
COPY main.py README.md ./
COPY screener_bot ./screener_bot
COPY config ./config

# Install the project itself into the environment.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Secrets and runtime config come from the environment (Coolify env vars),
# never from a baked-in .env. Defaults below are non-sensitive only.
ENV BOT_CONFIG_PATH=config/bot.yaml \
    LOG_LEVEL=INFO

# Outbound-only Telegram long-polling worker: no port, no HTTP server.
CMD ["uv", "run", "--no-dev", "python", "-m", "screener_bot"]
