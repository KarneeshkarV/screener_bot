from __future__ import annotations

import logging
from pathlib import Path

import yaml

from . import portfolio_store
from .bot import build_application
from .config import BotConfig, load_config, load_settings
from .paper.store import PaperStore

logger = logging.getLogger(__name__)


def _seed_portfolio_from_yaml(yaml_path: Path) -> None:
    """One-time migration: seed Turso from bot.yaml if the portfolio table is empty."""
    client = portfolio_store.connect()
    if client is None:
        return
    try:
        if not portfolio_store.portfolio_is_empty(client):
            return
        if not yaml_path.exists():
            return
        raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
        items = raw.get("portfolio") or []
        if not items:
            return
        inserted = portfolio_store.seed_portfolio(client, items)
        logger.info("Seeded %d portfolio rows into Turso from %s", inserted, yaml_path)
    finally:
        client.close()


def _seed_paper_portfolios(config: BotConfig) -> None:
    """Upsert paper trading portfolios from config into Turso.

    Runs on every startup so config changes are reflected in the database.
    """
    if not config.paper_trading.portfolios:
        return
    store = PaperStore()
    for name, pf_cfg in config.paper_trading.portfolios.items():
        try:
            store.upsert_portfolio(
                name=name,
                market=pf_cfg.market,
                strategy=pf_cfg.strategy,
                enabled=pf_cfg.enabled,
                initial_capital=pf_cfg.initial_capital,
                slots=pf_cfg.slots,
                stop_loss_pct=pf_cfg.stop_loss_pct,
                take_profit_pct=pf_cfg.take_profit_pct,
                trailing_stop_pct=pf_cfg.trailing_stop_pct,
                slippage_bps=pf_cfg.slippage_bps,
                tickers=pf_cfg.tickers,
            )
            logger.info("paper portfolio '%s' upserted", name)
        except Exception:
            logger.exception("failed to upsert paper portfolio '%s'", name)


def main() -> None:
    settings = load_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    _seed_portfolio_from_yaml(Path("config/bot.yaml"))
    config = load_config(settings)
    _seed_paper_portfolios(config)
    app = build_application(settings, config)
    app.run_polling()


if __name__ == "__main__":
    main()

