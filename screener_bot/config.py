from __future__ import annotations

from typing import Literal

from pathlib import Path

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from . import portfolio_store


Market = Literal["india", "us"]


class TelegramConfig(BaseModel):
    allowed_chat_ids: list[int] = Field(default_factory=list)
    # Optional chat that receives short error notes when a scheduled job fails.
    admin_chat_id: int | None = None


class PortfolioItem(BaseModel):
    symbol: str
    market: Market
    avg_price: float | None = None
    stop_loss: float | None = None
    ruleset: str

    @field_validator("symbol", "ruleset")
    @classmethod
    def non_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value


class RuleExpression(BaseModel):
    expression: str


class RuleGroup(BaseModel):
    all: list[RuleExpression] | None = None
    any: list[RuleExpression] | None = None


class Ruleset(BaseModel):
    entry: RuleGroup = Field(default_factory=RuleGroup)
    exit: RuleGroup = Field(default_factory=RuleGroup)


class SnapshotExpression(BaseModel):
    label: str
    expression: str


class TechnicalSnapshotConfig(BaseModel):
    expressions: list[SnapshotExpression] = Field(default_factory=list)


class ScreenerCommandConfig(BaseModel):
    label: str
    command: list[str]

    @field_validator("label")
    @classmethod
    def label_not_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value

    @field_validator("command")
    @classmethod
    def command_not_empty(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("must contain at least one argument")
        return value


class ScheduledScreenerConfig(BaseModel):
    enabled: bool = False
    times: list[str] = Field(default_factory=list)
    working_directory: str = "/home/karneeshkar/Desktop/personal/screener"
    chat_ids: list[int] = Field(default_factory=list)
    timeout_seconds: int = 300
    commands: list[ScreenerCommandConfig] = Field(default_factory=list)

    @field_validator("times")
    @classmethod
    def valid_times(cls, value: list[str]) -> list[str]:
        for item in value:
            parts = item.split(":")
            if len(parts) != 2:
                raise ValueError("times must use HH:MM format")
            hour, minute = (int(part) for part in parts)
            if not 0 <= hour <= 23 or not 0 <= minute <= 59:
                raise ValueError("times must use HH:MM format")
        return value

    @field_validator("timeout_seconds")
    @classmethod
    def positive_timeout(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("must be positive")
        return value


class AlertsConfig(BaseModel):
    enabled: bool = True
    interval_minutes: int = 60
    near_high_pct: float = 15.0
    near_stop_pct: float = 3.0
    volume_spike_multiple: float = 2.0
    chat_ids: list[int] = Field(default_factory=list)

    @field_validator("interval_minutes")
    @classmethod
    def positive_interval(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("must be positive")
        return value

    @field_validator("near_high_pct", "near_stop_pct")
    @classmethod
    def valid_near_pct(cls, value: float) -> float:
        if not 0 < value < 100:
            raise ValueError("must be between 0 and 100")
        return value

    @field_validator("volume_spike_multiple")
    @classmethod
    def positive_multiple(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("must be positive")
        return value


class PaperPortfolioConfig(BaseModel):
    """Configuration for a single named paper portfolio."""

    enabled: bool = True
    market: Market = "india"
    strategy: str = "rs_breakout"
    initial_capital: float = 1_000_000
    slots: int = 5
    stop_loss_pct: float | None = None
    take_profit_pct: float | None = None
    trailing_stop_pct: float | None = None
    slippage_bps: float = 10
    tickers: str | None = None

    @field_validator("strategy")
    @classmethod
    def strategy_not_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value

    @field_validator("slots")
    @classmethod
    def positive_slots(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("must be positive")
        return value


class PaperTradingConfig(BaseModel):
    """Top-level paper trading configuration."""

    portfolios: dict[str, PaperPortfolioConfig] = Field(default_factory=dict)


class BotConfig(BaseModel):
    timezone: str = "Asia/Kolkata"
    telegram: TelegramConfig
    portfolio: list[PortfolioItem]
    rulesets: dict[str, Ruleset]
    technical_snapshot: TechnicalSnapshotConfig = Field(
        default_factory=TechnicalSnapshotConfig
    )
    scheduled_screener: ScheduledScreenerConfig = Field(
        default_factory=ScheduledScreenerConfig
    )
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)
    paper_trading: PaperTradingConfig = Field(default_factory=PaperTradingConfig)

    @field_validator("portfolio")
    @classmethod
    def portfolio_not_empty(cls, value: list[PortfolioItem]) -> list[PortfolioItem]:
        if not value:
            raise ValueError("portfolio must contain at least one item")
        return value


class EnvSettings(BaseSettings):
    telegram_bot_token: str | None = None
    telegram_allowed_chat_ids: str = ""
    telegram_admin_chat_id: int | None = None
    turso_database_url: str | None = None
    turso_auth_token: str | None = None
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    def chat_ids(self) -> list[int]:
        return [
            int(part)
            for part in (
                chunk.strip() for chunk in self.telegram_allowed_chat_ids.split(",")
            )
            if part
        ]


DEFAULT_TIMEZONE = "Asia/Kolkata"

DEFAULT_SCHEDULED_SCREENER = ScheduledScreenerConfig(
    enabled=True,
    times=["16:00", "02:30"],
    working_directory=".",
    timeout_seconds=300,
    commands=[
        ScreenerCommandConfig(
            label="India EMA",
            command=[
                "uv",
                "run",
                "screener",
                "screen",
                "-m",
                "india",
                "-c",
                "ema",
                "-n",
                "30",
                "--csv",
            ],
        ),
        ScreenerCommandConfig(
            label="US EMA",
            command=[
                "uv",
                "run",
                "screener",
                "screen",
                "-m",
                "us",
                "-c",
                "ema",
                "-n",
                "30",
                "--csv",
            ],
        ),
        ScreenerCommandConfig(
            label="India GARP",
            command=[
                "uv",
                "run",
                "screener",
                "garp",
                "-m",
                "india",
                "-n",
                "30",
                "--csv",
            ],
        ),
        ScreenerCommandConfig(
            label="US GARP",
            command=["uv", "run", "screener", "garp", "-m", "us", "-n", "30", "--csv"],
        ),
        ScreenerCommandConfig(
            label="India Promoter Holding Change",
            command=[
                "uv",
                "run",
                "screener",
                "promoter-buys",
                "-m",
                "india",
                "--min-change",
                "0",
                "-n",
                "30",
                "--csv",
            ],
        ),
        ScreenerCommandConfig(
            label="US Insider Holding Change",
            command=[
                "uv",
                "run",
                "screener",
                "promoter-buys",
                "-m",
                "us",
                "-n",
                "30",
                "--csv",
            ],
        ),
    ],
)

DEFAULT_ALERTS = AlertsConfig(
    enabled=True,
    interval_minutes=60,
    near_high_pct=15.0,
    near_stop_pct=3.0,
    volume_spike_multiple=2.0,
)

DEFAULT_RULESETS: dict[str, Ruleset] = {
    "swing_momentum": Ruleset(
        entry=RuleGroup(
            all=[
                RuleExpression(expression="rsi(close, 14) > 55"),
                RuleExpression(expression="close > ema(close, 20)"),
            ]
        ),
        exit=RuleGroup(
            any=[
                RuleExpression(expression="rsi(close, 14) < 45"),
                RuleExpression(expression="close < ema(close, 20)"),
            ]
        ),
    ),
}

DEFAULT_TECHNICAL_SNAPSHOT = TechnicalSnapshotConfig(
    expressions=[
        SnapshotExpression(label="RSI 14", expression="rsi(close, 14)"),
        SnapshotExpression(label="Above EMA20", expression="close > ema(close, 20)"),
        SnapshotExpression(label="Above EMA50", expression="close > ema(close, 50)"),
    ]
)


def _fetch_portfolio_items() -> list[PortfolioItem]:
    client = portfolio_store.connect()
    if client is None:
        raise RuntimeError(
            "Turso is not configured. Set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN."
        )
    try:
        rows = portfolio_store.fetch_portfolio(client)
    finally:
        client.close()
    return [PortfolioItem.model_validate(row) for row in rows]


def load_paper_trading_config(
    yaml_path: Path = Path("config/paper_trading.yaml"),
) -> PaperTradingConfig:
    """Load paper trading portfolio definitions from YAML."""
    if not yaml_path.exists():
        return PaperTradingConfig()
    raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    portfolios_raw = raw.get("portfolios") or {}
    portfolios = {
        name: PaperPortfolioConfig.model_validate(cfg)
        for name, cfg in portfolios_raw.items()
    }
    return PaperTradingConfig(portfolios=portfolios)


def load_config(settings: EnvSettings | None = None) -> BotConfig:
    load_dotenv()
    settings = settings or load_settings()
    portfolio = _fetch_portfolio_items()
    paper_trading = load_paper_trading_config()
    return BotConfig(
        timezone=DEFAULT_TIMEZONE,
        telegram=TelegramConfig(
            allowed_chat_ids=settings.chat_ids(),
            admin_chat_id=settings.telegram_admin_chat_id,
        ),
        portfolio=portfolio,
        rulesets=DEFAULT_RULESETS,
        technical_snapshot=DEFAULT_TECHNICAL_SNAPSHOT,
        scheduled_screener=DEFAULT_SCHEDULED_SCREENER,
        alerts=DEFAULT_ALERTS,
        paper_trading=paper_trading,
    )


def load_settings() -> EnvSettings:
    load_dotenv()
    return EnvSettings()
