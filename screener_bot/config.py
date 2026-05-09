from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


Market = Literal["india", "us"]


class TelegramConfig(BaseModel):
    allowed_chat_ids: list[int] = Field(default_factory=list)


class PortfolioItem(BaseModel):
    symbol: str
    market: Market
    avg_price: float | None = None
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


class BotConfig(BaseModel):
    timezone: str = "Asia/Kolkata"
    telegram: TelegramConfig
    portfolio: list[PortfolioItem]
    rulesets: dict[str, Ruleset]
    technical_snapshot: TechnicalSnapshotConfig = Field(
        default_factory=TechnicalSnapshotConfig
    )

    @field_validator("portfolio")
    @classmethod
    def portfolio_not_empty(cls, value: list[PortfolioItem]) -> list[PortfolioItem]:
        if not value:
            raise ValueError("portfolio must contain at least one item")
        return value


class EnvSettings(BaseSettings):
    telegram_bot_token: str | None = None
    bot_config_path: str = "config/bot.yaml"
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


def load_config(path: str | Path | None = None) -> BotConfig:
    load_dotenv()
    resolved = Path(path or os.environ.get("BOT_CONFIG_PATH", "config/bot.yaml"))
    with resolved.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    return BotConfig.model_validate(raw)


def load_settings() -> EnvSettings:
    load_dotenv()
    return EnvSettings()
