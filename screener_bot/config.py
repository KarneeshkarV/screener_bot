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
