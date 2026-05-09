from __future__ import annotations

import asyncio
import sys

from screener_bot.config import BotConfig
from screener_bot.formatting import split_messages
from screener_bot.scheduled_screener import ScheduledScreenerService


def test_unauthorized_chat_logic() -> None:
    from screener_bot.bot import _authorized

    class Chat:
        id = 2

    class Update:
        effective_chat = Chat()

    config = BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "x"}],
            "rulesets": {"x": {}},
        }
    )
    assert _authorized(config, Update()) is False


def test_long_output_is_split() -> None:
    messages = split_messages("x\n" * 5000, limit=100)
    assert len(messages) > 1
    assert all(len(message) <= 100 for message in messages)


def test_pre_blocks_are_split_with_balanced_tags() -> None:
    text = (
        "<b>Report</b>\n<pre>"
        + "\n".join(f"SYM{i}  10  20" for i in range(30))
        + "</pre>"
    )

    messages = split_messages(text, limit=100)

    assert len(messages) > 1
    assert all(len(message) <= 100 for message in messages)
    assert all(
        message.count("<pre>") == message.count("</pre>") for message in messages
    )


def test_run_all_command_is_registered() -> None:
    from screener_bot.bot import BOT_COMMANDS, HELP_TEXT

    assert "/run_all" in HELP_TEXT
    assert "run_all" in {command.command for command in BOT_COMMANDS}


def test_schedules_configured_screener_times() -> None:
    from screener_bot.bot import _schedule_screener_jobs

    config = BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "x"}],
            "rulesets": {"x": {}},
            "scheduled_screener": {
                "enabled": True,
                "times": ["16:00", "02:30"],
                "commands": [{"label": "test", "command": ["true"]}],
            },
        }
    )

    class JobQueue:
        def __init__(self) -> None:
            self.calls = []

        def run_daily(self, callback, *, time, data, name):
            self.calls.append((callback, time, data, name))

    class App:
        def __init__(self) -> None:
            self.job_queue = JobQueue()

    app = App()
    service = ScheduledScreenerService(config)
    _schedule_screener_jobs(app, config, service)

    assert [call[1].strftime("%H:%M") for call in app.job_queue.calls] == [
        "16:00",
        "02:30",
    ]
    assert [call[3] for call in app.job_queue.calls] == [
        "scheduled-screener-16:00",
        "scheduled-screener-02:30",
    ]


def test_scheduled_screener_service_runs_command() -> None:
    config = BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "x"}],
            "rulesets": {"x": {}},
            "scheduled_screener": {
                "enabled": True,
                "times": ["16:00"],
                "working_directory": ".",
                "commands": [
                    {
                        "label": "Smoke",
                        "command": [sys.executable, "-c", "print('ok')"],
                    }
                ],
            },
        }
    )

    report = asyncio.run(ScheduledScreenerService(config).run(full_list=True))

    assert "<b>Smoke</b> (ok)" in report
    assert "ok" in report


def test_scheduled_screener_formats_csv_output() -> None:
    config = BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "x"}],
            "rulesets": {"x": {}},
            "scheduled_screener": {
                "commands": [
                    {
                        "label": "India EMA",
                        "command": [
                            sys.executable,
                            "-c",
                            (
                                "print('name,close,change,setup_score');"
                                "print('ATHERENERG,915.05,0.87,80.39')"
                            ),
                        ],
                    }
                ],
            },
        }
    )

    report = asyncio.run(ScheduledScreenerService(config).run(full_list=True))

    assert "┏" not in report
    assert "<pre>Symbol" in report
    assert "ATHERENERG" in report
    assert "+0.87%" in report


def test_scheduled_screener_parses_csv_after_progress_lines() -> None:
    config = BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "x"}],
            "rulesets": {"x": {}},
            "scheduled_screener": {
                "commands": [
                    {
                        "label": "India Promoter Holding Change",
                        "command": [
                            sys.executable,
                            "-c",
                            (
                                "print('Universe: 192 liquid tickers');"
                                "print('Enriching...');"
                                "print('name,promoter_pct_latest,promoter_change,fii_pct_latest,dii_pct_latest');"
                                "print('IDEA,25.64,0.07,6.19,5.56')"
                            ),
                        ],
                    }
                ],
            },
        }
    )

    report = asyncio.run(
        ScheduledScreenerService(config).run("india promoter", full_list=True)
    )

    assert "Universe: 192" not in report
    assert "promoter_pct_latest" not in report
    assert "IDEA" in report
    assert "+0.07" in report


def test_specific_screener_query_shows_all_rows() -> None:
    rows = [
        "name,close,change,setup_score",
        *[f"SYM{i},10,{i},50" for i in range(13)],
    ]
    config = BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "x"}],
            "rulesets": {"x": {}},
            "scheduled_screener": {
                "commands": [
                    {
                        "label": "India EMA",
                        "command": [sys.executable, "-c", "print('\\n'.join(%r))" % rows],
                    },
                    {
                        "label": "US EMA",
                        "command": [sys.executable, "-c", "print('should not run')"],
                    },
                ],
            },
        }
    )

    report = asyncio.run(
        ScheduledScreenerService(config).run("india ema", full_list=True)
    )

    assert "SYM12" in report
    assert "+1 more rows" not in report
    assert "US EMA" not in report


def test_delta_report_first_run_shows_all_rows_as_added(tmp_path) -> None:
    config = BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "x"}],
            "rulesets": {"x": {}},
            "scheduled_screener": {
                "commands": [
                    {
                        "label": "India EMA",
                        "command": [
                            sys.executable,
                            "-c",
                            (
                                "print('name,close,change,setup_score');"
                                "print('AAA,10,1,50');"
                                "print('BBB,20,2,60')"
                            ),
                        ],
                    }
                ],
            },
        }
    )

    service = ScheduledScreenerService(config, tmp_path / "snapshots.json")
    report = asyncio.run(service.run())

    assert "<b>Screener Changes</b>" in report
    assert "<b>Added:</b> AAA, BBB" in report
    assert "<b>Removed:</b>" not in report


def test_delta_report_shows_added_and_removed_only(tmp_path) -> None:
    snapshot_path = tmp_path / "snapshots.json"
    first_config = BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "x"}],
            "rulesets": {"x": {}},
            "scheduled_screener": {
                "commands": [
                    {
                        "label": "India EMA",
                        "command": [
                            sys.executable,
                            "-c",
                            (
                                "print('name,close,change,setup_score');"
                                "print('AAA,10,1,50');"
                                "print('BBB,20,2,60')"
                            ),
                        ],
                    }
                ],
            },
        }
    )
    second_config = BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "x"}],
            "rulesets": {"x": {}},
            "scheduled_screener": {
                "commands": [
                    {
                        "label": "India EMA",
                        "command": [
                            sys.executable,
                            "-c",
                            (
                                "print('name,close,change,setup_score');"
                                "print('AAA,11,3,70');"
                                "print('CCC,30,4,80')"
                            ),
                        ],
                    }
                ],
            },
        }
    )

    asyncio.run(ScheduledScreenerService(first_config, snapshot_path).run())
    report = asyncio.run(ScheduledScreenerService(second_config, snapshot_path).run())

    assert "<b>Added:</b> CCC" in report
    assert "<b>Removed:</b> BBB" in report
    assert "AAA,11" not in report
