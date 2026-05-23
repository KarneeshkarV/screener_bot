from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

from screener_bot.config import BotConfig, ScreenerCommandConfig
from screener_bot.scheduled_screener import (
    ScheduledScreenerService,
    _clip,
    _extra_count,
    _extract_csv_text,
    _filter_stderr,
    _fmt_float,
    _fmt_fraction_pct,
    _fmt_int,
    _fmt_pct,
    _fmt_shares,
    _fmt_signed,
    _format_delta_rows,
    _format_output,
    _matching_commands,
    _parse_csv_rows,
    _row_identity,
    _to_float,
    _truncate,
    send_screener_report,
)


def _cfg(
    *,
    commands: list[dict],
    chat_ids: list[int] | None = None,
    timeout_seconds: int = 300,
    working_directory: str = ".",
) -> BotConfig:
    scheduled: dict = {
        "commands": commands,
        "timeout_seconds": timeout_seconds,
        "working_directory": working_directory,
    }
    if chat_ids is not None:
        scheduled["chat_ids"] = chat_ids
    return BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "x"}],
            "rulesets": {"x": {}},
            "scheduled_screener": scheduled,
        }
    )


def _py(code: str) -> list[str]:
    return [sys.executable, "-c", code]


def run(coro):
    return asyncio.run(coro)


# --- pure helpers ----------------------------------------------------------


def test_truncate() -> None:
    assert _truncate("abc", 10) == "abc"
    out = _truncate("x" * 700, 650)
    assert "output truncated" in out
    assert len(out) <= 650


def test_matching_commands() -> None:
    commands = [
        ScreenerCommandConfig(label="India EMA", command=["a"]),
        ScreenerCommandConfig(label="US Holding", command=["b"]),
    ]
    assert _matching_commands(commands, None) == commands
    assert _matching_commands(commands, "   ") == commands
    assert [c.label for c in _matching_commands(commands, "india")] == ["India EMA"]
    assert [c.label for c in _matching_commands(commands, "india ema")] == ["India EMA"]
    assert _matching_commands(commands, "zzz") == []


def test_to_float_and_number_formatters() -> None:
    assert _to_float(None) is None
    assert _to_float("") is None
    assert _to_float("nan") is None
    assert _to_float("abc") is None
    assert _to_float("3.5") == 3.5

    assert _fmt_float(None, 2) == "-"
    assert _fmt_float("3.14159", 2) == "3.14"
    assert _fmt_signed(None, 2) == "-"
    assert _fmt_signed("1.5", 2) == "+1.50"
    assert _fmt_signed("-1.5", 2) == "-1.50"
    assert _fmt_pct(None) == "-"
    assert _fmt_pct("2.5") == "+2.50%"
    assert _fmt_fraction_pct(None) == "-"
    assert _fmt_fraction_pct("0.5") == "+50.00%"
    assert _fmt_int(None) == "-"
    assert _fmt_int("3.9") == "3"
    assert _fmt_shares(None) == "-"
    assert _fmt_shares("1500000") == "+1.5M"
    assert _fmt_shares("1500") == "+1.5K"
    assert _fmt_shares("-500") == "-500"


def test_clip() -> None:
    assert _clip("short", 10) == "short"
    assert _clip("abcdefghijk", 5) == "abcd…"


def test_row_identity() -> None:
    assert _row_identity({"name": "AAA"}) == "AAA"
    assert _row_identity({"ticker": "BBB"}) == "BBB"
    assert _row_identity({"symbol": "CCC"}) == "CCC"
    assert _row_identity({"other": "x"}) is None
    assert _row_identity({"name": "  "}) is None


def test_format_delta_rows() -> None:
    assert _format_delta_rows([], []) == ["<i>No changes since last run.</i>"]
    out = _format_delta_rows(["AAA"], ["BBB"])
    assert any("Added" in line for line in out)
    assert any("Removed" in line for line in out)


def test_extra_count() -> None:
    rows = [{"a": "1"} for _ in range(15)]
    assert _extra_count(rows, 10) == ["<i>+5 more rows</i>"]
    assert _extra_count(rows, 20) == []


def test_extract_csv_text_and_parse() -> None:
    text = "junk line\nname,close,change\nAAA,1,2"
    assert "name,close" in _extract_csv_text(text)
    assert _parse_csv_rows(text) == [{"name": "AAA", "close": "1", "change": "2"}]
    assert _parse_csv_rows("no csv here") == []
    assert _parse_csv_rows("ticker,close\nX,5")[0]["ticker"] == "X"


def test_parse_csv_rows_handles_csv_error() -> None:
    import csv

    previous = csv.field_size_limit()
    csv.field_size_limit(1000)
    try:
        # A field larger than the limit makes csv raise mid-iteration.
        assert _parse_csv_rows("name,close\n" + "x" * 5000 + ",1") == []
    finally:
        csv.field_size_limit(previous)


def test_filter_stderr_removes_noise() -> None:
    out = _filter_stderr(
        "Universe: 100 tickers\nBytecode compiled\nReal error here", success=False
    )
    assert "Universe" not in out
    assert "Bytecode" not in out
    assert "Real error here" in out


def test_filter_stderr_success_drops_quote_errors() -> None:
    out = _filter_stderr("HTTP Error 404\nQuote not found\nkeep this", success=True)
    assert "404" not in out
    assert "Quote not found" not in out
    assert "keep this" in out


def test_filter_stderr_collapses_network_failures() -> None:
    err = "\n".join(
        ["Network is unreachable"] * 3 + ["failed for company page X", "ok line"]
    )
    out = _filter_stderr(err, success=False)
    assert "network failures" in out
    assert "ok line" in out
    assert "Network is unreachable" not in out


# --- _format_output dispatch ----------------------------------------------


def test_format_output_empty_universe() -> None:
    out = _format_output("India EMA", "garp_score")
    assert "No rows returned" in out[0]


def test_format_output_raw_dump_for_non_csv() -> None:
    out = _format_output("Misc", "line one\nline two\nline three\n" + "x" * 100)
    assert out[0].startswith("<pre>")


def test_format_output_ema_table() -> None:
    out = _format_output(
        "India EMA", "name,close,change,setup_score\nAAA,915.05,0.87,80.39"
    )
    assert out[0].startswith("<pre>Symbol")
    assert any("AAA" in line for line in out)
    assert any("+0.87%" in line for line in out)


def test_format_output_promoter_holding_table() -> None:
    csv = (
        "name,promoter_pct_latest,promoter_change,fii_pct_latest,dii_pct_latest\n"
        "IDEA,25.64,0.07,6.19,5.56"
    )
    out = _format_output("India Promoter Holding", csv)
    assert "Prom%" in out[0]
    assert any("IDEA" in line for line in out)


def test_format_output_insider_holding_table() -> None:
    csv = (
        "name,yf_net_shares_6m,yf_net_pct_6m,yf_buy_trans_6m,yf_sell_trans_6m\n"
        "AAPL,1500000,0.05,3,1"
    )
    out = _format_output("US Insider", csv)
    assert "Net Shrs" in out[0]
    assert any("AAPL" in line for line in out)


def test_format_output_generic_table() -> None:
    out = _format_output("Random Screen", "alpha,beta,gamma,delta,epsilon\n1,2,3,4,5")
    assert out[0].startswith("<pre>")
    assert any("1" in line for line in out)


# --- service: run() paths --------------------------------------------------


def test_chat_ids_prefers_configured_then_falls_back() -> None:
    assert ScheduledScreenerService(_cfg(commands=[], chat_ids=[7])).chat_ids() == [7]
    assert ScheduledScreenerService(_cfg(commands=[])).chat_ids() == [1]


def test_run_no_commands_configured() -> None:
    report = run(ScheduledScreenerService(_cfg(commands=[])).run())
    assert "No screener commands configured" in report


def test_run_no_command_matched_query() -> None:
    config = _cfg(commands=[{"label": "India EMA", "command": ["true"]}])
    report = run(ScheduledScreenerService(config).run("zzz"))
    assert "No screener command matched" in report


def test_run_command_timeout() -> None:
    config = _cfg(
        timeout_seconds=1,
        commands=[{"label": "Slow", "command": _py("import time; time.sleep(5)")}],
    )
    report = run(ScheduledScreenerService(config).run(full_list=True))
    assert "timed out" in report


def test_run_command_subprocess_error() -> None:
    config = _cfg(
        commands=[{"label": "Bad", "command": ["definitely_not_a_real_binary_xyz"]}]
    )
    report = run(ScheduledScreenerService(config).run(full_list=True))
    assert "<b>Bad</b>" in report


def test_run_command_no_output() -> None:
    config = _cfg(commands=[{"label": "Quiet", "command": _py("pass")}])
    report = run(ScheduledScreenerService(config).run(full_list=True))
    assert "No output." in report


def test_run_command_missing_working_directory(tmp_path) -> None:
    config = _cfg(
        commands=[{"label": "X", "command": _py("print('name,close\\nAAA,1')")}],
        working_directory=str(tmp_path / "does-not-exist"),
    )
    report = run(ScheduledScreenerService(config).run(full_list=True))
    assert "<b>X</b>" in report


# --- service: delta report paths -------------------------------------------


def test_delta_report_non_csv_falls_back_to_output(tmp_path) -> None:
    config = _cfg(
        commands=[
            {
                "label": "Weird",
                "command": _py("print('hello world one\\nsecond line here')"),
            }
        ]
    )
    report = run(ScheduledScreenerService(config, tmp_path / "s.json").run())
    assert "Weird" in report
    assert "hello world one" in report


def test_delta_report_error_branch(tmp_path) -> None:
    config = _cfg(
        commands=[
            {
                "label": "Err",
                "command": _py(
                    "import sys; sys.stderr.write('boom error'); sys.exit(2)"
                ),
            }
        ]
    )
    report = run(ScheduledScreenerService(config, tmp_path / "s.json").run())
    assert "exit 2" in report
    assert "boom error" in report


def test_delta_report_no_output_branch(tmp_path) -> None:
    config = _cfg(
        commands=[{"label": "Empty", "command": _py("import sys; sys.exit(3)")}]
    )
    report = run(ScheduledScreenerService(config, tmp_path / "s.json").run())
    assert "No output." in report


def test_load_snapshots_ignores_non_dict(tmp_path) -> None:
    path = tmp_path / "s.json"
    path.write_text("[1, 2, 3]")
    config = _cfg(
        commands=[
            {
                "label": "India EMA",
                "command": _py(
                    "print('name,close,change,setup_score');print('AAA,1,1,1')"
                ),
            }
        ]
    )
    report = run(ScheduledScreenerService(config, path).run())
    assert "Added:" in report


def test_save_snapshots_oserror_is_swallowed(tmp_path) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    path = blocker / "s.json"  # parent is a file
    config = _cfg(
        commands=[
            {
                "label": "India EMA",
                "command": _py(
                    "print('name,close,change,setup_score');print('AAA,1,1,1')"
                ),
            }
        ]
    )
    report = run(ScheduledScreenerService(config, path).run())
    assert "Added:" in report  # produced despite snapshot persistence failing


# --- send_screener_report --------------------------------------------------


def test_send_screener_report_uses_service_chat_ids(tmp_path) -> None:
    config = _cfg(
        commands=[
            {
                "label": "India EMA",
                "command": _py(
                    "print('name,close,change,setup_score');print('AAA,1,1,1')"
                ),
            }
        ],
        chat_ids=[5, 6],
    )
    service = ScheduledScreenerService(config, tmp_path / "s.json")
    ctx = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
    run(send_screener_report(ctx, service))
    chats = {call.kwargs["chat_id"] for call in ctx.bot.send_message.await_args_list}
    assert chats == {5, 6}


def test_send_screener_report_explicit_chat_ids(tmp_path) -> None:
    config = _cfg(commands=[{"label": "X", "command": _py("pass")}])
    service = ScheduledScreenerService(config, tmp_path / "s.json")
    ctx = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))
    run(send_screener_report(ctx, service, chat_ids=[42]))
    assert ctx.bot.send_message.await_args_list[0].kwargs["chat_id"] == 42
