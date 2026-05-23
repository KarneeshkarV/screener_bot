from __future__ import annotations

from screener_bot.alerts import AlertService
from screener_bot.config import BotConfig, PortfolioItem
from screener_bot.technical import RuleStatus, TechnicalStatus


def _config(**alerts) -> BotConfig:
    return BotConfig.model_validate(
        {
            "telegram": {"allowed_chat_ids": [1]},
            "portfolio": [{"symbol": "AAPL", "market": "us", "ruleset": "x"}],
            "rulesets": {"x": {}},
            "alerts": alerts or {"enabled": True},
        }
    )


class _StubTechnical:
    def __init__(self, statuses: list[TechnicalStatus]) -> None:
        self._statuses = statuses

    def check_portfolio(self) -> list[TechnicalStatus]:
        return self._statuses


def _status(
    *,
    close: float,
    exit_matched: bool,
    entry_matched: bool = True,
    high: float = 200.0,
    low: float = 50.0,
    volume: float = 1000.0,
    avg_volume: float = 1000.0,
) -> TechnicalStatus:
    item = PortfolioItem(symbol="AAPL", market="us", ruleset="x")
    status = TechnicalStatus(item=item, ticker="AAPL")
    status.close = close
    status.daily_change_pct = 1.0
    status.entry = RuleStatus(entry_matched)
    status.exit = RuleStatus(exit_matched)
    status.high_52w = high
    status.low_52w = low
    status.last_volume = volume
    status.avg_volume_20 = avg_volume
    return status


def test_first_run_is_silent_baseline(tmp_path) -> None:
    service = AlertService(
        _config(),
        _StubTechnical([_status(close=100.0, exit_matched=False)]),
        state_path=tmp_path / "state.json",
    )
    assert service.evaluate() is None


def test_no_change_produces_no_report(tmp_path) -> None:
    path = tmp_path / "state.json"
    status = _status(close=100.0, exit_matched=False)
    AlertService(_config(), _StubTechnical([status]), state_path=path).evaluate()
    report = AlertService(_config(), _StubTechnical([status]), state_path=path).evaluate()
    assert report is None


def test_exit_flip_volume_spike_and_near_high(tmp_path) -> None:
    path = tmp_path / "state.json"
    AlertService(
        _config(),
        _StubTechnical([_status(close=100.0, exit_matched=False)]),
        state_path=path,
    ).evaluate()

    report = AlertService(
        _config(),
        _StubTechnical([_status(close=180.0, exit_matched=True, volume=3000.0)]),
        state_path=path,
    ).evaluate()

    assert report is not None
    assert "Exit signal triggered" in report
    assert "Within 15% of 52-week high" in report
    assert "Volume spike" in report


def test_new_52_week_high(tmp_path) -> None:
    path = tmp_path / "state.json"
    AlertService(
        _config(),
        _StubTechnical([_status(close=180.0, exit_matched=False)]),
        state_path=path,
    ).evaluate()

    report = AlertService(
        _config(),
        _StubTechnical([_status(close=205.0, exit_matched=False, high=205.0)]),
        state_path=path,
    ).evaluate()

    assert report is not None
    assert "New 52-week high" in report


def test_data_gap_does_not_clear_baseline(tmp_path) -> None:
    path = tmp_path / "state.json"
    AlertService(
        _config(),
        _StubTechnical([_status(close=100.0, exit_matched=False)]),
        state_path=path,
    ).evaluate()

    # A run with no price data must not emit a spurious "cleared" alert.
    error_status = TechnicalStatus(
        item=PortfolioItem(symbol="AAPL", market="us", ruleset="x"),
        ticker="AAPL",
        error="No price data available",
    )
    report = AlertService(
        _config(), _StubTechnical([error_status]), state_path=path
    ).evaluate()
    assert report is None

    # After data returns with a real change, alerting resumes from the baseline.
    resumed = AlertService(
        _config(),
        _StubTechnical([_status(close=100.0, exit_matched=True)]),
        state_path=path,
    ).evaluate()
    assert resumed is not None
    assert "Exit signal triggered" in resumed


def test_chat_ids_default_to_allowed(tmp_path) -> None:
    service = AlertService(_config(), _StubTechnical([]), state_path=tmp_path / "s.json")
    assert service.chat_ids() == [1]


def test_alerts_config_defaults() -> None:
    config = _config()
    assert config.alerts.enabled is True
    assert config.alerts.interval_minutes == 60
    assert config.alerts.near_high_pct == 15.0
    assert config.alerts.volume_spike_multiple == 2.0
