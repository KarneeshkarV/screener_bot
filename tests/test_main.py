from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock


def test_main_loads_config_and_runs_polling(monkeypatch) -> None:
    from screener_bot import __main__ as m

    settings = SimpleNamespace(log_level="DEBUG")
    captured: dict = {}

    def fake_load_config(s):
        captured["settings"] = s
        return "CONFIG"

    monkeypatch.setattr(m, "load_settings", lambda: settings)
    monkeypatch.setattr(m, "load_config", fake_load_config)
    monkeypatch.setattr(m, "_seed_portfolio_from_yaml", lambda _path: None)
    monkeypatch.setattr(m, "_seed_paper_portfolios", lambda _cfg: None)

    app = MagicMock()

    def fake_build(s, c):
        captured["build"] = (s, c)
        return app

    monkeypatch.setattr(m, "build_application", fake_build)

    m.main()

    assert captured["settings"] is settings
    assert captured["build"] == (settings, "CONFIG")
    app.run_polling.assert_called_once_with()


def test_main_falls_back_to_info_for_bad_log_level(monkeypatch) -> None:
    from screener_bot import __main__ as m

    settings = SimpleNamespace(log_level="not-a-level")
    monkeypatch.setattr(m, "load_settings", lambda: settings)
    monkeypatch.setattr(m, "load_config", lambda s: "CONFIG")
    monkeypatch.setattr(m, "_seed_portfolio_from_yaml", lambda _path: None)
    monkeypatch.setattr(m, "_seed_paper_portfolios", lambda _cfg: None)
    app = MagicMock()
    monkeypatch.setattr(m, "build_application", lambda s, c: app)

    m.main()  # must not raise on an unrecognised log level

    app.run_polling.assert_called_once_with()
