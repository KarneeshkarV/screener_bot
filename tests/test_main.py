from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock


def test_main_loads_config_and_runs_polling(monkeypatch) -> None:
    from screener_bot import __main__ as m

    settings = SimpleNamespace(log_level="DEBUG", bot_config_path="cfg.yaml")
    captured: dict = {}

    def fake_load_config(path):
        captured["config_path"] = path
        return "CONFIG"

    monkeypatch.setattr(m, "load_settings", lambda: settings)
    monkeypatch.setattr(m, "load_config", fake_load_config)

    app = MagicMock()

    def fake_build(s, c):
        captured["build"] = (s, c)
        return app

    monkeypatch.setattr(m, "build_application", fake_build)

    m.main()

    assert captured["config_path"] == "cfg.yaml"
    assert captured["build"] == (settings, "CONFIG")
    app.run_polling.assert_called_once_with()


def test_main_falls_back_to_info_for_bad_log_level(monkeypatch) -> None:
    from screener_bot import __main__ as m

    settings = SimpleNamespace(log_level="not-a-level", bot_config_path="cfg.yaml")
    monkeypatch.setattr(m, "load_settings", lambda: settings)
    monkeypatch.setattr(m, "load_config", lambda path: "CONFIG")
    app = MagicMock()
    monkeypatch.setattr(m, "build_application", lambda s, c: app)

    m.main()  # must not raise on an unrecognised log level

    app.run_polling.assert_called_once_with()
