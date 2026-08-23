import importlib
import logging

def test_observability_module_imports():
    obs = importlib.import_module("observability")
    assert callable(obs.configure_logging)
    assert callable(obs.init_sentry)
    assert callable(obs.capture_exception)

def test_log_level_is_applied(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    import observability
    observability.configure_logging()
    assert logging.getLogger().level == logging.DEBUG

def test_sentry_is_optional(monkeypatch):
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    import observability
    assert observability.init_sentry() is False

def test_secret_redaction():
    from observability import SecretRedactionFilter
    record = logging.LogRecord("test", logging.INFO, "", 0,
                               "api_key=supersecret password=hunter2", (), None)
    SecretRedactionFilter().filter(record)
    rendered = record.getMessage()
    assert "supersecret" not in rendered
    assert "hunter2" not in rendered
    assert "[REDACTED]" in rendered
