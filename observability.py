"""Production observability for PHANTA.

Optional Sentry integration plus safe, configurable application logging.
Sentry is enabled only when SENTRY_DSN is configured; local development does
not require it. Secrets and request payloads are deliberately not logged.
"""
from __future__ import annotations

import logging
import os
import re
from logging.config import dictConfig

_SECRET_PATTERNS = (
    re.compile(r"(?i)(password|passwd|secret|api[_-]?key|access[_-]?token|auth[_-]?token|authorization|private[_-]?key)\s*[=:]\s*[^\s,;]+"),
    re.compile(r"(?i)(bearer)\s+[A-Za-z0-9._~+/=-]+"),
)

class SecretRedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
            for pattern in _SECRET_PATTERNS:
                message = pattern.sub(r"\1=[REDACTED]" if pattern.groups else "[REDACTED]", message)
            record.msg = message
            record.args = ()
        except Exception:
            # Logging must never break application execution.
            record.msg = "[unavailable log message]"
            record.args = ()
        return True

def configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {"redact_secrets": {"()": SecretRedactionFilter}},
        "formatters": {
            "standard": {
                "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "filters": ["redact_secrets"],
                "stream": "ext://sys.stdout",
            }
        },
        "root": {"level": level, "handlers": ["console"]},
    })

def _scrub_event(event, hint):
    """Remove common secret-bearing fields before sending an event."""
    request = event.get("request") or {}
    headers = request.get("headers") or {}
    for key in list(headers):
        if str(key).lower() in {"authorization", "cookie", "x-api-key", "x-hub-signature-256"}:
            headers[key] = "[REDACTED]"
    request["headers"] = headers
    event["request"] = request
    # Never attach request bodies/forms to Sentry.
    request.pop("data", None)
    request.pop("cookies", None)
    return event

def init_sentry() -> bool:
    dsn = os.getenv("SENTRY_DSN", "").strip()
    if not dsn:
        return False
    import sentry_sdk
    from sentry_sdk.integrations.flask import FlaskIntegration
    sentry_sdk.init(
        dsn=dsn,
        integrations=[FlaskIntegration()],
        send_default_pii=False,
        before_send=_scrub_event,
        environment=os.getenv("RAILWAY_ENVIRONMENT_NAME") or os.getenv("FLASK_ENV") or "development",
    )
    return True

def capture_exception(exc: BaseException) -> None:
    if not os.getenv("SENTRY_DSN", "").strip():
        return
    try:
        import sentry_sdk
        sentry_sdk.capture_exception(exc)
    except Exception:
        logging.getLogger(__name__).exception("sentry_capture_failed")
