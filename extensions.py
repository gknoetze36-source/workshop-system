"""Shared Flask extension instances.

The limiter lives here rather than in phanta_app.py so route blueprints can
decorate their endpoints without importing phanta_app -- phanta_app imports
every blueprint at module level, so a blueprint importing phanta_app back
would be a circular import.

phanta_app.py is responsible for binding this instance to the application
(limiter.init_app(app)). Until that happens the decorators are inert, which
is exactly the bug this module was introduced to fix: the previous Limiter
instance was constructed but never bound to any app, so no limit it declared
was ever enforced.

NOTE ON STORAGE: the default storage is in-process memory. That is only
accurate with a single web worker (WEB_CONCURRENCY=1); with more workers each
process keeps its own counters and the effective limit is multiplied by the
worker count. Set RATELIMIT_STORAGE_URI to a shared backend before raising
WEB_CONCURRENCY. Per-account brute-force protection is handled separately
(failed-login lockout) and does not depend on this storage.
"""
from __future__ import annotations

import os

from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[os.getenv("DEFAULT_RATE_LIMIT", "300 per hour")],
    storage_uri=os.getenv("RATELIMIT_STORAGE_URI", "memory://"),
)
