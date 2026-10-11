"""Inbox auto-refresh: the page polls itself every 10s (360/hour), so it must be
exempt from the default 300/hour per-IP limit or staff get locked out."""
import os
import subprocess
import sys

from tests.unit.test_settings_permission_boundaries import client_with_roles  # noqa: F401  (fixture)

SCRIPT = r'''
import phanta_app
from tests.unit.test_dashboard_actions import _register_and_onboard
app = phanta_app.app
app.config["TESTING"] = True
c = app.test_client()
import uuid; _register_and_onboard(c, "rl" + uuid.uuid4().hex[:8])
inbox = {c.get("/dashboard/inbox").status_code for _ in range(6)}
other = [c.get("/customers").status_code for _ in range(6)]
print("INBOX", sorted(inbox), "OTHER", other)
assert inbox == {200}, inbox
assert 429 in other, other  # proves the limiter is really on in this process
'''


def test_inbox_polling_is_not_rate_limited(tmp_path):
    env = dict(os.environ, RATELIMIT_ENABLED="true", DEFAULT_RATE_LIMIT="3 per hour")
    result = subprocess.run([sys.executable, "-c", SCRIPT], env=env, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-2000:]


def test_inbox_page_has_auto_refresh(client_with_roles):
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    assert "setInterval(refresh, 10000)" in ctx["client"].get("/dashboard/inbox").get_data(as_text=True)
