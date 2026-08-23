"""HTTP smoke test for a running PHANTA instance.

Start PHANTA first, then run this script. It tests public startup, login,
platform/location routing, the PHANTA logo, and basic security responses.
"""
from __future__ import annotations

import os
import sys
import requests
import re

BASE = os.getenv("PHANTA_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
EMAIL = os.getenv("SUPERADMIN_USERNAME", "admin@phanta.local")
PASSWORD = os.getenv("SUPERADMIN_PASSWORD")


def check(name, response, expected):
    ok = response.status_code in expected
    print(f"{'PASS' if ok else 'FAIL'} {name}: {response.status_code}")
    return ok


def main():
    failures = 0
    s = requests.Session()
    try:
        r = s.get(BASE + "/", allow_redirects=False, timeout=10)
        failures += not check("GET /", r, {302, 303})
        login_page = s.get(BASE + "/login", timeout=10)
        failures += not check("GET /login", login_page, {200})
        r = s.get(BASE + "/static/images/phanta-logo.svg", timeout=10)
        failures += not check("GET PHANTA logo", r, {200})
        r = s.get(BASE + "/favicon.ico", timeout=10)
        failures += not check("GET favicon", r, {200})
        if PASSWORD:
            csrf_token = None
            match = re.search(r'name=["\']csrf_token["\']\s+value=["\']([^"\']+)', login_page.text, re.IGNORECASE)
            if match:
                csrf_token = match.group(1)
                print("PASS login CSRF token: found")
            else:
                print("FAIL login CSRF token: not found")
                failures += 1
            form = {"email": EMAIL, "password": PASSWORD}
            if csrf_token:
                form["csrf_token"] = csrf_token
            r = s.post(BASE + "/login", data=form, allow_redirects=False, timeout=10)
            failures += not check("POST /login", r, {302, 303})
            if r.status_code in {302, 303}:
                target = r.headers.get("Location", "/")
                r2 = s.get(BASE + target, timeout=10)
                failures += not check("authenticated landing", r2, {200, 302, 303})
                r3 = s.get(BASE + "/platform/dashboard", timeout=10)
                failures += not check("platform dashboard", r3, {200, 302, 303})
        else:
            print("SKIP login: SUPERADMIN_PASSWORD is not set")
    except requests.RequestException as exc:
        print("FAIL HTTP connection:", exc)
        return 1
    print("PHANTA HTTP SMOKE:", "PASS" if not failures else "FAIL")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
