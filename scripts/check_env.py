"""Check this deployment's environment variables.

WHY
---
Most PHANTA deployment failures are a missing environment variable, and the
symptom rarely names the variable. `ADMIN_DATABASE_URL` fails the deploy before
the application starts. `RAILWAY_ENVIRONMENT` missing means no secure cookie
and no HSTS, which looks like nothing at all. `SENTRY_DSN` missing on the cron
services means every scheduled-job failure is silent.

This reports what is set, what is missing, and what each missing one will
actually break -- without ever printing a value.

USAGE
-----
    python -m scripts.check_env             # on the web service
    python -m scripts.check_env --cron      # on a cron service

Exit code 0 = safe to deploy. 1 = something required is missing.

SAFETY
------
Values are NEVER printed, only whether they are set and (for a few) whether the
value is obviously wrong -- for example WEB_CONCURRENCY above 1, which silently
multiplies every rate limit. Safe to run in a shared terminal and safe to
screenshot when asking for help.
"""
from __future__ import annotations

import argparse
import os
import sys

# (variable, breaks-what-if-missing)
REQUIRED_ALWAYS = [
    ("DATABASE_URL",
     "The app cannot start. Railway supplies this from the Postgres service."),
    ("FLASK_SECRET_KEY",
     "The app refuses to start. Generate: python -c \"import secrets; print(secrets.token_hex(32))\""),
    ("SUPERADMIN_PASSWORD",
     "Bootstrap refuses to create your platform admin account, so you cannot log in."),
    ("META_TOKEN_ENCRYPTION_KEY",
     "WhatsApp credentials cannot be encrypted or decrypted. Generate: "
     "python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""),
]

REQUIRED_IN_PRODUCTION = [
    ("ADMIN_DATABASE_URL",
     "preDeployCommand exits 1 and THE DEPLOY FAILS before the app starts. "
     "Set it to the same value as DATABASE_URL unless you want migrations to "
     "use a more privileged role."),
    ("REGISTRATION_INVITE_CODE",
     "Registration is disabled entirely (fail-closed by design). Nobody can "
     "create an account, including your pilot workshops."),
]

RECOMMENDED = [
    ("SENTRY_DSN",
     "No error visibility. On a CRON service this means every scheduled-job "
     "failure is completely silent."),
    ("PAYSTACK_AUTHORIZATION_ENCRYPTION_KEY",
     "Recurring card authorisations cannot be stored, so subscriptions cannot "
     "renew. Must be a DIFFERENT Fernet key from META_TOKEN_ENCRYPTION_KEY."),
    ("PHANTA_PUBLIC_BASE_URL",
     "Customer-facing links (booking pages, opt-out links) may be wrong."),
    ("MESSAGE_BODY_RETENTION_DAYS",
     "Defaults to 14. Must match what your Privacy Policy states."),
    ("BACKUP_RETENTION_DAYS",
     "Defaults to 30. Used to tell offboarded workshops when their data ages "
     "out of backups -- verify it against Railway's actual window."),
    ("SESSION_LIFETIME_HOURS", "Defaults to 12."),
]

INTEGRATIONS = {
    "WhatsApp / Embedded Signup": [
        "META_APP_ID", "META_APP_SECRET", "META_SYSTEM_USER_TOKEN",
        "META_WHATSAPP_CONFIG_ID",
    ],
    "WhatsApp inbound webhook": ["META_WEBHOOK_VERIFY_TOKEN"],
    "Flyer Lady (Facebook posting)": ["META_FLYER_LADY_CONFIG_ID", "META_SOCIAL_REDIRECT_URI"],
    "Payments (Paystack)": ["PAYSTACK_SECRET_KEY", "PAYSTACK_PUBLIC_KEY", "PAYSTACK_WEBHOOK_SECRET"],
    "AI Service Advisor": ["OPENAI_API_KEY"],
    "Google Business Profile (optional)": ["GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET"],
}

# Variables that must NOT be set in production, and why.
DANGEROUS_IN_PRODUCTION = [
    ("RATELIMIT_ENABLED", ("0", "false", "no", "off"),
     "Rate limiting is DISABLED. This exists only for the test suite."),
    ("ALLOW_DEV_DEFAULT_CREDENTIALS", ("1", "true", "yes", "on"),
     "Development default credentials are accepted."),
    ("SKIP_ALEMBIC_MIGRATIONS", ("1", "true", "yes", "on"),
     "Database migrations are being skipped."),
]


def _set(name: str) -> bool:
    return bool((os.getenv(name) or "").strip())


def _is_production() -> bool:
    return (
        (os.getenv("FLASK_ENV") or "").lower() == "production"
        or (os.getenv("APP_ENV") or "").lower() == "production"
        or bool((os.getenv("RAILWAY_ENVIRONMENT") or "").strip())
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="check_env", description=__doc__)
    parser.add_argument("--cron", action="store_true",
                        help="checking a cron service rather than the web service")
    args = parser.parse_args(argv)

    production = _is_production()
    problems, warnings = [], []

    print("PHANTA environment check")
    print(f"  mode: {'PRODUCTION' if production else 'development'}")
    print(f"  service: {'cron' if args.cron else 'web'}")
    print()

    if not production:
        warnings.append(
            "RAILWAY_ENVIRONMENT is not set. In production this means NO secure "
            "cookie flag and NO HSTS header -- the app runs but is not hardened. "
            "Railway normally sets this itself; if it is missing on a Railway "
            "deployment, set RAILWAY_ENVIRONMENT=production manually."
        )

    print("REQUIRED")
    for name, breaks in REQUIRED_ALWAYS:
        ok = _set(name)
        print(f"  {'OK  ' if ok else 'MISS'}  {name}")
        if not ok:
            problems.append((name, breaks))

    if production:
        for name, breaks in REQUIRED_IN_PRODUCTION:
            ok = _set(name)
            print(f"  {'OK  ' if ok else 'MISS'}  {name}")
            if not ok:
                problems.append((name, breaks))

    print("\nRECOMMENDED")
    for name, breaks in RECOMMENDED:
        ok = _set(name)
        print(f"  {'OK  ' if ok else '--  '}  {name}")
        if not ok:
            warnings.append(f"{name}: {breaks}")

    print("\nINTEGRATIONS")
    for label, names in INTEGRATIONS.items():
        missing = [n for n in names if not _set(n)]
        if not missing:
            print(f"  OK    {label}")
        else:
            print(f"  --    {label}  (missing: {', '.join(missing)})")

    # Concurrency: not "missing", but wrong values silently weaken protection.
    concurrency = (os.getenv("WEB_CONCURRENCY") or "").strip()
    print("\nCONFIGURATION")
    if not concurrency:
        warnings.append(
            "WEB_CONCURRENCY is not set. Rate limiting uses in-process memory, "
            "so with more than one worker the effective limit is multiplied by "
            "the worker count. Set WEB_CONCURRENCY=1."
        )
        print("  --    WEB_CONCURRENCY not set (should be 1)")
    elif concurrency != "1":
        warnings.append(
            f"WEB_CONCURRENCY is {concurrency}. Rate limiting is in-process, so "
            f"every limit is effectively multiplied by {concurrency}. Set it to "
            "1, or configure RATELIMIT_STORAGE_URI with a shared backend."
        )
        print(f"  WARN  WEB_CONCURRENCY={concurrency} (should be 1)")
    else:
        print("  OK    WEB_CONCURRENCY=1")

    if production:
        for name, bad_values, breaks in DANGEROUS_IN_PRODUCTION:
            value = (os.getenv(name) or "").strip().lower()
            if value in bad_values:
                problems.append((name, f"MUST NOT be set in production. {breaks}"))
                print(f"  BAD   {name} is set to a value that weakens production")

    if args.cron and not _set("SENTRY_DSN"):
        problems.append((
            "SENTRY_DSN",
            "On a cron service this is effectively required: without it every "
            "scheduled-job failure is silent. Set it on the cron services as "
            "well as web -- they are separate deployments with separate variables."
        ))

    print()
    if problems:
        print("=" * 70)
        print("MUST FIX")
        for name, breaks in problems:
            print(f"\n  {name}")
            print(f"    {breaks}")
    if warnings:
        print("\n" + "=" * 70)
        print("WORTH FIXING")
        for warning in warnings:
            print(f"\n  - {warning}")

    print("\n" + "=" * 70)
    if problems:
        print(f"RESULT: {len(problems)} problem(s). Do not deploy until these are fixed.")
        return 1
    print("RESULT: no blocking problems found.")
    if warnings:
        print(f"        {len(warnings)} item(s) worth reviewing above.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
