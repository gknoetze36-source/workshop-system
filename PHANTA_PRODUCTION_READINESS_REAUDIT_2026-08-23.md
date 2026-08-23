# PHANTA Production Readiness — Re-Audit — 2026-08-23

Re-running the same checks as the prior Step 10 audit (which found the app
could not boot at all), against the current codebase after the
2026-08-22/23 fixes. Every row below was actually executed against the code
in this session, not inferred.

Status legend: VERIFIED (checked, works) / PARTIAL (works but incomplete) /
GAP (missing, needs work before launch) / BLOCKED (needs something outside
this codebase — an external account, a real Postgres instance, etc.)

| # | Area | Status | Evidence | Next action |
|---|------|--------|----------|-------------|
| 1 | App boots from empty DB | **VERIFIED** | `initialize_database()` succeeds from zero, `phanta_app` imports, gunicorn serves `/health` → 200 `{"status":"ok"}` | none |
| 2 | `app.py`/`app` package naming collision (prior finding) | **VERIFIED resolved** | Entry point is `phanta_app.py`; no top-level `app.py` exists alongside the `app/` package | none |
| 3 | Automated test suite | **VERIFIED** | `184 passed, 3 skipped, 0 failed` — stable across 3 repeated runs. The 3 skips are `test_postgres_phase2.py` self-skipping correctly without a live Postgres URL | none |
| 4 | Boot smoke test exists and would catch a bad deploy | **VERIFIED** | `tests/test_boot_smoke.py`, 5/5 passing; previously proven to fail identically to the real incident against the unpatched code | wire into CI (see #10) |
| 5 | Schema single-source-of-truth (prior conflict) | **VERIFIED resolved** | `locations` defined once, in `owner_location.py`; `initialize.py` and `predeploy.py` share one bootstrap path; dead `locationes` typo and duplicate table defs removed | none |
| 6 | Sentry/logging wiring (prior finding: "dependency present, not wired") | **VERIFIED resolved** | `observability.py` calls `sentry_sdk.init()` gated on `SENTRY_DSN`; called from both `phanta_app.py` and `jobs/scheduler.py`; request bodies explicitly excluded from Sentry payloads | confirm `SENTRY_DSN` is actually set in Railway's variables — the code is ready, the env var is an external step |
| 7 | Background jobs / cron wiring (prior finding: "not wired") | **VERIFIED resolved** | `railway-cron.toml` runs `python -m jobs.scheduler` every 5 minutes as a dedicated Railway service; `run_scheduled_jobs()` includes `meta_token_monitor`, `lifecycle_communication`, `follow_up`, `flyer_lady`, `paystack_reconciliation`, and the new `automation_engine` job | deploy the second Railway service from `railway-cron.toml` — it's a separate service Railway won't create automatically from `railway.toml` alone |
| 8 | Backup / disaster recovery plan (prior finding: "none") | **VERIFIED resolved** | `ops/backup_postgres.sh` (real `pg_dump --format=custom`, refuses to overwrite an existing file) and `ops/BACKUP_RECOVERY.md` both exist | confirm this script is actually scheduled somewhere (cron, Railway, or manual runbook) — the script existing isn't the same as it running |
| 9 | Row-Level Security coverage | **VERIFIED, with one deliberate gap** | RLS enabled via migrations 0001/0005/0007/0008/0009/0011/0012/0013/0014/0015/0017, covering customers, vehicles, bookings, conversations, quotes, messaging, webhooks, and more. `users` is intentionally *not* RLS-scoped — correct, since login must resolve a user before any location context exists to scope by | no action — this is correct as designed, not a gap |
| 10 | CI pipeline | **GAP** | No `.github/workflows`, no CI config of any kind found anywhere in the repo. Nothing currently runs `pytest` — including the new boot smoke test — automatically before a deploy | add a CI step (even a minimal one) that runs `pytest tests/ --ignore=tests/integration/test_owner_location_isolation_railway.py --ignore=tests/integration_postgres_rls_webhooks.py` on every push, so the class of bug from the original incident can't reach Railway again without a human explicitly skipping the check |
| 11 | Environment variable documentation | **GAP** | Code references 46 distinct env vars across Flask, DB, Meta, Paystack, OpenAI, Sentry, rate limiting, and superadmin bootstrap. `railway-vars.example.json` documents 3 of them | expand `railway-vars.example.json` (or a dedicated `.env.example`) to list every required var, with which are hard-required at boot (`FLASK_SECRET_KEY`, `SUPERADMIN_PASSWORD`, `DATABASE_URL`) vs feature-gated (`SENTRY_DSN`, `META_*`, `PAYSTACK_*`, `OPENAI_API_KEY`) — right now this list only exists in my head from grepping the source, not anywhere a deploying human can read it |
| 12 | `.dockerignore` | **GAP** | None exists. `Dockerfile` does `COPY . .` with no exclusions — if anyone builds locally with a `.env` file present, it gets baked into the image layer (not caught by `.gitignore`, which only protects git, not `docker build`) | add a `.dockerignore` excluding at minimum `.env*`, `.git`, `tests/`, `docs/`, `*.md`, `__pycache__/`, `.pytest_cache/` |
| 13 | CSRF protection | **VERIFIED** | `WTF_CSRF_CHECK_DEFAULT = True` set globally in `phanta_app.py`; confirmed by `test_json_requests_are_csrf_protected` and `test_flyer_lady_sends_csrf_header`, both passing | none |
| 14 | Committed secrets | **VERIFIED clean** | Scanned for Stripe/Paystack-style live/test key patterns and Google API key patterns across `.py`/`.json`/`.md` — none found | none |
| 15 | Owner/Location tenant model matches target architecture | **VERIFIED** | `Location.owner_id` is `NOT NULL UNIQUE`, enforced at the DB layer (confirmed by the 58 test failures this constraint correctly caused before test fixtures were fixed) | none |
| 16 | Automation engine (new capability from this pass) | **VERIFIED, limited scope** | `fire_event()`/`process_due_automation_jobs()` tested end-to-end with real data: immediate + delayed execution, condition matching, job completion. Only `log_only` and `whatsapp_message` actions are wired — Flyer Lady publish and Service Advisor actions are stubbed as documented extension points, not implemented | decide the calling contract for those two actions before relying on them in a live automation rule |
| 17 | Postgres-only tests that can't run here | **BLOCKED (needs your Railway environment)** | `tests/integration/test_owner_location_isolation_railway.py` and `tests/integration_postgres_rls_webhooks.py` both hard-`SystemExit` without a real `DATABASE_URL` — by design, they're meant to run inside the Railway container, not a sandbox | run these two directly against your Railway Postgres before go-live: `python tests/integration/test_owner_location_isolation_railway.py` |

## Summary

Every issue from the prior Step 10 audit that could be checked here is
resolved: the app boots, Sentry is wired, cron is wired, backups exist, and
the naming collision is gone. Nothing regressed.

Three real gaps remain, all genuinely new findings from this pass, none of
them app-breaking on their own but all worth closing before you call this
launch-ready:

1. **No CI** — the boot smoke test that would have caught the original
   incident exists but nothing runs it automatically yet.
2. **Env vars are 46-deep in the code but 3 lines in the docs** — anyone
   deploying this without reading the source will miss required
   configuration.
3. **No `.dockerignore`** — a real (if narrow) risk of a local secret
   ending up inside a built image.

None of these block a first deploy by themselves, but #1 is the one I'd
close first — it's the difference between "this incident can't happen
again" and "this incident can't happen again as long as someone remembers
to run the tests."
