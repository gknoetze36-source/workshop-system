# PHANTA

PHANTA is the consolidated multi-industry communications and booking platform. The current build is governed by `BUILD_ORDER.md`.

## Current scope
- Workshop is the first production vertical.
- The foundation retains the nine configured industry templates: workshop, salon, dentist, clinic, hotel, consultant, gym, cleaning, and repair.
- OpenAI is the only AI provider enabled for v1; the provider abstraction remains for future providers.
- Meta Embedded Signup and customer-scoped Meta token storage use the Phase 4–9 architecture.
- Booking confirmation is the only customer Yes/No decision recorded by PHANTA.
- Customer-facing booking communication uses date + morning arrival; exact time slots are not exposed.
- PHANTA does not determine workshop pricing, draft quotes, or repair authorisation.
- Review requests use a workshop-configured plain Google Review or HelloPeter URL.
- Phase 19 has separate workshop/reception and PHANTA platform-admin dashboards.

## Runtime
The Flask application entry point is `phanta_phanta_app.py` and the production container starts it with Gunicorn:

```text
gunicorn phanta_app:app --bind 0.0.0.0:${PORT:-8080}
```

## Environment
Copy `.env.example` into the deployment environment and supply real secrets there. Never commit provider tokens or encryption keys. `META_TOKEN_ENCRYPTION_KEY` is the canonical encryption key for Meta customer access tokens.

## Validation
Before GitHub/Railway deployment, run:

```text
python -m compileall -q .
python -m pytest -q
```

The application must also pass an actual local Gunicorn/Flask startup and PostgreSQL smoke test before production launch.

See `BUILD_ORDER.md` for phase-by-phase scope and production gates.

---

## Demo branch

This `demo` branch adds one thing on top of `main`: a seed script that
populates a demo workshop so the reception dashboard has real data to show
instead of loading empty.

### To view it

1. Deploy this branch to its own Railway environment (or run locally against
   a real Postgres instance — see note below on SQLite).
2. After the pre-deploy step runs (`python -m database.predeploy`), run once:
   ```
   python -m scripts.seed_demo_data
   ```
3. Go to `/login` and sign in:
   - **Email:** `demo.reception@phanta.example`
   - **Password:** `DemoPass123!`
4. You'll land on `/dashboard` — the reception/workshop dashboard — with:
   - a pending booking awaiting confirmation
   - a vehicle checked in and waiting
   - an overdue vehicle (in progress, past its scheduled end)
   - an unanswered WhatsApp message
   - connection health showing "not connected" and billing showing
     "not configured" (both correct and expected — no Meta/Paystack
     connection was seeded, which is itself useful to see)

Safe to re-run — `seed_demo_data.py` checks for the demo workshop's slug
first and no-ops if it already exists, so redeploys won't pile up duplicate
demo data.

### Note on local/SQLite testing

The dashboard's notes feature (`ai/dashboard/queries.py::booking_notes`)
uses Postgres-only SQL (`ANY(...)` array matching against the `notes`
table, which is created by an Alembic migration that only runs on the
Postgres backend). Every other part of the dashboard was verified directly
against seeded SQLite data in this session. Full end-to-end rendering,
notes included, needs to run against real Postgres — which is exactly
where this branch is meant to be deployed and reviewed, so this shouldn't
affect actually checking the dashboard on Railway.
