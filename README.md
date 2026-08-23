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
