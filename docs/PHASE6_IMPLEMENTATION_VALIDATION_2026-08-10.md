# PHANTA Phase 6 — Implementation & Validation

## Build_Order scope

Phase 6 requires:
- Encrypt tokens.
- debug_token.
- expiry/status checks.
- reconnect state.
- scheduled expiry monitor.
- dashboard connection health.

## Implementation status

### 1. Encrypted customer Meta token storage — COMPLETE
- `integrations/meta/auth/token_store.py` encrypts customer Meta business tokens with Fernet.
- Key comes from `META_TOKEN_ENCRYPTION_KEY` and is not stored in the database.
- `token_key_version` and a non-secret `token_secret_ref` are stored.
- Raw customer tokens are not returned by dashboard health responses.

### 2. debug_token — COMPLETE
- `GraphApiClient.debug_customer_token()` calls Meta `/debug_token`.
- The app credentials are used server-side for the debug request.
- Customer token is supplied only to the server-side client.
- No token is exposed to frontend JavaScript.

### 3. Expiry/status checks — COMPLETE
- `MetaTokenStatusService` validates the stored token with `debug_token`.
- It records Meta's expiry timestamp when supplied.
- `connected` = valid and outside the warning window.
- `expiring_soon` = valid and within 7 days of expiry.
- `reconnect_required` = missing, invalid, expired, or undecryptable token.
- `not_connected` = no connection record.

### 4. Reconnect state — COMPLETE
- Connection state is persisted as `reconnect_required` when health checks fail.
- The existing Phase 5 Embedded Signup flow is the reconnect mechanism.
- The canonical Connect WhatsApp page now changes to `Reconnect WhatsApp` for `reconnect_required` / `expiring_soon` states.
- The workshop dashboard exposes a reconnect/refresh action for those states.
- Workshop owners never manually paste Meta access tokens.

### 5. Scheduled expiry monitor — COMPLETE
- `jobs/meta_token_monitor.py` checks every active tenant.
- Each tenant is checked in its own database session with tenant context applied.
- `jobs/scheduler.py` invokes the monitor.
- Railway Cron can execute `python -m jobs.scheduler`.
- Results include tenant ID, status, health and reconnect requirement for operational logging.

### 6. Dashboard connection health — COMPLETE
- API endpoint: `GET /integrations/meta/connection-health`.
- Workshop dashboard reads connection health from the dashboard query layer.
- Health includes status, quality rating, last health check, phone number and reconnect state.
- Platform dashboard aggregates connection states across tenants.

## Validation performed

- Phase 6 unit tests: **13 passed**.
- Phase 19 dashboard tests: included in the Phase 6 targeted run and passed.
- Python `compileall`: **PASS**.
- Full repository pytest collection: **BLOCKED by environment**, not by Phase 6 code. Two Paystack test modules require `psycopg2`, which is not installed in this execution environment. The project `requirements.txt` already declares `psycopg2-binary`.

## External/runtime verification still required

- Set a production `META_TOKEN_ENCRYPTION_KEY` in the secret manager.
- Run the application against PostgreSQL/Railway.
- Perform a real Meta Embedded Signup connection.
- Verify a real customer token with Meta `debug_token`.
- Allow the scheduled monitor to run in the production scheduler/Railway Cron.
- Confirm dashboard health and reconnect behaviour with a real connection.
- Test actual token expiry/re-authorization in the Meta environment when practical.

## Scope boundary

Phase 6 does not add phone registration, webhooks, or outbound messaging. Those remain Phase 7, Phase 8 and Phase 9 respectively.
