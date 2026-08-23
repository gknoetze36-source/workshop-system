# PHANTA Phase 6 — Meta Token Management

## Scope

This phase implements the Build Order items:

- encrypted customer token storage
- `debug_token` validation
- expiry/status checks
- reconnect state
- scheduled expiry monitoring
- dashboard connection health

Phase 7 phone registration, Phase 8 webhooks, and Phase 9 outbound messaging remain outside this phase.

## Token security

Customer Embedded Signup tokens are encrypted with Fernet before persistence. The encryption key is supplied by `META_TOKEN_ENCRYPTION_KEY` and is never stored in the database.

The database stores:

- `encrypted_access_token`
- `token_key_version`
- `token_expires_at`
- non-secret `token_secret_ref`

The raw customer token is never returned by the dashboard endpoint and is never written to normal application logs.

## Health states

- `connected` — token is valid and not within the expiry warning window.
- `expiring_soon` — token is valid but expires within 7 days.
- `reconnect_required` — token is missing, invalid, expired, or cannot be decrypted.
- `not_connected` — tenant has no Meta connection.

A reconnect is performed through the existing Phase 5 Embedded Signup flow; Phase 6 marks the connection state so the dashboard presents a reconnect action. No raw token is manually entered by the workshop owner.

## Scheduled job

`jobs.meta_token_monitor.run_meta_token_monitor()` checks every active tenant. Each tenant is checked inside its own RLS transaction; the monitor does not bypass tenant isolation.

Railway Cron can run:

```bash
python -m jobs.scheduler
```

The scheduler entry point executes the Meta token monitor for every active tenant. The monitor runs each tenant health check in a tenant-scoped database session, records the resulting connection state, and returns a per-tenant status summary for operational logging.

## Required secret

```text
META_TOKEN_ENCRYPTION_KEY=<Fernet key>
```

Generate a key with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Do not replace the key without a token re-encryption plan.
