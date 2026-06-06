# Improvement Roadmap

## CRITICAL

1. Remove or harden known-password reset paths.
   - Files: `reset_live_passwords.py`, `app.py`
   - Issue: `password1234` and `login1234`.
   - Action: require env password, owner confirmation, or remove from production.

2. Gate demo account bootstrap.
   - File: `database.py`
   - Function: `_ensure_demo_access_accounts()`
   - Action: require `ENABLE_DEMO_ACCOUNTS=true` outside local development.

3. Add retention policy for webhook and message logs.
   - Tables: `webhook_events`, `paystack_webhook_events`, `communication_logs`, `chatbot_messages`.
   - Action: archive or prune old rows by policy.

## HIGH

1. Validate all `NOT VALID` foreign keys.
   - Migration: `20260519_0001_production_constraints.py`
   - Action: data cleanup and `VALIDATE CONSTRAINT`.

2. Replace DB polling queue at scale.
   - Files: `automation_worker.py`, `automation_engine.py`
   - Action: move to durable queue when job volume grows.

3. Modularize `app.py`.
   - Current `routes/*.py` are empty placeholders.
   - Action: move active routes into registered blueprints.

4. Add Paystack refunds/cancellations.
   - File: `services/paystack.py`
   - Action: implement refund API and cancellation workflows.

5. Add Meta token refresh/expiry monitoring.
   - Table: `messaging_accounts.token_expiry`
   - Action: alert before expiry.

6. Add Sentry initialization or remove dependency.
   - File: `requirements.txt`
   - Action: configure `sentry_sdk.init()` or remove package.

## MEDIUM

1. Align Prisma schema with runtime schema.
   - Files: `prisma/schema.prisma`, `database.py`
   - Issue: Prisma models are cleaner but not full runtime schema.

2. Add unique customer constraints.
   - Table: `customers`
   - Action: consider `(franchise_id, phone)` after deduplication.

3. Add index for communication duplicate suppression.
   - Table: `communication_logs`
   - Query: recipient + subject + created_at.

4. Add UI for failed Paystack events.
   - Template: `templates/admin_client_audit.html`

5. Add role migration plan for:
   - `branch_manager`
   - `technician`
   - `accounts`
   - `viewer`

6. Add backup restore runbook test automation.

## LOW

1. Remove empty `routes/*.py` placeholders or add explicit comments.
2. Clean legacy `whatsapp_numbers` after migration.
3. Move long docs into `/docs` folder if repo grows.
4. Add doc links in admin UI.
5. Add richer dashboard metrics.

## Meta Improvements

- Support multiple active numbers per workshop only if tenant architecture is intentionally changed.
- Add phone quality/status display from Meta.
- Add template messaging support.
- Add webhook subscription verification tool.
- Add Meta app review checklist page.

## Railway Improvements

- Add Redis for rate limits.
- Add separate staging environment.
- Add database backup verification automation.
- Add release checklist script.
- Add structured log drains.

## Security Improvements

- Remove known temporary passwords.
- Enforce MFA externally for Railway, Meta, Paystack, GitHub.
- Add IP allowlist for superadmin if business process permits.
- Add periodic access reviews.
- Add secret rotation calendar.

## Scalability Improvements

- Queue backend for automation jobs.
- Retention/archive jobs.
- More granular indexes.
- Split `app.py` into blueprints.
- Add read replicas only after query patterns are measured.

## Reliability Improvements

- Worker stale-lock recovery.
- Scheduler job-state persistence instead of in-memory daily markers.
- Alerting on failed messages and failed jobs.
- Alerting on payment webhook failures.
- Alerting on Meta token expiry.
