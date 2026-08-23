# Phase 20 — Production Readiness / QA Hardening

## Objective

Phase 20 is the internal engineering gate before external production verification. The gate covers code correctness, database migrations, tests, replay/idempotency controls, security hygiene, backups/recovery, monitoring and alerting.

The architecture remains deliberately simple: Python/Flask/SQLAlchemy, relational database memory, direct Meta WhatsApp Cloud API, Paystack, and a single Service Advisor loop rather than a custom multi-agent framework. This matches the Service Advisor blueprint's v1 constraint.

## Completed in this pass

### 1. Syntax / import health
- Python AST parse: PASS for all project Python files.
- `python -m compileall -q .`: PASS.
- No syntax errors found.

### 2. Automated test suite
- `pytest -q`: **114 passed, 3 skipped**.
- The three skipped tests require a configured PostgreSQL test database.
- Existing `datetime.utcnow()` test warnings were removed by using timezone-aware UTC datetimes.

### 3. Migration integrity
A fresh Alembic upgrade initially exposed a real defect: Phase 2's migration creates the current model metadata with `create_all()`, while later migrations also create Phase 4/5/8/9/15 tables. A fresh `alembic upgrade head` therefore failed with `table service_rules already exists`.

This pass hardens the affected migrations so the current schema snapshot and incremental migrations converge safely.

Validation:
- fresh SQLite `alembic upgrade head`: PASS
- resulting revision: `0010_post_service_review_phase18 (head)`
- fresh SQLite `alembic downgrade base`: PASS

PostgreSQL-only RLS/GiST constraints still require execution against the real Railway PostgreSQL database; SQLite cannot validate those PostgreSQL-specific controls.

### 4. Repository hygiene
- Added `.gitignore`.
- Generated Python bytecode/cache directories are excluded from source control.
- Local SQLite database artifacts are excluded from source control.
- Temporary test database is not part of the deliverable.

### 5. Secret scan
A repository scan found no obvious hard-coded provider/API key patterns in the source package. This is only a source scan; live Railway secrets still require an operator-side review.

## Required Phase 20 gates still outstanding

### A. PostgreSQL integration gate
Run the full integration suite against a disposable/staging PostgreSQL database, including:
- RLS tenant isolation
- booking overlap EXCLUDE constraints
- append-only approval protection
- migration upgrade/downgrade
- transaction-local tenant context

### B. Meta staging gate
Use a dedicated staging WhatsApp number/business and verify:
- webhook signature validation
- webhook replay/idempotency
- inbound message persistence
- outbound send
- delivery/read/failed status handling
- template handling
- token validity/expiry monitoring
- reconnect behaviour

The Meta blueprint explicitly calls for idempotent webhook handling and token expiry monitoring.

### C. Paystack staging gate
Run the complete test-mode lifecycle:
- initialize
- hosted checkout
- callback
- verify
- `charge.success` webhook
- duplicate webhook delivery
- failed payment
- subscription create/disable
- failed renewal/dunning
- refund
- reconciliation

The Paystack blueprint requires webhook signature verification, amount verification and idempotent fulfilment keyed by the transaction reference.

### D. Google integration gate
If Google Calendar is part of the deployed build, verify:
- OAuth consent flow
- encrypted token storage
- one-way event creation
- token failure/re-authentication
- sync/reconciliation behaviour

Google Calendar remains a sync target rather than PHANTA's booking source of truth.

### E. Security gate
Perform an operator review of:
- Railway environment variables
- Meta app secret
- Meta token encryption key
- Paystack secret key
- AI provider keys
- Google OAuth credentials
- webhook signature validation
- access-control enforcement on both dashboards
- tenant isolation
- logging of sensitive values
- POPIA retention/deletion procedures

### F. Backup / recovery gate
Must be demonstrated against the actual production/staging database:
1. create known test data;
2. take a backup;
3. delete/alter the test data;
4. restore into a separate database;
5. verify counts, tenant boundaries and critical audit/approval records;
6. record restore time and result.

### G. Monitoring / alerting gate
Minimum operational signals:
- WhatsApp send/delivery failures
- Meta permission error spikes
- Meta token expiry/invalid state
- WhatsApp quality degradation
- Paystack failed payments
- Paystack webhook/reconciliation failures
- Google token failures, if enabled
- AI latency
- AI token usage/cost
- tool execution failures
- background job failures

## Current Phase 20 status

**IN PROGRESS — internal code/QA hardening is green; external provider and real PostgreSQL gates remain.**

Do not mark Phase 20 production-ready until the PostgreSQL, Meta, Paystack, backup/recovery and monitoring gates have been exercised in a staging/production-like environment.
