# PHANTA Step 4 — Scheduled Automation / Railway Cron

## Target

```text
OWNER
  |
LOCATION
  |
AUTOMATIONS
  |
  +-- Meta token monitoring
  +-- Lifecycle communication
  +-- Deterministic follow-ups
  +-- Flyer Lady publish queue
  +-- Paystack reconciliation
```

There is no franchise or branch scheduling.

## Execution

`jobs.scheduler` is a finite command:

```text
python -m jobs.scheduler
```

It executes each registered job once and exits. A failure in one job is logged
and returned as an error result without preventing the other jobs from running.

The dedicated Railway Cron service should use:

```text
Cron: */5 * * * *
Start: python -m jobs.scheduler
Config: railway-cron.toml
```

The web service must continue using `railway.toml` and Gunicorn. Do not put the
cron schedule on the web service.

Railway cron schedules use UTC and the minimum supported interval is five
minutes. A cron execution must terminate when the command finishes; overlapping
executions are skipped by Railway.

## Job inventory

| Job | Entry point | Location scope |
|---|---|---|
| Meta token monitor | `jobs/meta_token_monitor.py` | Active Location IDs + `set_location_id` |
| Lifecycle communication | `jobs/lifecycle_communication.py` | Active Location IDs + `set_location_id` |
| Follow-up worker | `jobs/follow_up.py` | Active Location IDs + `set_location_id` |
| Flyer Lady queue | `jobs/flyer_lady.py` | `post.location_id` |
| Paystack reconciliation | `jobs/paystack_reconciliation.py` | Active Location IDs + `set_location_id` + repository filter |

## Duplicate execution

Railway skips a new cron execution while a previous execution is still
running. Existing follow-up records also use location-scoped deduplication.
The automation repository contains job locking/status handling for scheduled
automation records.

A database-level uniqueness/claim mechanism should be added before scaling to
multiple independent cron consumers. This Step 4 does not introduce Redis or
a new queue architecture.

## External Railway action still required

A second Railway service must be created/configured for the cron process.
The repository supplies the exact config and command, but it cannot create the
Railway service from inside the ZIP.

The cron service should receive the same required environment variables as the
application service, especially `DATABASE_URL` and the existing Meta/Paystack
configuration.

No credentials are added by this change.
