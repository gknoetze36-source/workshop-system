# PHANTA Step 9 — Automation Engine

## Runtime boundary

The automation engine is universal and executes Location-owned automation rules. Every runtime record is scoped to `location_id`.

```text
OWNER
  └── LOCATION
       └── AUTOMATIONS
            ├── universal engine
            │    ├── triggers
            │    ├── conditions_json
            │    ├── action_json
            │    ├── scheduling
            │    └── execution/retry/logging
            └── selected industry workflow definitions
                 ├── workshop
                 ├── salon
                 └── other supported industries
```

## Separation

`app/core/domain/automation/catalog.py` contains industry workflow definitions. It does not execute them. The repository/service layer is universal and takes `location_id` as the ownership boundary.

A Location's selected industry controls which automation templates are presented/configured. Workshop rules are not loaded for a salon, and vice versa.

## Database migration

`0020_automation_location_ownership.py` makes `automation_rules`, `scheduled_jobs`, `automation_logs`, and `failed_jobs` Location-owned. Existing ownership is derived only from existing relationships; unresolved rows cause the migration to stop rather than guessing.

## Production readiness

Before running migration 0020 on Railway:

1. Back up PostgreSQL.
2. Inspect existing automation rules for `location_id` coverage.
3. Confirm every scheduled/failed/log record can be traced to its automation rule/job.
4. Run Alembic migration.
5. Verify Location A cannot see or execute Location B automation records.
6. Test at least one existing workshop booking confirmation/reminder end-to-end.
7. Test the automation settings page for a non-workshop Location.
