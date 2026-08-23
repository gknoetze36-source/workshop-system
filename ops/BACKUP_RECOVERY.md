# PHANTA Production Backup & Recovery

## Scope

PHANTA production uses PostgreSQL on Railway. The repository's deployment configuration requires
`DATABASE_URL`/Railway PostgreSQL variables and runs Alembic migrations during the Railway pre-deploy
step.

The repository does **not** prove that Railway volume backups or PITR are currently enabled, and it
does not contain an offsite backup service. Those are production infrastructure settings, not code
features. They must therefore be verified/enabled in the Railway project.

## What is backed up?

The primary production data is the PostgreSQL database, including:

- Owner and Location data
- Customers
- Vehicles
- Bookings and booking requests
- Notes/history
- Automation state
- Service/industry configuration
- WhatsApp/integration records stored in the database
- Billing/payment records stored in the database
- authentication/account records

The backup should be a PostgreSQL logical dump in PostgreSQL custom format (`pg_dump --format=custom`).
This is portable and can be restored to another PostgreSQL service.

The application source code is separately protected by Git/GitHub. Database backups are not a substitute
for source control.

## Required Railway protection layers

Railway currently provides three useful PostgreSQL protection layers:

1. **Volume backups** — routine snapshots of the PostgreSQL volume.
2. **Point-in-time recovery (PITR)** — recovery to a timestamp inside the enabled archive window.
3. **Logical `pg_dump` backups** — portable copies that survive deletion of the Railway project.

For PHANTA production, enable all three where practical. Railway's current documentation describes
daily/weekly/monthly volume backup schedules and PITR for PostgreSQL. See the Railway backup guide:
https://docs.railway.com/guides/postgres-backups-restores

### Minimum recommended schedule

- Railway volume backup: Daily.
- Railway volume backup: Weekly + Monthly retention if available/appropriate.
- PITR: Enabled before live customer data is introduced.
- Logical dump: Daily to storage outside the production database/project.
- Restore drill: Before first live customer, then periodically (at least monthly).

**Important:** the repository cannot verify that these Railway settings are enabled. The operator must
check the PostgreSQL service's **Backups** tab.

## Logical backup

Use:

```bash
pg_dump "$DATABASE_URL" --format=custom --no-owner --file="backup-YYYYMMDD-HHMMSS.dump"
```

For Railway's private database, the Railway CLI can provide a tunnel:

```text
railway connect postgres --tunnel-only
```

Then run `pg_dump` against the tunnel connection shown by Railway.

The repository contains:

```text
ops/backup_postgres.sh
```

It performs a logical custom-format dump and refuses to overwrite an existing dump.

## Where backups should live

A backup stored only inside the production Railway service is **not an adequate disaster-recovery copy**.

The operational target is:

```text
PHANTA PostgreSQL
      │
      ├── Railway volume backups
      │
      ├── Railway PITR
      │
      └── pg_dump
            │
            └── OFFSITE / SEPARATE STORAGE
```

The offsite destination must be a separate storage system (for example an S3-compatible bucket).
The repository does not contain credentials for such storage and therefore does not pretend that
offsite automation is already configured.

Until an offsite bucket is configured, a manually downloaded encrypted dump stored separately from
Railway is the minimum practical fallback.

## Restore procedure

### A. Bad deployment / recent data mistake

1. Stop or disable the affected application path if necessary.
2. Identify the incident timestamp.
3. Prefer PITR when the exact recovery point matters.
4. Otherwise restore the appropriate Railway volume backup.
5. If using PITR, Railway creates a restored PostgreSQL service rather than overwriting the source.
6. Verify the restored database before changing PHANTA's `DATABASE_URL`.
7. Point PHANTA at the verified restored database.
8. Run application health checks.
9. Run critical owner/location isolation and booking/database smoke tests.
10. Keep the original database available until the recovery is verified.

### B. Restore from a logical dump

Use:

```bash
pg_restore \
  --dbname="$TARGET_DATABASE_URL" \
  --no-owner \
  --exit-on-error \
  backup-YYYYMMDD-HHMMSS.dump
```

The repository contains:

```text
ops/restore_postgres.sh
```

The script requires an explicit confirmation variable so it cannot silently overwrite a target.

**Never perform a first restore directly into the live production database.**

Restore into a scratch/new PostgreSQL service first.

## Restore verification

After restoring, verify at minimum:

1. Alembic migration state.
2. Database connectivity.
3. Owner → Location relationships.
4. Customer/vehicle/booking counts.
5. Recent booking records.
6. Authentication.
7. Owner/location isolation.
8. WhatsApp/integration configuration records.
9. Billing/payment records.
10. `/health`.
11. Critical application smoke tests.

Record:

- dump timestamp
- restore timestamp
- restore duration
- database size
- latest recoverable point
- test results

These are the real Recovery Point Objective (RPO) and Recovery Time Objective (RTO), rather than estimates.

## Failed deployment recovery

PHANTA's deployment currently runs:

```text
python -m database.predeploy
```

before the web process starts.

The pre-deploy process creates/ensures the legacy schema, runs compatibility work, and then runs
Alembic migrations against PostgreSQL.

If a migration/deployment fails:

1. Do not repeatedly run destructive manual SQL.
2. Inspect the failed migration and deployment logs.
3. If the database is still valid, fix the migration/deployment issue and redeploy.
4. If data/schema was damaged, restore to a new PostgreSQL service using PITR or a verified backup.
5. Verify the restored database.
6. Change PHANTA's database reference to the verified database.
7. Only then continue migrations.

## Migration recovery rule

Database restore and application migration are separate operations.

Do **not** blindly run `alembic upgrade head` on an uncertain restored database.

After restoration:

```bash
alembic current
alembic heads
```

Compare the restored migration state with the source code.

Only after the database is verified should pending migrations be applied.

Before a production migration that changes data destructively:

1. Take/verify a current backup.
2. Record the current Alembic revision.
3. Deploy the migration.
4. Verify application health and critical workflows.
5. Keep the recovery point available until verification is complete.

## Production actions required

Before onboarding real customers:

- [ ] Open PHANTA PostgreSQL service in Railway.
- [ ] Open **Backups**.
- [ ] Enable a daily volume backup schedule.
- [ ] Add weekly/monthly retention as appropriate.
- [ ] Enable PITR.
- [ ] Wait until Railway reports PITR/base backup health.
- [ ] Create a manual volume backup.
- [ ] Take one logical `pg_dump`.
- [ ] Store the dump outside the Railway project.
- [ ] Perform a restore drill into a scratch database/service.
- [ ] Verify the restored application.
- [ ] Record restore duration and recovered timestamp.
- [ ] Schedule recurring logical offsite dumps once external storage is configured.
- [ ] Do not store production database credentials in Git.

## Known recovery risks

1. The ZIP does not prove that Railway volume backups are enabled.
2. The ZIP does not prove PITR is enabled.
3. No offsite object-storage backup service is currently present in the repository.
4. A Railway volume backup is tied to the same project/environment; it is not the sole disaster-recovery layer.
5. A logical dump has no value if it has never been successfully restored.
6. Migrations can make a database structurally incompatible with an older application version; recovery must therefore include application/migration compatibility verification.
7. Credentials used to access production backups must never be committed to source control.

## Security finding

The supplied ZIP contained `railway-vars.json` with production database credentials. That file is not
required by the application and should not be part of a production source archive.

The production-ready archive removes the credential-bearing file and provides a safe
`railway-vars.example.json` placeholder instead.

Rotate the exposed PostgreSQL credential in Railway if the credential-bearing file was ever committed
to GitHub, shared outside the trusted environment, or otherwise exposed.
