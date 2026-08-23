#!/bin/sh
set -eu

: "${TARGET_DATABASE_URL:?TARGET_DATABASE_URL is required}"
: "${BACKUP_FILE:?BACKUP_FILE is required}"
: "${CONFIRM_RESTORE:?Set CONFIRM_RESTORE=YES to authorize a restore}"

if [ "$CONFIRM_RESTORE" != "YES" ]; then
  echo "Refusing restore. Set CONFIRM_RESTORE=YES explicitly." >&2
  exit 1
fi

test -f "$BACKUP_FILE" || {
  echo "Backup file not found: $BACKUP_FILE" >&2
  exit 1
}

command -v pg_restore >/dev/null 2>&1 || {
  echo "pg_restore is required. Install PostgreSQL client tools first." >&2
  exit 1
}

echo "WARNING: restoring into the target database: $TARGET_DATABASE_URL"
echo "This should normally be a new/scratch PostgreSQL database, not live production."
pg_restore \
  --dbname="$TARGET_DATABASE_URL" \
  --no-owner \
  --exit-on-error \
  "$BACKUP_FILE"

echo "Restore completed. Run the PHANTA recovery verification checklist before cutover."
