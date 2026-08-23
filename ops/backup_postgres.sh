#!/bin/sh
set -eu

: "${DATABASE_URL:?DATABASE_URL is required}"

STAMP="$(date -u +%Y%m%d-%H%M%S)"
OUTPUT="${BACKUP_FILE:-phanta-postgres-${STAMP}.dump}"

if [ -e "$OUTPUT" ]; then
  echo "Refusing to overwrite existing backup: $OUTPUT" >&2
  exit 1
fi

command -v pg_dump >/dev/null 2>&1 || {
  echo "pg_dump is required. Install PostgreSQL client tools first." >&2
  exit 1
}

echo "Creating PHANTA PostgreSQL logical backup: $OUTPUT"
pg_dump "$DATABASE_URL" \
  --format=custom \
  --no-owner \
  --file="$OUTPUT"

test -s "$OUTPUT"
echo "Backup created successfully: $OUTPUT"
