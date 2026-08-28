"""One-off migration: hash any remaining plaintext user passwords.

WHY THIS EXISTS
---------------
services/auth_service.py used to contain a fallback that compared the
submitted password directly against a plaintext ``users.password`` column:

    if not valid and user.get("password") and user.get("password") == password:

That branch is being removed. Any account still relying on it -- i.e. one
that has a plaintext password but no usable password_hash -- would be locked
out the moment the branch goes, so this script converts them first.

WHAT IT DOES
------------
For every user row with a non-empty ``password``:
  * if ``password_hash`` is empty/NULL, derive it from the plaintext
  * blank the plaintext column either way

It deliberately NEVER overwrites an existing password_hash. If a row somehow
has both, the hash is the newer credential and the plaintext is stale.

RUN THIS BEFORE DEPLOYING the auth_service.py change, so there is no window
in which an affected user cannot log in.

USAGE
-----
    python -m scripts.migrate_plaintext_passwords --dry-run    # report only
    python -m scripts.migrate_plaintext_passwords              # apply

Safe to run repeatedly; a second run finds nothing to do. Prints no password
values, only counts and user ids.
"""
from __future__ import annotations

import argparse
import sys

from werkzeug.security import generate_password_hash

from database import query_db, execute_db, utc_now


def find_affected():
    """Return rows that still carry a plaintext password."""
    return query_db(
        """
        SELECT id, username, password, password_hash
        FROM users
        WHERE password IS NOT NULL AND password <> ''
        """
    ) or []


def migrate(dry_run: bool = False) -> int:
    rows = find_affected()
    if not rows:
        print("No plaintext passwords found. Nothing to migrate.")
        print("The auth_service.py fallback branch can be removed safely.")
        return 0

    print(f"Found {len(rows)} user row(s) with a plaintext password.")
    hashed = 0
    cleared_only = 0

    for row in rows:
        user_id = row["id"]
        has_hash = bool((row.get("password_hash") or "").strip())

        if has_hash:
            # Existing hash wins; the plaintext is stale and is only cleared.
            cleared_only += 1
            action = "clear stale plaintext (hash already present)"
        else:
            hashed += 1
            action = "derive password_hash from plaintext, then clear it"

        print(f"  user id={user_id} username={row.get('username')!r}: {action}")

        if dry_run:
            continue

        if has_hash:
            execute_db(
                "UPDATE users SET password=%s, updated_at=%s WHERE id=%s",
                ("", utc_now(), user_id),
            )
        else:
            execute_db(
                "UPDATE users SET password=%s, password_hash=%s, updated_at=%s WHERE id=%s",
                ("", generate_password_hash(row["password"]), utc_now(), user_id),
            )

    if dry_run:
        print("\nDRY RUN -- nothing was written.")
        return len(rows)

    remaining = len(find_affected())
    print(f"\nDone. Hashed: {hashed}. Stale plaintext cleared: {cleared_only}.")
    print(f"Rows still carrying a plaintext password: {remaining}")
    if remaining:
        print("WARNING: rows remain. Do NOT remove the auth fallback branch yet.")
        return 1
    print("All clear. The auth_service.py fallback branch can be removed safely.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report what would change, write nothing")
    args = parser.parse_args()
    return migrate(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
