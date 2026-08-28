"""Report on the legacy plaintext-credential tables before they are dropped.

WHAT THESE TABLES ARE
---------------------
whatsapp_numbers, messaging_accounts and webhook_events predate the
owner/location tenant model. They are keyed on the removed franchise-era
workshop_id, they have no row level security, and they carry credentials as
PLAINTEXT columns:

    whatsapp_numbers    access_token, webhook_verify_token
    messaging_accounts  access_token, auth_secret, webhook_secret,
                        webhook_verify_token
    webhook_events      (no credentials, but same dead workshop_id key)

Nothing in the running application reads or writes them any more --
services/messaging_provider.py resolves WhatsApp connections through
MetaBusinessConnection, where tokens are Fernet-encrypted via MetaTokenStore.
They are still created on every boot by database/schema.py, so they may still
hold rows written by an older deployment.

WHY THIS SCRIPT EXISTS RATHER THAN A STRAIGHT DROP MIGRATION
------------------------------------------------------------
If these tables still contain rows, those are live Meta credentials sitting in
plaintext. Dropping the tables would remove the evidence without addressing
the exposure: any token that was stored there must be treated as compromised
and rotated at Meta first.

So: run this, act on what it reports, then drop.

USAGE
-----
    python -m scripts.inspect_legacy_credential_tables

Prints row counts and, where rows exist, how many carry a non-empty
credential column. It never prints a credential value.
"""
from __future__ import annotations

import sys

from database import query_db

LEGACY_TABLES = {
    "whatsapp_numbers": ["access_token", "webhook_verify_token"],
    "messaging_accounts": ["access_token", "auth_secret", "webhook_secret", "webhook_verify_token"],
    "webhook_events": [],
}


def _count(sql, args=()):
    row = query_db(sql, args, one=True)
    if not row:
        return 0
    return int(list(row.values())[0] or 0)


def main() -> int:
    findings = []

    for table, secret_columns in LEGACY_TABLES.items():
        try:
            total = _count(f"SELECT COUNT(*) AS c FROM {table}")
        except Exception as exc:
            print(f"{table}: could not be read ({type(exc).__name__}) -- may not exist. Skipping.")
            continue

        print(f"\n{table}: {total} row(s)")

        if total == 0:
            print("  Empty. Safe to drop.")
            continue

        for column in secret_columns:
            try:
                populated = _count(
                    f"SELECT COUNT(*) AS c FROM {table} WHERE {column} IS NOT NULL AND {column} <> ''"
                )
            except Exception:
                continue
            if populated:
                findings.append((table, column, populated))
                print(f"  {column}: {populated} row(s) hold a PLAINTEXT value")

    print("\n" + "=" * 60)
    if not findings:
        print("No plaintext credentials found in the legacy tables.")
        print("They can be dropped without rotating anything at Meta.")
        return 0

    print("PLAINTEXT CREDENTIALS FOUND. Do NOT simply drop these tables.")
    print("Treat every value below as exposed:")
    for table, column, count in findings:
        print(f"  - {table}.{column}: {count} value(s)")
    print("\nRotate/invalidate the affected tokens at Meta first, confirm the")
    print("current WhatsApp connections still work through MetaBusinessConnection,")
    print("then drop the tables.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
