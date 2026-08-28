"""Tenant offboarding — command line.

WHY A CLI
---------
`services/offboarding_service.py` had no entry point: no route, no script.
It could only be run from a Python shell.

A CLI rather than an admin button is deliberate. Stage 2 permanently
anonymises every customer belonging to a workshop. That is not an action that
should sit one mis-click away in a web interface, and it is rare enough
(a workshop leaving) that the friction of a terminal is a feature.

THE TWO STAGES
--------------
    stop      access disabled, users signed out, integrations revoked.
              REVERSIBLE. Data is left intact so it can still be exported.

    status    what remains before deletion can proceed.

    delete    anonymises customer personal information. NOT REVERSIBLE.
              Refuses to run unless a data export has been recorded, because
              the export opportunity is a step in the process rather than a
              courtesy. --force records that the workshop declined it.

Billing records, legal acceptances and audit/security logs deliberately
survive: they are retained for legal and accountability obligations.

USAGE
-----
    python -m scripts.offboard status --location-id 12
    python -m scripts.offboard stop   --location-id 12 --actor you@phanta --reason "non-payment"
    python -m scripts.offboard delete --location-id 12 --actor you@phanta
"""
from __future__ import annotations

import argparse
import json
import sys

from services.offboarding_service import (
    begin_offboarding, complete_offboarding, offboarding_readiness,
)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="offboard", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    for name, help_text in (
        ("status", "show what remains before deletion"),
        ("stop", "stage 1: disable access and integrations (reversible)"),
        ("delete", "stage 2: anonymise customer data (NOT reversible)"),
    ):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("--location-id", type=int, required=True)
        if name != "status":
            p.add_argument("--actor", required=True, help="who is performing this")
        if name == "stop":
            p.add_argument("--reason", default=None)
        if name == "delete":
            p.add_argument("--force", action="store_true",
                           help="proceed without a recorded export (records that it was declined)")
            p.add_argument("--yes", action="store_true",
                           help="skip the interactive confirmation")

    args = parser.parse_args(argv)

    if args.command == "status":
        print(json.dumps(offboarding_readiness(args.location_id), indent=2))
        return 0

    if args.command == "stop":
        result = begin_offboarding(args.location_id, args.actor, reason=args.reason)
        print(json.dumps(result, indent=2))
        return 0

    # delete
    readiness = offboarding_readiness(args.location_id)
    print(json.dumps(readiness, indent=2))
    print()
    if not readiness["data_export_taken"] and not args.force:
        print("No data export has been recorded for this location.")
        print("Give the workshop its export first, or pass --force to record that "
              "they declined it.")
        return 1

    if not args.yes:
        print(f"This will permanently anonymise "
              f"{readiness['customers_pending_anonymisation']} customer record(s) "
              f"for location {args.location_id}. This cannot be undone.")
        confirm = input("Type the location id to confirm: ").strip()
        if confirm != str(args.location_id):
            print("Confirmation did not match. Nothing was changed.")
            return 1

    result = complete_offboarding(args.location_id, args.actor, force=args.force)
    print(json.dumps(result, indent=2))
    print("\n" + result["backup_note"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
