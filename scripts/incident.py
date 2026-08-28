"""Security incident register — command line.

WHY A CLI RATHER THAN A WEB PAGE
--------------------------------
`services/incident_service.py` had no entry point at all: no route, no script,
nothing imported it. It could only be driven from a Python shell, which means
that during an actual incident -- the one moment it matters -- there was no way
to use it without writing code under pressure.

A CLI rather than a dashboard page is deliberate:

  * incidents are recorded by whoever is responding, who is already at a
    terminal, not clicking through an admin UI;
  * an incident may involve the web application being unavailable or
    untrusted, in which case a page inside that application is exactly the
    wrong place to keep the record;
  * it keeps the audit trail free of a web form that could be used to
    fabricate entries through a compromised admin session.

The platform-admin read view for security_events is still in the dashboard;
this is for writing the incident record itself.

USAGE
-----
    python -m scripts.incident open \\
        --type unauthorised_access --severity high \\
        --summary "Suspicious admin login from unknown IP" \\
        --detected-by "monitoring" [--location-id 12] [--system auth]

    python -m scripts.incident list [--status open]
    python -m scripts.incident show <id>
    python -m scripts.incident update <id> --status contained \\
        --containment "Revoked sessions, rotated Meta token"
    python -m scripts.incident scope <id> [--location-id 12]

`scope` answers "whose data was involved" and records the COUNT on the
incident. It deliberately does not copy the affected people's details into the
incident record -- that would create a second store of the very data the
incident concerns.
"""
from __future__ import annotations

import argparse
import json
import sys

from services.incident_service import (
    open_incident, update_incident, get_incident, list_incidents, scope_incident,
    INCIDENT_TYPES, SEVERITY_LOW, SEVERITY_MEDIUM, SEVERITY_HIGH, SEVERITY_CRITICAL,
    STATUS_OPEN, STATUS_INVESTIGATING, STATUS_CONTAINED, STATUS_RESOLVED,
)

SEVERITIES = (SEVERITY_LOW, SEVERITY_MEDIUM, SEVERITY_HIGH, SEVERITY_CRITICAL)
STATUSES = (STATUS_OPEN, STATUS_INVESTIGATING, STATUS_CONTAINED, STATUS_RESOLVED)


def _print(row):
    if not row:
        print("not found")
        return
    for key, value in row.items():
        if value not in (None, ""):
            print(f"  {key}: {value}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="incident", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_open = sub.add_parser("open", help="record a newly detected incident")
    p_open.add_argument("--type", required=True, choices=INCIDENT_TYPES)
    p_open.add_argument("--severity", required=True, choices=SEVERITIES)
    p_open.add_argument("--summary", required=True)
    p_open.add_argument("--detected-by", required=True)
    p_open.add_argument("--location-id", type=int, default=None,
                        help="omit for an incident affecting PHANTA itself")
    p_open.add_argument("--system", default=None, dest="system_affected")
    p_open.add_argument("--data", default=None,
                        help="comma-separated categories of data involved")

    p_list = sub.add_parser("list", help="list recent incidents")
    p_list.add_argument("--status", choices=STATUSES, default=None)
    p_list.add_argument("--limit", type=int, default=50)

    p_show = sub.add_parser("show", help="show one incident")
    p_show.add_argument("incident_id", type=int)

    p_update = sub.add_parser("update", help="record investigation progress")
    p_update.add_argument("incident_id", type=int)
    p_update.add_argument("--status", choices=STATUSES)
    p_update.add_argument("--severity", choices=SEVERITIES)
    p_update.add_argument("--containment", dest="containment_actions")
    p_update.add_argument("--investigation", dest="investigation_notes")
    p_update.add_argument("--recovery", dest="recovery_actions")
    p_update.add_argument("--notifications", dest="notifications_sent")
    p_update.add_argument("--resolved-at", dest="resolved_at")

    p_scope = sub.add_parser("scope", help="determine how many records were involved")
    p_scope.add_argument("incident_id", type=int)
    p_scope.add_argument("--location-id", type=int, default=None)
    p_scope.add_argument("--customer-ids", default=None,
                         help="comma-separated customer ids, if a specific set is known")

    args = parser.parse_args(argv)

    if args.command == "open":
        categories = [c.strip() for c in (args.data or "").split(",") if c.strip()]
        incident_id = open_incident(
            incident_type=args.type, severity=args.severity, summary=args.summary,
            detected_by=args.detected_by, location_id=args.location_id,
            system_affected=args.system_affected, data_categories=categories,
        )
        print(f"incident {incident_id} opened")
        print("Next: record containment as it happens, and run "
              f"`python -m scripts.incident scope {incident_id}` to size the exposure.")
        return 0

    if args.command == "list":
        rows = list_incidents(status=args.status, limit=args.limit)
        if not rows:
            print("no incidents recorded")
            return 0
        for row in rows:
            print(f"[{row['id']}] {row['detected_at']} {row['severity']:<8} "
                  f"{row['status']:<13} {row['incident_type']:<24} {row.get('summary') or ''}")
        return 0

    if args.command == "show":
        _print(get_incident(args.incident_id))
        return 0

    if args.command == "update":
        fields = {k: v for k, v in vars(args).items()
                  if k not in ("command", "incident_id") and v is not None}
        if not fields:
            print("nothing to update")
            return 1
        update_incident(args.incident_id, **fields)
        print(f"incident {args.incident_id} updated")
        _print(get_incident(args.incident_id))
        return 0

    if args.command == "scope":
        customer_ids = None
        if args.customer_ids:
            customer_ids = [int(x) for x in args.customer_ids.split(",") if x.strip()]
        result = scope_incident(args.incident_id, location_id=args.location_id,
                                customer_ids=customer_ids)
        print(json.dumps(result, indent=2))
        if not result["scope_determined"]:
            print("\nScope could not be determined from the information given. "
                  "That is a real answer -- record it as such rather than estimating.")
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
