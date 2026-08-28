"""Tenant-scoped data export.

WHAT THIS IS FOR
----------------
Two things, and they are the same mechanism:

  * A workshop asking for its own data (portability, or simply wanting a copy).
  * The "data export opportunity" step of tenant offboarding, before deletion.

DESIGN: ALLOWLIST, NOT DUMP
---------------------------
Exporting is not "select everything where location_id = X". The location-scoped
tables include integration credentials (encrypted Meta and Google tokens),
OAuth state nonces, raw provider webhook payloads and internal AI telemetry.
None of that is the workshop's business data, and several of those columns
must never leave the system in a file a customer can email around.

So EXPORT_TABLES is an explicit allowlist of table -> columns. A table not
listed is not exported. A column not listed is not exported. Adding a table
here is a deliberate act.

DEFENCE IN DEPTH
----------------
_assert_no_credential_columns() re-checks every column name against a
forbidden pattern set at export time and raises if one matches. The allowlist
should already prevent this; the guard exists because an allowlist is edited
by humans, and a credential silently entering an export is the kind of
mistake that is only discovered afterwards.

TENANT ISOLATION
----------------
Every query filters on location_id, and the raw query layer applies the
request's RLS context, so PostgreSQL enforces the same boundary independently.
The location_id is taken from the authenticated session, never from user
input.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from database import query_db

logger = logging.getLogger(__name__)

# Column-name fragments that must never appear in an export.
FORBIDDEN_COLUMN_PATTERNS = (
    "token", "secret", "password", "nonce", "encrypted",
    "credential", "api_key", "signature",
)

# Explicit allowlist: table -> exported columns.
# Business records the workshop owns. Deliberately excludes every
# meta_*/google_* integration table (credentials and provider internals),
# ai_usage_log and tool_executions (PHANTA telemetry, not workshop data),
# and meta_webhook_events (raw provider payloads).
EXPORT_TABLES = {
    "customers": [
        "id", "first_name", "last_name", "whatsapp_number", "email",
        "notes", "accepts_whatsapp", "marketing_consent_state",
        "marketing_consent_at", "marketing_consent_source",
        "marketing_consent_method", "created_at", "updated_at", "deleted_at",
    ],
    "vehicles": [
        "id", "customer_id", "make", "model", "year", "mileage", "engine",
        "created_at", "updated_at",
    ],
    "bookings": [
        "id", "customer_id", "vehicle_id", "start_time", "end_time",
        "status", "created_at", "updated_at",
    ],
    "service_records": [
        "id", "vehicle_id", "booking_id", "service_type", "performed_at",
        "mileage_at_service", "notes", "created_at",
    ],
    "quotes": [
        "id", "customer_id", "booking_id", "status", "currency",
        "total_amount", "version", "created_at", "updated_at",
    ],
    "quote_line_items": [
        "id", "quote_id", "description", "labour_category", "parts",
        "price", "created_at",
    ],
    # NOTE: invoices has no created_at column -- period_start/period_end are
    # the dates that matter for a billing record.
    "invoices": [
        "id", "amount", "status", "period_start", "period_end",
        "paystack_invoice_id", "failure_reason",
    ],
    "conversations": [
        "id", "customer_id", "channel", "started_at", "ended_at", "created_at",
    ],
    "messages": [
        "id", "conversation_id", "direction", "channel", "body", "status",
        "created_at",
    ],
    "conversation_summaries": [
        "id", "customer_id", "conversation_id", "summary_text", "created_at",
    ],
    "service_rules": [
        "id", "service_type", "interval_km", "interval_months", "make",
        "model", "engine",
    ],
    "legal_acceptances": [
        "id", "document_key", "document_version", "document_label",
        "user_id", "method", "accepted_at",
    ],
}


class ExportError(RuntimeError):
    """Raised when an export cannot be produced safely."""


def _assert_no_credential_columns():
    """Fail loudly if the allowlist has acquired a credential column."""
    for table, columns in EXPORT_TABLES.items():
        for column in columns:
            lowered = column.lower()
            for pattern in FORBIDDEN_COLUMN_PATTERNS:
                if pattern in lowered:
                    raise ExportError(
                        f"refusing to export {table}.{column}: matches forbidden "
                        f"pattern {pattern!r}"
                    )


def build_export(location_id: int) -> dict:
    """Return every allowlisted record for one location.

    `location_id` must come from the authenticated session. Passing a value
    derived from user input would defeat the entire purpose of this function.
    """
    if not isinstance(location_id, int) or location_id <= 0:
        raise ExportError("a valid authenticated location context is required")

    _assert_no_credential_columns()

    export = {
        "export_metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "location_id": location_id,
            "format_version": "1",
            "note": (
                "Business records held by PHANTA on behalf of this workshop. "
                "Integration credentials, provider webhook payloads and PHANTA "
                "internal telemetry are deliberately excluded."
            ),
        },
        "data": {},
        "counts": {},
    }

    for table, columns in EXPORT_TABLES.items():
        column_sql = ", ".join(columns)
        try:
            rows = query_db(
                f"SELECT {column_sql} FROM {table} WHERE location_id = %s",
                (location_id,),
            ) or []
        except Exception:
            # A table that does not exist in this deployment (or lacks a
            # column) must not abort the whole export -- the workshop still
            # gets everything else, and the gap is recorded rather than hidden.
            logger.exception("export_table_failed table=%s location_id=%s", table, location_id)
            export["data"][table] = []
            export["counts"][table] = {"error": "unavailable"}
            continue

        serialisable = [
            {key: (value if _is_json_safe(value) else str(value)) for key, value in row.items()}
            for row in rows
        ]
        export["data"][table] = serialisable
        export["counts"][table] = len(serialisable)

    return export


def _is_json_safe(value) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def export_to_json(location_id: int) -> str:
    """Serialise an export to indented JSON."""
    return json.dumps(build_export(location_id), indent=2, sort_keys=False, default=str)
