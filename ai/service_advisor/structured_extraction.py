"""Structured schemas used by the Phase 12 Service Advisor."""
from __future__ import annotations

CUSTOMER_CONTEXT_SCHEMA = {
    "type": "object",
    "properties": {
        "first_name": {"type": "string"},
        "last_name": {"type": "string"},
        "vehicle_id": {"type": ["integer", "null"]},
        "make": {"type": ["string", "null"]},
        "model": {"type": ["string", "null"]},
        "year": {"type": ["integer", "null"]},
        "mileage": {"type": ["integer", "null"]},
        "problem": {"type": ["string", "null"]},
        "urgency": {
            "type": ["string", "null"],
            "enum": ["routine", "soon", "urgent", "unsafe_to_drive", None],
        },
    },
    "required": [
        "first_name", "last_name", "vehicle_id", "make", "model",
        "year", "mileage", "problem", "urgency",
    ],
    "additionalProperties": False,
}
