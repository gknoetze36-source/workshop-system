from __future__ import annotations

class AIContextBuilder:
    """Build lean, explicit, location-scoped context for one AI turn."""
    def build(self, *, owner=None, location=None, customer=None, vehicles=None,
              bookings=None, summary: str | None = None, identity_kind: str | None = None,
              last_vehicle_id: int | None = None) -> dict:
        return {
            "owner": owner, "location": location, "customer": customer,
            "customer_type": identity_kind, "last_vehicle_id": last_vehicle_id,
            "vehicles": vehicles or [], "open_bookings": bookings or [],
            "conversation_summary": summary,
        }
