"""Scheduled Phase 6 Meta token health monitor.

Run this job from Railway cron or the existing scheduler process. It resolves
active location IDs first, then executes each health check inside that location's
RLS transaction so background jobs never bypass location isolation.
"""
from __future__ import annotations

from sqlalchemy import select

from database import SessionLocal, set_location_id
from models.core import Location
from integrations.meta.services.token_status_service import MetaTokenStatusService


def run_meta_token_monitor() -> list[dict]:
    session = SessionLocal()
    try:
        location_ids = list(session.scalars(select(Location.id).where(Location.active.is_(True))))
    finally:
        session.close()

    results = []
    for location_id in location_ids:
        location_session = SessionLocal()
        try:
            set_location_id(location_session, location_id)
            health = MetaTokenStatusService().monitor_location(location_session, location_id)
            location_session.commit()
            results.append({
                "location_id": location_id,
                "status": health.status,
                "healthy": health.healthy,
                "reconnect_required": health.reconnect_required,
            })
        except Exception as exc:
            location_session.rollback()
            results.append({"location_id": location_id, "status": "monitor_error", "healthy": False, "error": str(exc)})
        finally:
            location_session.close()
    return results
