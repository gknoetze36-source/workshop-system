"""Keep Instagram Login tokens alive.

They last 60 days and Meta only refreshes a token that is at least 24 hours
old, so each connection is refreshed once a week; one missed run costs
nothing. Runs per location inside its RLS scope, like every other job.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select

from database import SessionLocal, set_location_id
from models.core import Location
from models.integration_models import InstagramConnection

REFRESH_AFTER = timedelta(days=7)


def run_instagram_token_refresh(*, now: datetime | None = None, client=None) -> list[dict]:
    from services.integration_status import integration_status
    if not integration_status("instagram")["configured"]:
        return []
    from integrations.instagram.api_client import InstagramAPIError, InstagramLoginClient
    from integrations.instagram.config import InstagramAuthConfig
    from integrations.instagram.tokens import get_token, save_token
    now = now or datetime.now(timezone.utc)
    client = client or InstagramLoginClient(InstagramAuthConfig.from_env())

    session = SessionLocal()
    try:
        location_ids = list(session.scalars(
            select(Location.id).where(Location.active.is_(True), Location.access_locked.is_(False))))
    finally:
        session.close()

    results = []
    for location_id in location_ids:
        db = SessionLocal()
        try:
            set_location_id(db, location_id)
            connection = db.scalar(select(InstagramConnection).where(
                InstagramConnection.location_id == location_id,
                InstagramConnection.connection_status == "connected",
                or_(InstagramConnection.token_refreshed_at.is_(None),
                    InstagramConnection.token_refreshed_at <= now - REFRESH_AFTER)))
            if connection is None:
                continue
            try:
                save_token(connection, client.refresh_long_lived_token(get_token(connection)))
                results.append({"location_id": location_id, "status": "refreshed"})
            except InstagramAPIError as exc:
                if exc.status_code in (401, 403):  # 401 = expired/revoked (Meta code 190)
                    connection.connection_status = "reconnect_required"
                results.append({"location_id": location_id, "status": "error", "error": str(exc)})
            db.commit()
        except Exception as exc:
            db.rollback()
            results.append({"location_id": location_id, "status": "error", "error": str(exc)})
        finally:
            db.close()
    return results
