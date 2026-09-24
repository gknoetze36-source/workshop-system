"""Phase 17 deterministic follow-up worker."""
from __future__ import annotations

from sqlalchemy import select

from database import SessionLocal, set_location_id
from models.core import Location
from ai.follow_up.service import DeterministicFollowUpService
from integrations.meta.auth.capability_config import WhatsAppMetaConfig
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.messaging.messaging_service import MetaMessagingService
from integrations.meta.services.graph_api_client import GraphApiClient


def run_follow_up_worker() -> list[dict]:
    session = SessionLocal()
    try:
        # access_locked excludes locations the payment wall has shut off for
        # non-payment. Location.active alone does not -- lock_location()
        # (services/access_lock_service.py) only ever sets access_locked; it
        # never touches active. Without this, a locked-out, non-paying
        # workshop's customers would keep receiving real WhatsApp messages
        # through PHANTA's own Meta API credentials indefinitely, the one
        # channel the payment wall (services/auth_service.py, enforced on
        # every authenticated route) has no reach into because this job has
        # no request/session context to check against.
        location_ids = list(session.scalars(
            select(Location.id).where(Location.active.is_(True), Location.access_locked.is_(False))
        ))
    finally:
        session.close()

    results = []
    for location_id in location_ids:
        location_session = SessionLocal()
        try:
            set_location_id(location_session, location_id)
            messaging = MetaMessagingService(
                location_session,
                graph=GraphApiClient(WhatsAppMetaConfig.from_env()),
                token_store=MetaTokenStore(),
            )
            service = DeterministicFollowUpService(location_session, messaging)
            seeded = service.seed_due_followups(location_id)
            sent = service.process_due(location_id)
            location_session.commit()
            results.append({
                "location_id": location_id,
                "seeded_followups": [item.id for item in seeded],
                "sent_followups": sent,
            })
        except Exception as exc:
            location_session.rollback()
            results.append({"location_id": location_id, "status": "error", "error": str(exc)})
        finally:
            location_session.close()
    return results
