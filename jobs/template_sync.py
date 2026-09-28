"""Mirror each connected location's WhatsApp template statuses from Meta.

Status webhooks keep the mirror current, but an approval Meta announced while
webhooks were failing is never re-sent. This pull closes that gap, reusing the
onboarding sync (MetaTemplateSyncService), one location per RLS transaction.
"""
from __future__ import annotations

from sqlalchemy import select

from database import SessionLocal, set_location_id
from models.core import Location
from models.integration_models import MetaBusinessConnection
from integrations.meta.auth.capability_config import WhatsAppMetaConfig
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.business.tech_provider_operations import TechProviderOperations
from integrations.meta.services.graph_api_client import GraphApiClient
from integrations.meta.whatsapp.template_sync_service import MetaTemplateSyncService


def run_template_sync() -> list[dict]:
    # ponytail: one Graph call per connected location every scheduler tick;
    # move to an hourly cron if the location count grows large.
    session = SessionLocal()
    try:
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
            connection = location_session.scalar(select(MetaBusinessConnection).where(
                MetaBusinessConnection.location_id == location_id,
                MetaBusinessConnection.connection_status == "connected",
            ))
            if connection is None or not connection.waba_id or not connection.encrypted_access_token:
                continue
            # iter_message_templates only uses the Graph client; no provider config needed.
            operations = TechProviderOperations(GraphApiClient(WhatsAppMetaConfig.from_env()), provider=None)
            result = MetaTemplateSyncService(operations).sync(
                location_session, location_id=location_id, waba_id=connection.waba_id,
                customer_token=MetaTokenStore().get_customer_token(connection),
            )
            location_session.commit()
            results.append({"location_id": location_id, "synced": result.synced, "skipped": result.skipped})
        except Exception as exc:
            location_session.rollback()
            results.append({"location_id": location_id, "status": "error", "error": str(exc)})
        finally:
            location_session.close()
    return results
