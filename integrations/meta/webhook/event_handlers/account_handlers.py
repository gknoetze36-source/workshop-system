from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session
from models.integration_models import MetaBusinessConnection, MetaAuditLog


class MetaAccountHandlers:
    def __init__(self, session: Session):
        self.session = session

    def handle(self, *, location_id: int | None, value: dict) -> dict:
        waba_id = value.get("waba_id") or value.get("id")
        business_id = value.get("business_id") or value.get("business")
        phone_number_id = value.get("phone_number_id")
        connection = None
        if location_id is not None:
            connection = self.session.scalar(select(MetaBusinessConnection).where(MetaBusinessConnection.location_id == location_id))
        if connection is not None:
            if waba_id:
                connection.waba_id = str(waba_id)
            if business_id:
                connection.business_id = str(business_id)
            if phone_number_id:
                connection.phone_number_id = str(phone_number_id)
            # An account_update webhook reports a change to the WABA. It is
            # not evidence that Tech Provider onboarding finished, so it must
            # not promote the connection. Doing so previously meant Meta's
            # PARTNER_ADDED notification alone could mark a workshop connected
            # with no System User access, no credit line and no verified
            # webhook subscription. Only the onboarding orchestrator sets
            # connection_status='connected'.
        log = MetaAuditLog(location_id=location_id, action="meta_account_update", details=value)
        self.session.add(log)
        self.session.flush()
        return {"updated": connection is not None, "audit_log_id": log.id}
