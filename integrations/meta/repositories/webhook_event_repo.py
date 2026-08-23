"""Persistence and idempotency helpers for Meta webhook events."""
from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from models.integration_models import MetaWebhookEvent


class MetaWebhookEventRepository:
    def get_by_external_id(self, session: Session, external_event_id: str) -> MetaWebhookEvent | None:
        if not external_event_id:
            return None
        return session.scalar(
            select(MetaWebhookEvent).where(MetaWebhookEvent.external_event_id == external_event_id)
        )

    def record(
        self,
        session: Session,
        *,
        location_id: int | None,
        waba_id: str | None,
        phone_number_id: str | None,
        external_event_id: str,
        event_type: str,
        payload: dict,
        signature_valid: bool = True,
    ) -> tuple[MetaWebhookEvent, bool]:
        """Return (event, is_new). The unique DB key is the final idempotency guard."""
        existing = self.get_by_external_id(session, external_event_id)
        if existing:
            return existing, False

        event = MetaWebhookEvent(
            location_id=location_id,
            waba_id=waba_id,
            phone_number_id=phone_number_id,
            external_event_id=external_event_id,
            event_type=event_type,
            payload=payload,
            signature_valid=signature_valid,
            processing_status="received",
        )
        session.add(event)
        try:
            with session.begin_nested():
                session.flush()
            return event, True
        except IntegrityError:
            existing = self.get_by_external_id(session, external_event_id)
            if existing:
                return existing, False
            raise

    def mark_processed(self, session: Session, event: MetaWebhookEvent) -> None:
        event.processing_status = "processed"
        event.processed_at = datetime.now(timezone.utc)
        session.flush()

    def mark_failed(self, session: Session, event: MetaWebhookEvent) -> None:
        event.processing_status = "failed"
        session.flush()
