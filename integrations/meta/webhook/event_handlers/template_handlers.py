from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy.orm import Session

from models.integration_models import MetaAuditLog
from ...messaging.template_repository import MetaTemplateRepository


class MetaTemplateHandlers:
    def __init__(self, session: Session):
        self.session = session
        self.templates = MetaTemplateRepository()

    def handle(self, *, location_id: int | None, payload: dict) -> dict:
        event = str(payload.get("event") or payload.get("status") or "UNKNOWN").upper()
        template_id = payload.get("message_template_id")
        name = payload.get("message_template_name") or payload.get("name")
        language = payload.get("language") or payload.get("language_code") or "en_ZA"
        category = payload.get("category") or "UTILITY"
        reason = payload.get("reason")

        template_id_text = str(template_id) if template_id is not None else None
        if location_id is not None and name:
            obj = self.templates.upsert(
                self.session,
                location_id=location_id,
                waba_id=payload.get("waba_id"),
                name=str(name),
                language=str(language),
                category=str(category),
                status=event,
                meta_template_id=template_id_text,
                reason=str(reason) if reason is not None else None,
                components=payload.get("components"),
            )
            template_id_db = obj.id
        else:
            template_id_db = None

        log = MetaAuditLog(
            location_id=location_id,
            action="meta_template_status_update",
            details=payload,
            created_at=datetime.now(timezone.utc),
        )
        self.session.add(log)
        self.session.flush()
        return {
            "recorded": True,
            "audit_log_id": log.id,
            "template_id": template_id_db,
            "event": event,
        }
