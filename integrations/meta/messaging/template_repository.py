"""Repository for PHANTA's mirrored Meta template state."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from models.integration_models import MetaMessageTemplate


class MetaTemplateRepository:
    def get(self, session: Session, *, location_id: int, name: str, language: str) -> MetaMessageTemplate | None:
        return session.scalar(
            select(MetaMessageTemplate).where(
                MetaMessageTemplate.location_id == location_id,
                MetaMessageTemplate.name == name,
                MetaMessageTemplate.language == language,
            )
        )

    def upsert(
        self,
        session: Session,
        *,
        location_id: int,
        waba_id: str | None,
        name: str,
        language: str,
        category: str = "UTILITY",
        status: str = "PENDING",
        meta_template_id: str | None = None,
        reason: str | None = None,
        components: dict | None = None,
    ) -> MetaMessageTemplate:
        obj = self.get(session, location_id=location_id, name=name, language=language)
        if obj is None:
            obj = MetaMessageTemplate(
                location_id=location_id, waba_id=waba_id, name=name, language=language,
                category=category.upper(), status=status.upper()
            )
            session.add(obj)
        obj.waba_id = waba_id or obj.waba_id
        obj.category = category.upper() if category else obj.category
        obj.status = status.upper() if status else obj.status
        obj.meta_template_id = meta_template_id or obj.meta_template_id
        obj.reason = reason
        if components is not None:
            obj.components_json = components
        session.flush()
        return obj
