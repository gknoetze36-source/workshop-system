"""Initial message-template synchronisation for a newly onboarded WABA.

This does not replace the existing template systems. Meta remains the source
of truth; ``message_template_status_update`` webhooks keep PHANTA's mirror
current after onboarding. This service only performs the one-time initial
pull, because a WABA that existed before it was connected to PHANTA already
has approved templates that no webhook will ever re-announce.

Storage goes through the existing :class:`MetaTemplateRepository`, whose
upsert key is ``(location_id, name, language)``. Every write is therefore
scoped to the onboarding location and cannot touch another tenant's rows.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from ..messaging.template_repository import MetaTemplateRepository


@dataclass(frozen=True)
class TemplateSyncResult:
    location_id: int
    waba_id: str
    synced: int
    skipped: int


class MetaTemplateSyncService:
    def __init__(self, operations, repository: MetaTemplateRepository | None = None):
        self.operations = operations
        self.repository = repository or MetaTemplateRepository()

    def sync(
        self,
        session: Session,
        *,
        location_id: int,
        waba_id: str,
        customer_token: str,
    ) -> TemplateSyncResult:
        """Pull every template on ``waba_id`` and mirror it for ``location_id``.

        Safe to rerun: each template is upserted by name+language within the
        location, so a second run updates rather than duplicating. A WABA with
        no templates yet is a valid, documented outcome (a brand-new WABA has
        none), and returns ``synced=0`` rather than failing.
        """
        synced = 0
        skipped = 0
        for template in self.operations.iter_message_templates(customer_token, waba_id):
            name = template.get("name")
            language = template.get("language")
            if not name or not language:
                # Meta always returns both; anything else is malformed and is
                # counted rather than written, so a bad row cannot poison the
                # tenant's template table.
                skipped += 1
                continue
            self.repository.upsert(
                session,
                location_id=location_id,
                waba_id=str(waba_id),
                name=str(name),
                language=str(language),
                category=str(template.get("category") or "UTILITY"),
                status=str(template.get("status") or "PENDING"),
                meta_template_id=self._template_id(template),
                reason=self._reason(template),
                components=self._components(template),
            )
            synced += 1
        session.flush()
        return TemplateSyncResult(int(location_id), str(waba_id), synced, skipped)

    @staticmethod
    def _template_id(template: dict[str, Any]) -> str | None:
        value = template.get("id")
        return str(value) if value is not None else None

    @staticmethod
    def _reason(template: dict[str, Any]) -> str | None:
        value = template.get("reason")
        return str(value) if value is not None else None

    @staticmethod
    def _components(template: dict[str, Any]) -> dict[str, Any] | None:
        components = template.get("components")
        if components is None:
            return None
        # The model column is JSON; Meta returns a list of component objects.
        # Wrap so the stored shape is stable regardless of Meta's top-level type.
        if isinstance(components, list):
            return {"components": components}
        if isinstance(components, dict):
            return components
        return None
