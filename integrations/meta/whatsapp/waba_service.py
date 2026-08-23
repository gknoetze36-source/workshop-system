"""Phase 7 WABA-level operations."""
from __future__ import annotations

from typing import Any
from sqlalchemy.orm import Session

from .phone_number_service import PhoneNumberService


class WABAService:
    """Thin WABA service kept separate from phone registration concerns."""

    def __init__(self, phone_service: PhoneNumberService | None = None):
        self.phone_service = phone_service or PhoneNumberService()

    def get_info(self, session: Session, location_id: int) -> dict[str, Any]:
        return self.phone_service.waba_info(session, location_id)
