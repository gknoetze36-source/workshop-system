from sqlalchemy import select
from sqlalchemy.orm import Session
from .models import Special, SpecialApproval

class SpecialApprovalService:
    def decide(self, session: Session, location_id: int, special_id: int, decision: str, decided_by: str):
        if decision not in {"approved", "rejected"}: raise ValueError("decision must be approved or rejected")
        special = session.scalar(select(Special).where(Special.id == special_id, Special.location_id == location_id))
        if not special: raise ValueError("special not found")
        approval = SpecialApproval(location_id=location_id, special_id=special_id, decision=decision, decided_by=decided_by)
        session.add(approval); special.status = decision; session.flush(); return approval
    def is_approved(self, session: Session, location_id: int, special_id: int) -> bool:
        return session.scalar(select(SpecialApproval.id).where(SpecialApproval.location_id == location_id, SpecialApproval.special_id == special_id, SpecialApproval.decision == "approved").order_by(SpecialApproval.decided_at.desc())) is not None
