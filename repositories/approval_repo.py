from sqlalchemy.orm import Session
from models.core import Approval

class ApprovalRepository:
    def __init__(self, session: Session):
        self.session = session

    def record(self, quote_line_item_id: int, decision: str, decided_by: str, raw_message: str, channel: str):
        # Approvals are append-only by design. No update/delete method is exposed.
        approval = Approval(
            quote_line_item_id=quote_line_item_id,
            decision=decision,
            decided_by=decided_by,
            raw_message=raw_message,
            channel=channel,
        )
        self.session.add(approval)
        self.session.flush()
        return approval
