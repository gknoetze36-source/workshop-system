from __future__ import annotations
from datetime import datetime, timedelta, timezone
from integrations.paystack.payments.transaction_service import TransactionService
from integrations.paystack.repositories.payment_repo import PaymentRepository


class PaystackReconciliationService:
    """Verify unresolved transactions as a backstop for missed webhooks."""
    def __init__(self, transaction_service: TransactionService, payment_repo: PaymentRepository | None = None):
        self.transaction_service = transaction_service
        self.payment_repo = payment_repo or PaymentRepository()

    def reconcile(self, session, *, older_than_minutes: int = 15, limit: int = 100, location_id: int | None = None):
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=older_than_minutes)
        payments = self.payment_repo.list_unresolved(session, older_than=cutoff, location_id=location_id)[:limit]
        results = []
        for payment in payments:
            try:
                updated, success = self.transaction_service.verify_and_reconcile(session, reference=payment.reference)
                results.append({"reference": payment.reference, "status": updated.status if updated else None, "success": success})
            except Exception as exc:
                results.append({"reference": payment.reference, "status": "error", "success": False, "error": str(exc)})
        return results
