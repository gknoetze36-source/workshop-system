"""Scheduled Paystack reconciliation entry point.

Payments are location-owned. The scheduler discovers active locations and
reconciles only unresolved payments belonging to the current location scope.
"""
from __future__ import annotations

import logging

from sqlalchemy import select

from database import SessionLocal, set_location_id
from models.core import Location
from integrations.paystack.payments.transaction_service import TransactionService
from integrations.paystack.services.paystack_client import PaystackClient
from integrations.paystack.reconciliation_service import PaystackReconciliationService

logger = logging.getLogger(__name__)


def run_paystack_reconciliation(*, older_than_minutes: int = 15, limit: int = 100):
    """Reconcile unresolved Paystack transactions for every active location."""
    discovery = SessionLocal()
    try:
        location_ids = list(discovery.scalars(select(Location.id).where(Location.active.is_(True))))
    finally:
        discovery.close()

    results = []
    for location_id in location_ids:
        session = SessionLocal()
        try:
            set_location_id(session, location_id)
            service = PaystackReconciliationService(
                TransactionService(PaystackClient())
            )
            location_results = service.reconcile(
                session,
                older_than_minutes=older_than_minutes,
                limit=limit,
                location_id=location_id,
            )
            session.commit()
            results.append({"location_id": location_id, "status": "ok", "payments": location_results})
        except Exception as exc:
            session.rollback()
            logger.exception("Paystack reconciliation failed for location %s", location_id)
            from observability import capture_exception
            capture_exception(exc)
            results.append({"location_id": location_id, "status": "error", "error": str(exc)})
        finally:
            session.close()

    return results
