from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
import uuid

from integrations.paystack.services.paystack_client import PaystackClient
from models.integration_models import Payment


class TransactionService:
    def __init__(self, client: PaystackClient):
        self.client = client

    @staticmethod
    def to_subunits(amount: Decimal | float | int | str) -> int:
        value = Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if value <= 0:
            raise ValueError("amount must be greater than zero")
        return int(value * 100)

    def initialize(
        self,
        *,
        email: str,
        amount: Decimal | float | int | str,
        location_id: int,
        callback_url: str | None = None,
        metadata: dict | None = None,
        plan: str | None = None,
        reference: str | None = None,
    ):
        ref = reference or f"phanta_{location_id}_{uuid.uuid4().hex}"
        meta = {"phanta_location_id": location_id, **(metadata or {})}
        return ref, self.client.initialize_transaction(
            email=email,
            amount_subunits=self.to_subunits(amount),
            reference=ref,
            callback_url=callback_url,
            metadata=meta,
            plan=plan,
        )

    def initialize_and_persist(self, session, *, email: str, amount, location_id: int, callback_url=None, metadata=None, plan=None, reference=None):
        ref = reference or f"phanta_{location_id}_{uuid.uuid4().hex}"
        existing = session.query(Payment).filter(Payment.reference == ref).one_or_none()
        if existing:
            return existing, None
        _, data = self.initialize(
            email=email, amount=amount, location_id=location_id, callback_url=callback_url,
            metadata=metadata, plan=plan, reference=ref,
        )
        payment = Payment(
            location_id=location_id,
            reference=ref,
            paystack_transaction_id=str(data.get("id")) if data.get("id") is not None else None,
            amount=Decimal(str(amount)).quantize(Decimal("0.01")),
            currency=str(data.get("currency") or "ZAR").upper(),
            status="initialized",
            metadata_json={"authorization_url": data.get("authorization_url"), **(metadata or {})},
        )
        session.add(payment)
        session.flush()
        return payment, data

    def verify(self, *, reference: str, expected_amount: Decimal | float | int | str, expected_currency: str = "ZAR"):
        data = self.client.verify_transaction(reference)
        if data.get("status") != "success":
            return data, False
        if int(data.get("amount", -1)) != self.to_subunits(expected_amount):
            raise ValueError("verified Paystack amount does not match expected amount")
        if str(data.get("currency", "")).upper() != expected_currency.upper():
            raise ValueError("verified Paystack currency does not match expected currency")
        return data, True

    def verify_and_reconcile(self, session, *, reference: str, location_id: int | None = None):
        query = session.query(Payment).filter(Payment.reference == reference)
        if location_id is not None:
            query = query.filter(Payment.location_id == location_id)
        payment = query.one_or_none()
        if not payment:
            return None, False
        data, success = self.verify(reference=reference, expected_amount=payment.amount, expected_currency=payment.currency)
        if success:
            payment.status = "success"
            payment.paystack_transaction_id = str(data.get("id")) if data.get("id") is not None else payment.paystack_transaction_id
            payment.gateway_response = data.get("gateway_response")
            payment.channel = data.get("channel")
        elif data.get("status") in {"failed", "abandoned"}:
            payment.status = str(data.get("status"))
            payment.gateway_response = data.get("gateway_response")
        session.flush()
        return payment, success

    def refund(self, *, reference: str, amount=None):
        return self.client.refund(transaction=reference, amount_subunits=self.to_subunits(amount) if amount is not None else None)
