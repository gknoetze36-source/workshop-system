from __future__ import annotations
from integrations.paystack.services.paystack_client import PaystackClient
from models.integration_models import PaymentCustomer


class PaystackCustomerService:
    def __init__(self, client: PaystackClient):
        self.client = client

    def create(self, *, email, first_name=None, last_name=None, phone=None, location_id=None, phanta_customer_id=None):
        if location_id is None:
            raise ValueError("location_id is required")
        metadata = {"phanta_location_id": location_id}
        if phanta_customer_id is not None:
            metadata["phanta_customer_id"] = phanta_customer_id
        return self.client.create_customer(email=email, first_name=first_name, last_name=last_name, phone=phone, metadata=metadata)

    def create_and_persist(self, session, *, email, location_id, first_name=None, last_name=None, phone=None, phanta_customer_id=None):
        existing = None
        if phanta_customer_id is not None:
            existing = session.query(PaymentCustomer).filter_by(location_id=location_id, phanta_customer_id=phanta_customer_id).one_or_none()
        if existing:
            return existing, None
        data = self.create(email=email, first_name=first_name, last_name=last_name, phone=phone, location_id=location_id, phanta_customer_id=phanta_customer_id)
        record = PaymentCustomer(
            location_id=location_id,
            phanta_customer_id=phanta_customer_id,
            paystack_customer_code=data["customer_code"],
            email=email,
        )
        session.add(record)
        session.flush()
        return record, data
