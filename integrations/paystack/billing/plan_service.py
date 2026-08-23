from decimal import Decimal, ROUND_HALF_UP
from integrations.paystack.services.paystack_client import PaystackClient
from models.integration_models import Plan


class PlanService:
    def __init__(self, client: PaystackClient):
        self.client = client

    @staticmethod
    def to_subunits(amount):
        value = Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if value <= 0:
            raise ValueError("amount must be greater than zero")
        return int(value * 100)

    def create(self, *, name, amount, interval="monthly", invoice_limit=None):
        return self.client.create_plan(name=name, amount_subunits=self.to_subunits(amount), interval=interval, invoice_limit=invoice_limit)

    def create_and_persist(self, session, *, name, amount, interval="monthly", invoice_limit=None):
        data = self.create(name=name, amount=amount, interval=interval, invoice_limit=invoice_limit)
        record = Plan(paystack_plan_code=data["plan_code"], name=name, amount=Decimal(str(amount)), interval=interval, invoice_limit=invoice_limit)
        session.add(record)
        session.flush()
        return record, data
