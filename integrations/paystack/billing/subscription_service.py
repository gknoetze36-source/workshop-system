from decimal import Decimal, ROUND_HALF_UP
from integrations.paystack.services.paystack_client import PaystackClient
from models.integration_models import Subscription, PaymentCustomer


class SubscriptionService:
    def __init__(self, client: PaystackClient):
        self.client = client

    def create(self, *, customer_code, plan_code):
        return self.client.create_subscription(customer=customer_code, plan=plan_code)

    def cancel(self, *, subscription_code, email_token):
        if not email_token:
            raise ValueError("Paystack email_token is required to cancel a subscription")
        return self.client.disable_subscription(code=subscription_code, email_token=email_token)

    def get(self, subscription_code):
        return self.client.get_subscription(subscription_code)

    def charge_overage(self, *, email, amount, authorization_code):
        value = Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if value <= 0:
            raise ValueError("overage amount must be greater than zero")
        if not authorization_code:
            raise ValueError("authorization_code is required for overage charging")
        return self.client.charge_authorization(email=email, amount_subunits=int(value * 100), authorization_code=authorization_code)

    def create_and_persist(self, session, *, location_id, customer_code, plan_code):
        customer = session.query(PaymentCustomer).filter_by(
            location_id=location_id, paystack_customer_code=customer_code
        ).one_or_none()
        if customer is None:
            raise ValueError("Paystack customer does not belong to the requested location")
        data = self.create(customer_code=customer_code, plan_code=plan_code)
        record = Subscription(
            location_id=location_id,
            paystack_subscription_code=data["subscription_code"],
            paystack_email_token=data.get("email_token"),
            plan_code=plan_code,
            status=data.get("status", "active"),
        )
        session.add(record)
        session.flush()
        return record, data
