from models.integration_models import Subscription
class SubscriptionRepository:
    def get_by_code(self, session, code, location_id=None):
        q = session.query(Subscription).filter(Subscription.paystack_subscription_code == code)
        if location_id is not None: q = q.filter(Subscription.location_id == location_id)
        return q.one_or_none()
    def save(self, session, subscription): session.add(subscription); session.flush(); return subscription
