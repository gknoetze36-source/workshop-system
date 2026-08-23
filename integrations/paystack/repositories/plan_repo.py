from models.integration_models import Plan
class PlanRepository:
    def get_by_code(self, session, code): return session.query(Plan).filter(Plan.paystack_plan_code == code).one_or_none()
    def save(self, session, plan): session.add(plan); session.flush(); return plan
