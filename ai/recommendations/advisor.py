from __future__ import annotations

from ai.recommendations.rule_engine import ServiceRuleEngine


class ServiceRecommendationAdvisor:
    """Presentation layer over the deterministic service rule engine.

    This class deliberately contains no maintenance logic. The rule engine is
    the source of truth; the LLM can use its structured result to explain the
    recommendation in the Service Advisor's voice.
    """

    def __init__(self, session, location_id: int):
        self.engine = ServiceRuleEngine(session, location_id)

    def recommend(self, vehicle_id: int) -> dict:
        return self.engine.due_services(vehicle_id)

    @staticmethod
    def explanation_context(result: dict) -> list[str]:
        lines = []
        for item in result.get("due_services", []):
            status = item["status"]
            service = item["service_type"].replace("_", " ")
            if item.get("due_mileage"):
                lines.append(f"{status}: {service} at approximately {item['due_mileage']:,} km")
            elif item.get("due_date"):
                lines.append(f"{status}: {service} by {item['due_date'][:10]}")
            else:
                lines.append(f"{status}: {service}")
        return lines
