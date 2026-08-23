from pathlib import Path

from services.industry import get_industry_profile
from models.core import Location, Customer, Booking


def test_location_is_canonical_business_scope():
    assert Location.__tablename__ == "locations"
    assert Location.owner_id.property.columns[0].nullable is False
    assert Location.owner_id.property.columns[0].unique is True
    assert hasattr(Customer, "location_id")
    assert hasattr(Booking, "location_id")


def test_industry_profiles_are_selected_explicitly():
    workshop = get_industry_profile("workshop")
    salon = get_industry_profile("salon")
    assert workshop["subject"] == "vehicle"
    assert salon["subject"] is None
    assert workshop["default_services"] != salon["default_services"]
    assert "service_advisor" in workshop["workflows"]
    assert "service_advisor" not in salon["workflows"]


def test_runtime_sources_do_not_use_legacy_scope_terms():
    root = Path(__file__).resolve().parents[2]
    ignored = {"migrations", "tests", "__pycache__"}
    legacy = []
    for path in root.rglob("*.py"):
        rel = path.relative_to(root)
        if any(part in ignored for part in rel.parts):
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for term in ("franchise_id", "branch_id", "tenant_id", "class Tenant"):
            if term in text:
                legacy.append((str(rel), term))
    assert legacy == []
