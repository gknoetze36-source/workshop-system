import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / "app" / "core" / "domain" / "automation" / "catalog.py"
spec = importlib.util.spec_from_file_location("phanta_step9_catalog", CATALOG)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
WORKFLOW_DEFINITIONS = module.WORKFLOW_DEFINITIONS
workflows_for_industry = module.workflows_for_industry


def test_workflow_catalog_preserves_all_existing_industry_definitions():
    assert len(WORKFLOW_DEFINITIONS) == 40
    assert len(workflows_for_industry("workshop")) == 8
    assert len(workflows_for_industry("salon")) == 4


def test_industry_catalog_never_returns_other_industry_workflows():
    for industry in {w.industry for w in WORKFLOW_DEFINITIONS}:
        assert all(w.industry == industry for w in workflows_for_industry(industry))


def test_universal_repository_is_location_scoped():
    text = (ROOT / "repositories" / "automation_repository.py").read_text()
    assert "ar.location_id = %s" in text
    assert "automation_location_id" in text


def test_migration_makes_runtime_automation_records_location_owned():
    text = (ROOT / "migrations" / "versions" / "0020_automation_location_ownership.py").read_text()
    for table in ("automation_rules", "scheduled_jobs", "automation_logs", "failed_jobs"):
        assert table in text
    assert "nullable=False" in text
    assert "create_foreign_key" in text
