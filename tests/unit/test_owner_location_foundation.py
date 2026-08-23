from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _active_python_files():
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith("migrations/") or rel.startswith("tests/") or rel == "database/owner_location.py":
            continue
        if "__pycache__" in rel:
            continue
        yield path


def test_active_code_has_no_franchise_or_branch_id_scope():
    offenders = []
    for path in _active_python_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "franchise_id" in text or "branch_id" in text:
            offenders.append(str(path.relative_to(ROOT)))
    assert not offenders, offenders


def test_owner_location_schema_is_one_to_one():
    text = (ROOT / "database" / "owner_location.py").read_text(encoding="utf-8")
    assert "owner_id INTEGER UNIQUE" in text
    assert "owner_id INTEGER UNIQUE" in text
    assert 'CREATE TABLE IF NOT EXISTS owners' in text
    assert 'CREATE TABLE IF NOT EXISTS locations' in text


def test_auth_session_contains_owner_and_location_scope():
    text = (ROOT / "services" / "auth_service.py").read_text(encoding="utf-8")
    assert 'session_user["owner_id"]' in text
    assert 'session_user["location_id"]' in text
    assert 'active_location_required' in text
