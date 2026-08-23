
from services import location_provisioning_service as provisioning


def test_provision_owner_location_rejects_missing_owner(monkeypatch):
    monkeypatch.setattr(provisioning, "query_db", lambda *args, **kwargs: None)
    result = provisioning.provision_owner_location(1, "Test Location", "workshop")
    assert result == {"ok": False, "error": "owner not found or inactive"}


def test_provision_owner_location_rejects_second_location(monkeypatch):
    calls = iter([
        {"id": 7, "user_id": 12, "name": "Owner", "email": "owner@example.com", "active": True},
        {"id": 44},
    ])
    monkeypatch.setattr(provisioning, "query_db", lambda *args, **kwargs: next(calls))
    result = provisioning.provision_owner_location(7, "Second", "salon")
    assert result == {"ok": False, "error": "owner already has a location"}


def test_provision_owner_location_creates_and_binds_location(monkeypatch):
    calls = iter([
        {"id": 7, "user_id": 12, "name": "Owner", "email": "owner@example.com", "active": True},
        None,  # no existing location
        None,  # slug is available
        {"id": 44},  # created location
    ])
    executed = []
    monkeypatch.setattr(provisioning, "query_db", lambda *args, **kwargs: next(calls))
    monkeypatch.setattr(provisioning, "execute_db", lambda *args, **kwargs: executed.append((args, kwargs)))
    monkeypatch.setattr(provisioning, "utc_now", lambda: "2026-01-01T00:00:00Z")

    result = provisioning.provision_owner_location(7, "Pretoria Workshop", "workshop")

    assert result == {
        "ok": True, "location_id": 44, "owner_id": 7, "industry": "workshop"
    }
    assert len(executed) == 2
    assert "INSERT INTO locations" in executed[0][0][0]
    assert "UPDATE users" in executed[1][0][0]
