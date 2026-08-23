from helpers.permission import assert_location_scope, available_roles_for_creator


def test_platform_admin_can_scope_any_location():
    assert assert_location_scope({"location_id": 2}, {"role": "super_admin"}) is True


def test_owner_can_scope_only_own_location():
    user = {"role": "owner", "owner_id": 10, "location_id": 1}
    assert assert_location_scope({"location_id": 1}, user) is True
    assert assert_location_scope({"location_id": 2}, user) is False


def test_missing_location_is_denied_for_business_user():
    assert assert_location_scope({"location_id": 1}, {"role": "owner"}) is False
    assert assert_location_scope({"location_id": 1}, {"role": "owner", "location_id": None}) is False


def test_missing_row_is_denied():
    assert assert_location_scope(None, {"role": "owner", "location_id": 1}) is False


def test_allowed_roles_for_location_creator():
    roles = available_roles_for_creator({"role": "owner", "location_id": 1})
    assert roles == ["manager", "reception", "technician", "readonly"]


def test_platform_creator_can_create_supported_roles():
    roles = available_roles_for_creator({"role": "platform_admin"})
    assert roles == ["owner", "admin", "manager", "reception", "technician", "readonly"]


def test_operational_users_cannot_create_users():
    assert available_roles_for_creator({"role": "reception", "location_id": 1}) == []
