"""Creating a special from the Flyer Lady page (was a 500: bad get_location_by_id call)."""
import re

from tests.unit.test_settings_permission_boundaries import client_with_roles  # noqa: F401  (fixture)


def _post(ctx, location_id):
    page = ctx["client"].get("/customers/manual-booking").get_data(as_text=True)
    token = re.search(r'name="csrf-token" content="([^"]+)"', page).group(1)
    return ctx["client"].post("/dashboard/flyer-lady/specials", json={"location_id": location_id, "text": "R999 major service this week"},
                              headers={"X-CSRFToken": token})


def test_owner_creates_special_for_own_workshop(client_with_roles):
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    resp = _post(ctx, ctx["location_id"])
    assert resp.status_code == 201, resp.get_data(as_text=True)
    assert resp.get_json()["status"] == "draft" and resp.get_json()["booking_link"]


def test_cannot_create_special_for_another_workshop(client_with_roles):
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    assert _post(ctx, ctx["location_id"] + 999).status_code == 403
