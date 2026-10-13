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


def test_cancel_removes_unpublished_posts_and_allows_requeue(client_with_roles):
    from database import execute_db, query_db
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    special_id = _post(ctx, ctx["location_id"]).get_json()["id"]
    page = ctx["client"].get("/customers/manual-booking").get_data(as_text=True)
    headers = {"X-CSRFToken": re.search(r'name="csrf-token" content="([^"]+)"', page).group(1)}
    base = f"/dashboard/flyer-lady/specials/{special_id}"
    assert ctx["client"].post(f"{base}/approval", json={"decision": "approved"}, headers=headers).status_code == 200
    assert ctx["client"].post(f"{base}/queue", json={"platforms": ["facebook_feed", "whatsapp_status_prepared"]}, headers=headers).status_code == 200
    # one post already went out; it must survive the cancel
    execute_db("UPDATE flyer_lady_special_posts SET status='published' WHERE special_id=%s AND platform='whatsapp_status_prepared'", (special_id,))

    resp = ctx["client"].post(f"{base}/cancel", headers=headers)
    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert resp.get_json()["cancelled"] == ["facebook_feed"]
    rows = query_db("SELECT platform FROM flyer_lady_special_posts WHERE special_id=%s", (special_id,))
    assert [r["platform"] for r in rows] == ["whatsapp_status_prepared"]


def test_cancel_with_nothing_published_returns_special_to_approved(client_with_roles):
    from database import query_db
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    special_id = _post(ctx, ctx["location_id"]).get_json()["id"]
    page = ctx["client"].get("/customers/manual-booking").get_data(as_text=True)
    headers = {"X-CSRFToken": re.search(r'name="csrf-token" content="([^"]+)"', page).group(1)}
    base = f"/dashboard/flyer-lady/specials/{special_id}"
    ctx["client"].post(f"{base}/approval", json={"decision": "approved"}, headers=headers)
    ctx["client"].post(f"{base}/queue", json={"platforms": ["facebook_feed"]}, headers=headers)
    assert ctx["client"].post(f"{base}/cancel", headers=headers).get_json()["status"] == "approved"
    assert query_db("SELECT status FROM flyer_lady_specials WHERE id=%s", (special_id,), one=True)["status"] == "approved"
    # another workshop can't cancel it
    assert ctx["client"].post(f"/dashboard/flyer-lady/specials/{special_id + 999}/cancel", headers=headers).status_code == 404
