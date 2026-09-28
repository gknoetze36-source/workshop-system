"""Drag-and-drop image upload for Flyer Lady specials (Cloudflare R2).

Replaces the old requirement that a workshop paste a public image URL --
most don't have a public host for a photo. POST /dashboard/flyer-lady/uploads
takes the file directly and hands back the public R2 URL that slots straight
into the same media_url field create_special() already accepts.

These tests never touch real R2: boto3's S3 client is swapped for a fake
that just records what it was asked to store, so the actual validation
(size limit, magic-byte content sniffing, capability-gated 503) is what's
under test, not network behavior.
"""
import io
import re
from unittest.mock import MagicMock, patch

import pytest
from PIL import Image

from database import query_db
from integrations.storage.r2_client import MAX_UPLOAD_BYTES

NOT_AN_IMAGE = b"<html>not an image</html>"


def _real_jpeg(size=(20, 20), color=(200, 50, 50)):
    """A genuinely decodable JPEG -- upload_media() now decodes every
    upload (see integrations/storage/image_processing.py), so a bare
    magic-byte header is no longer enough to pass; unlike the old
    header-only fixture this replaces, this actually opens with PIL."""
    buf = io.BytesIO()
    Image.new("RGB", size, color).save(buf, format="JPEG")
    return buf.getvalue()


JPEG_HEADER = _real_jpeg()


def _register_and_create_location(client, suffix):
    email = f"flyerupload-{suffix}@test.example"

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        return m.group(1) if m else None

    token = csrf_from("/register")
    client.post("/register", data={
        "full_name": "Test", "email": email, "password": "SuperSecret123",
        "confirm_password": "SuperSecret123", "csrf_token": token,
    })
    token2 = csrf_from("/onboarding/location")
    client.post("/onboarding/location", data={
        "location_name": f"Flyer Upload Workshop {suffix}", "industry": "workshop", "csrf_token": token2,
    }, follow_redirects=False)
    return csrf_from, email


def _client():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    return phanta_app.app.test_client()


def _r2_env(monkeypatch):
    monkeypatch.setenv("R2_ACCOUNT_ID", "acct123")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key123")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret123")
    monkeypatch.setenv("R2_BUCKET_NAME", "phanta-flyer-uploads")
    monkeypatch.setenv("R2_PUBLIC_BASE_URL", "https://pub-test.r2.dev")


def test_uploads_a_valid_jpeg_and_returns_its_public_url(monkeypatch):
    _r2_env(monkeypatch)
    client = _client()
    csrf_from, _ = _register_and_create_location(client, "jpeg")
    token = csrf_from("/dashboard/flyer-lady/ui")

    fake_s3 = MagicMock()
    with patch("integrations.storage.r2_client.boto3.client", return_value=fake_s3):
        response = client.post(
            "/dashboard/flyer-lady/uploads",
            data={"file": (__import__("io").BytesIO(JPEG_HEADER), "special.jpg"), "csrf_token": token},
            content_type="multipart/form-data",
        )

    assert response.status_code == 201
    body = response.get_json()
    assert body["media_url"].startswith("https://pub-test.r2.dev/flyer/")
    assert body["media_url"].endswith(".jpg")
    # Stored under Content-Type image/jpeg, not whatever the browser claimed.
    assert fake_s3.put_object.call_args.kwargs["ContentType"] == "image/jpeg"
    assert fake_s3.put_object.call_args.kwargs["Bucket"] == "phanta-flyer-uploads"


def test_rejects_a_file_that_is_not_really_an_image(monkeypatch):
    """Content-Type header is not trusted -- the actual bytes are sniffed.
    A renamed HTML file claiming to be a .jpg must still be rejected."""
    _r2_env(monkeypatch)
    client = _client()
    csrf_from, _ = _register_and_create_location(client, "spoofed")
    token = csrf_from("/dashboard/flyer-lady/ui")

    fake_s3 = MagicMock()
    with patch("integrations.storage.r2_client.boto3.client", return_value=fake_s3):
        response = client.post(
            "/dashboard/flyer-lady/uploads",
            data={"file": (__import__("io").BytesIO(NOT_AN_IMAGE), "special.jpg"), "csrf_token": token},
            content_type="multipart/form-data",
        )

    assert response.status_code == 400
    assert "not a valid image" in response.get_json()["error"]
    fake_s3.put_object.assert_not_called()


def test_rejects_a_file_over_the_size_limit(monkeypatch):
    _r2_env(monkeypatch)
    client = _client()
    csrf_from, _ = _register_and_create_location(client, "toobig")
    token = csrf_from("/dashboard/flyer-lady/ui")

    oversized = JPEG_HEADER + b"\x00" * MAX_UPLOAD_BYTES
    fake_s3 = MagicMock()
    with patch("integrations.storage.r2_client.boto3.client", return_value=fake_s3):
        response = client.post(
            "/dashboard/flyer-lady/uploads",
            data={"file": (__import__("io").BytesIO(oversized), "big.jpg"), "csrf_token": token},
            content_type="multipart/form-data",
        )

    assert response.status_code == 413
    fake_s3.put_object.assert_not_called()


def test_returns_503_with_missing_variable_names_when_r2_is_not_configured():
    """No R2_* env vars set at all -- must fail clean with which variables
    are missing, matching the require_configured() pattern used by every
    other capability-gated integration in this codebase, not a raw 500."""
    client = _client()
    csrf_from, _ = _register_and_create_location(client, "unconfigured")
    token = csrf_from("/dashboard/flyer-lady/ui")

    response = client.post(
        "/dashboard/flyer-lady/uploads",
        data={"file": (__import__("io").BytesIO(JPEG_HEADER), "special.jpg"), "csrf_token": token},
        content_type="multipart/form-data",
    )

    assert response.status_code == 503
    body = response.get_json()
    assert body["configured"] is False
    assert "R2_ACCOUNT_ID" in body["missing"]


def test_rejects_when_no_file_is_present(monkeypatch):
    _r2_env(monkeypatch)
    client = _client()
    csrf_from, _ = _register_and_create_location(client, "nofile")
    token = csrf_from("/dashboard/flyer-lady/ui")

    response = client.post(
        "/dashboard/flyer-lady/uploads",
        data={"csrf_token": token},
        content_type="multipart/form-data",
    )
    assert response.status_code == 400


def test_a_png_upload_is_stored_as_jpeg_end_to_end(monkeypatch):
    """The route-level proof that normalize_to_jpeg() is actually wired
    in, not just correct in isolation: a genuine PNG upload must reach
    R2 as image/jpeg with a .jpg key, exactly like a native JPEG
    upload -- the pixel-level conversion itself is covered in depth by
    test_image_processing.py."""
    _r2_env(monkeypatch)
    client = _client()
    csrf_from, _ = _register_and_create_location(client, "pngupload")
    token = csrf_from("/dashboard/flyer-lady/ui")

    png_buf = io.BytesIO()
    Image.new("RGB", (30, 30), (0, 200, 0)).save(png_buf, format="PNG")

    fake_s3 = MagicMock()
    with patch("integrations.storage.r2_client.boto3.client", return_value=fake_s3):
        response = client.post(
            "/dashboard/flyer-lady/uploads",
            data={"file": (io.BytesIO(png_buf.getvalue()), "special.png"), "csrf_token": token},
            content_type="multipart/form-data",
        )

    assert response.status_code == 201
    body = response.get_json()
    assert body["media_url"].endswith(".jpg")
    assert fake_s3.put_object.call_args.kwargs["ContentType"] == "image/jpeg"
    stored_bytes = fake_s3.put_object.call_args.kwargs["Body"]
    assert stored_bytes[:3] == b"\xff\xd8\xff"
