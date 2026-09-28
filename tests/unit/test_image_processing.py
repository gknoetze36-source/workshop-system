"""integrations/storage/image_processing.py -- normalize_to_jpeg().

Instagram's Content Publishing API expects a JPEG at the image_url it
fetches; before this module, whatever format a workshop uploaded
(JPEG/PNG/WebP) was stored as-is. These tests operate directly on the
function, not through the HTTP upload route -- the route-level tests in
test_flyer_lady_uploads.py already cover size limits, the 503-when-
unconfigured path and the end-to-end 201 response; what's specific to
this module is the actual pixel-level transformation, which is easiest
and clearest to prove directly.
"""
from __future__ import annotations

import io

import pytest
from PIL import Image

from integrations.storage.image_processing import MAX_DIMENSION_PX, normalize_to_jpeg
from integrations.storage.r2_client import R2UploadError


def _encode(size=(40, 40), mode="RGB", color=(200, 50, 50), fmt="PNG", **save_kwargs):
    buf = io.BytesIO()
    Image.new(mode, size, color).save(buf, format=fmt, **save_kwargs)
    return buf.getvalue()


def test_png_converts_to_jpeg():
    png_bytes = _encode(fmt="PNG")
    result = normalize_to_jpeg(png_bytes)

    decoded = Image.open(io.BytesIO(result))
    assert decoded.format == "JPEG"
    # JPEG start-of-image marker, not PNG's signature.
    assert result[:3] == b"\xff\xd8\xff"


def test_webp_converts_to_jpeg():
    webp_bytes = _encode(fmt="WEBP")
    result = normalize_to_jpeg(webp_bytes)

    decoded = Image.open(io.BytesIO(result))
    assert decoded.format == "JPEG"
    assert result[:3] == b"\xff\xd8\xff"


def test_jpeg_remains_valid_jpeg():
    """A JPEG upload should pass through as a genuinely valid JPEG --
    re-encoded (so it still gets EXIF-stripped and size-bounded like
    everything else), not rejected or corrupted by the round trip."""
    jpeg_bytes = _encode(fmt="JPEG")
    result = normalize_to_jpeg(jpeg_bytes)

    decoded = Image.open(io.BytesIO(result))
    decoded.load()
    assert decoded.format == "JPEG"
    assert decoded.size == (40, 40)


def test_invalid_image_is_rejected():
    """Not just a bad header -- genuinely undecodable data must be
    rejected with the existing R2UploadError, the same error type the
    route already catches, so no new except clause was needed there."""
    with pytest.raises(R2UploadError):
        normalize_to_jpeg(b"this is not image data at all, just text")


def test_truncated_file_with_a_valid_header_is_still_rejected():
    """A magic-byte-only check would pass this; an actual decode
    correctly does not. This is the exact gap between sniffing a header
    and truly validating an image."""
    real_jpeg = _encode(fmt="JPEG")
    truncated = real_jpeg[:20]  # valid JPEG marker, body cut short
    with pytest.raises(R2UploadError):
        normalize_to_jpeg(truncated)


def test_exif_metadata_is_removed():
    image = Image.new("RGB", (30, 30), (10, 20, 30))
    exif = image.getexif()
    exif[0x0112] = 1          # Orientation
    exif[0x010F] = "TestCam"  # Make
    buf = io.BytesIO()
    image.save(buf, format="JPEG", exif=exif)
    source_bytes = buf.getvalue()

    # Confirm the fixture itself actually carries EXIF, so this test
    # would fail honestly if it didn't.
    assert dict(Image.open(io.BytesIO(source_bytes)).getexif())

    result = normalize_to_jpeg(source_bytes)
    result_exif = dict(Image.open(io.BytesIO(result)).getexif())
    assert result_exif == {}


def test_oversized_image_is_resized_preserving_aspect_ratio():
    large = _encode(size=(4000, 2000), fmt="JPEG")
    result = normalize_to_jpeg(large)

    decoded = Image.open(io.BytesIO(result))
    width, height = decoded.size
    assert max(width, height) == MAX_DIMENSION_PX
    # 4000x2000 is exactly 2:1 -- must stay 2:1 after resizing, not stretch.
    assert width / height == pytest.approx(4000 / 2000, rel=0.01)


def test_image_within_the_limit_is_not_resized():
    """Resizing must be conditional -- an already-small image should
    come out the same size, not padded or upscaled."""
    small = _encode(size=(300, 200), fmt="JPEG")
    result = normalize_to_jpeg(small)

    decoded = Image.open(io.BytesIO(result))
    assert decoded.size == (300, 200)


def test_transparent_png_is_flattened_not_left_with_alpha():
    """JPEG has no alpha channel -- a transparent PNG must come out as a
    plain RGB image, composited rather than silently corrupted."""
    rgba_png = _encode(mode="RGBA", color=(0, 0, 255, 128), fmt="PNG")
    result = normalize_to_jpeg(rgba_png)

    decoded = Image.open(io.BytesIO(result))
    assert decoded.mode == "RGB"
