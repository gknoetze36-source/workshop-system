"""Normalize an uploaded Flyer Lady image to a clean JPEG before it
reaches R2.

Why this exists: Instagram's Content Publishing API expects a JPEG at
the `image_url` it fetches. Facebook's Page Photos API is more
permissive, but there's no reason to store two different formats for
the same special's media_url when one (JPEG) satisfies both. Before
this module, whatever format a workshop dragged in (JPEG, PNG, WebP)
was stored as-is -- a PNG upload would have been handed to Instagram
verbatim, which is exactly the gap this closes.

Deliberately separate from integrations/storage/r2_client.py: this
module's job ends at "produce valid JPEG bytes"; it does not know R2
exists. routes/flyer_lady.py's upload_media() calls this first, then
hands the result to the unmodified R2Client exactly as before -- R2
still receives, sniffs, and stores a JPEG the same way it always has
for a JPEG upload, so nothing about R2Client's own logic needed to
change.

Raises the existing R2UploadError (not a new exception type) so the
route's current `except R2UploadError` continues to catch failures
from either stage without an added except clause.
"""
from __future__ import annotations

import io

from PIL import Image, ImageOps, UnidentifiedImageError

from .r2_client import R2UploadError

# Deliberately chosen, not a Meta-documented requirement: generous enough
# that a genuine product photo never visibly loses quality, small enough
# to actually bound a raw 6000x4000 camera photo or phone screenshot.
# Applied to the longest edge; aspect ratio is always preserved.
MAX_DIMENSION_PX = 1600

JPEG_QUALITY = 85


def normalize_to_jpeg(data: bytes) -> bytes:
    """Return `data` re-encoded as a clean JPEG: any orientation baked in
    from EXIF, any transparency flattened, resized if oversized, and no
    metadata carried forward. Raises R2UploadError if `data` cannot
    actually be decoded as an image at all -- a stronger check than a
    magic-byte sniff, since a truncated or corrupted file can still
    start with a valid header.
    """
    try:
        image = Image.open(io.BytesIO(data))
        # Image.open() is lazy; nothing is actually decoded until the
        # pixel data is read. A file with a valid-looking header but
        # truncated or corrupted body only fails here, not above.
        image.load()
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        raise R2UploadError("File is not a valid image") from exc

    # A phone photo's EXIF Orientation tag rotates it only through EXIF-
    # aware viewers; stripping EXIF below without first applying that
    # rotation to the actual pixels would leave the visible image
    # sideways everywhere else, including on Instagram/Facebook.
    image = ImageOps.exif_transpose(image)

    # JPEG has no alpha channel. A PNG/WebP upload with real transparency
    # is composited onto white rather than left to Pillow's default of
    # silently dropping the alpha channel, which can shift colors at
    # any partially-transparent edge.
    if image.mode in ("RGBA", "LA") or (image.mode == "P" and "transparency" in image.info):
        image = image.convert("RGBA")
        flattened = Image.new("RGB", image.size, (255, 255, 255))
        flattened.paste(image, mask=image.split()[-1])
        image = flattened
    else:
        image = image.convert("RGB")

    if max(image.size) > MAX_DIMENSION_PX:
        image.thumbnail((MAX_DIMENSION_PX, MAX_DIMENSION_PX), Image.LANCZOS)

    buffer = io.BytesIO()
    # No exif= argument -- Pillow only writes EXIF into the output when
    # explicitly given bytes to write, so omitting it strips it. Saving
    # a freshly built RGB image (rather than the original object) also
    # drops any ICC profile or other embedded metadata beyond EXIF.
    image.save(buffer, format="JPEG", quality=JPEG_QUALITY)
    return buffer.getvalue()
