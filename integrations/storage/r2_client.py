"""Cloudflare R2 storage for Flyer Lady's uploaded images.

Why R2 at all: Instagram's Content Publishing API and the Facebook Page
Photos API both require a public HTTPS `image_url`/`url` they fetch from --
neither accepts a direct binary upload from PHANTA. A workshop dragging a
JPEG onto the Flyer Lady screen has no public host for that file, so
something has to give it one, briefly, for Meta to pull from during
publish. R2 was the deliberate choice here (not S3) because it has zero
egress fees, which matters for a SaaS re-serving images on every workshop's
behalf, and because its S3-compatible API means no bespoke client is
needed -- boto3 talks to it directly once pointed at the right endpoint.

What this does NOT do: keep the image forever. VANTA's own instruction was
"we do not need to save the photos just post" -- this uploads, returns a
public URL immediately usable by every Special platform publisher (they all
already just take a `media_url` string; see flyer_lady/platforms/*), and
leaves cleanup to an R2 lifecycle rule configured directly in the Cloudflare
dashboard (bucket -> Settings -> Object lifecycle rules), not to any code
here. There is deliberately no delete_image() in this client -- publishing
already happened by the time anything might want to clean up, and Meta has
already fetched the bytes it needs by then.
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

#: Content-Length cap enforced by the upload route before this client is
#: ever reached. Kept here too as the single source of truth for both.
MAX_UPLOAD_BYTES = 8 * 1024 * 1024  # 8 MB

#: Accepted image types -> (file extension, magic-byte sniffer). Content-Type
#: headers from a browser can be spoofed or simply wrong; the magic bytes are
#: checked against the actual uploaded data, not trusted from the client.
_SNIFFERS = {
    "image/jpeg": (
        "jpg",
        lambda head: head[:3] == b"\xff\xd8\xff",
    ),
    "image/png": (
        "png",
        lambda head: head[:8] == b"\x89PNG\r\n\x1a\n",
    ),
    "image/webp": (
        "webp",
        lambda head: head[:4] == b"RIFF" and head[8:12] == b"WEBP",
    ),
}


class R2UploadError(RuntimeError):
    """The image could not be validated or stored in R2."""


def sniff_image_type(data: bytes) -> str | None:
    """Return the file extension for `data`'s real type, or None if it
    doesn't match any accepted image format regardless of what a
    Content-Type header claimed."""
    for _content_type, (extension, matches) in _SNIFFERS.items():
        if matches(data):
            return extension
    return None


@dataclass(frozen=True)
class R2Config:
    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket_name: str
    public_base_url: str

    @classmethod
    def from_env(cls) -> "R2Config":
        return cls(
            account_id=(os.getenv("R2_ACCOUNT_ID") or "").strip(),
            access_key_id=(os.getenv("R2_ACCESS_KEY_ID") or "").strip(),
            secret_access_key=(os.getenv("R2_SECRET_ACCESS_KEY") or "").strip(),
            bucket_name=(os.getenv("R2_BUCKET_NAME") or "").strip(),
            public_base_url=(os.getenv("R2_PUBLIC_BASE_URL") or "").strip().rstrip("/"),
        )

    def endpoint_url(self) -> str:
        return f"https://{self.account_id}.r2.cloudflarestorage.com"

    def validate(self) -> None:
        missing = [
            name
            for name, value in (
                ("R2_ACCOUNT_ID", self.account_id),
                ("R2_ACCESS_KEY_ID", self.access_key_id),
                ("R2_SECRET_ACCESS_KEY", self.secret_access_key),
                ("R2_BUCKET_NAME", self.bucket_name),
                ("R2_PUBLIC_BASE_URL", self.public_base_url),
            )
            if not value
        ]
        if missing:
            raise R2UploadError(
                "Cloudflare R2 is not configured. Missing: " + ", ".join(missing)
            )
        if not self.public_base_url.startswith("https://"):
            raise R2UploadError("R2_PUBLIC_BASE_URL must be an https:// URL")


class R2Client:
    """Thin wrapper around boto3's S3 client pointed at an R2 bucket.

    Not the encrypted, per-workshop token pattern used for Meta/Google/
    Paystack credentials (integrations/meta/auth/token_store.py and
    friends) -- deliberately. There is exactly one R2 bucket for the whole
    PHANTA deployment, shared across every workshop's Flyer Lady uploads, so
    this is one set of deployment-level credentials read straight from the
    environment like META_APP_ID/META_APP_SECRET, not a per-tenant secret
    that needs its own encrypted row.
    """

    def __init__(self, config: R2Config | None = None):
        self.config = config or R2Config.from_env()
        self.config.validate()
        self._client = boto3.client(
            "s3",
            endpoint_url=self.config.endpoint_url(),
            aws_access_key_id=self.config.access_key_id,
            aws_secret_access_key=self.config.secret_access_key,
            config=BotoConfig(signature_version="s3v4"),
            region_name="auto",
        )

    def upload_image(self, data: bytes, *, location_id: int) -> str:
        """Store `data` (already validated by the caller) and return its
        public URL. Raises R2UploadError on any failure talking to R2."""
        extension = sniff_image_type(data)
        if extension is None:
            raise R2UploadError("File is not a recognized JPEG, PNG or WebP image")
        content_type = next(
            ct for ct, (ext, _matches) in _SNIFFERS.items() if ext == extension
        )
        # Scoped under the location so a bucket listing (or a future
        # per-workshop lifecycle rule) can tell whose upload is whose.
        # Randomized rather than derived from the special/filename -- this
        # runs before a Special row exists yet, and a guessable key would
        # let anyone enumerate other workshops' flyer images off a public
        # bucket.
        key = f"flyer/{location_id}/{uuid.uuid4().hex}.{extension}"
        try:
            self._client.put_object(
                Bucket=self.config.bucket_name,
                Key=key,
                Body=data,
                ContentType=content_type,
                CacheControl="public, max-age=604800",
            )
        except (BotoCoreError, ClientError) as exc:
            raise R2UploadError(f"Upload to R2 failed: {exc}") from exc
        return f"{self.config.public_base_url}/{key}"
