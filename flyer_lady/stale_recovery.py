"""Recovers SpecialPost rows orphaned at status="publishing" because the
worker that claimed them crashed (or was killed, or the process died)
before FlyerLadyPublishService.publish_post()'s except block ever ran.

Without this, such a post is stuck forever: jobs/flyer_lady.py's claim
query only selects status IN (pending, failed), so "publishing" is
never picked up again by anything.

The rule, stated once and followed exactly: a stale post is never
automatically published again. For a platform whose publish flow has an
async container/creation step (Instagram), the platform's own status is
checked first -- a confirmed, already-completed success is recorded as
such, since recognizing something that already genuinely happened is
not "publishing it again". Every other outcome -- a confirmed failure,
or (today, always -- see _check_platform_status()'s docstring) an
inconclusive check -- moves the post to needs_check, which the ordinary
scheduler claim query does not select either, so it waits for a human
rather than being silently retried. Facebook, X and Google Business
Profile publishers make one synchronous call with no intermediate
reference at all; for those, blindly retrying risks a duplicate real
post, so they go straight to needs_check with no check attempted.

Deliberately its own small module, not folded into jobs/flyer_lady.py
or publish_service.py: this is a self-contained decision (is this post
stale, and if so what happens to it), easiest to test and reason about
on its own, and jobs/flyer_lady.py's run_flyer_lady_publish_queue() only
needs to call it, not contain it.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from .models import SpecialPost

#: Deliberately generous, not a platform-documented value: Instagram
#: publishing (flyer_lady/platforms/instagram_publisher.py) legitimately
#: polls for up to MAX_POLLS * POLL_SECONDS = 5 * 60 = 300 seconds (5
#: minutes) before it ever raises on its own. This must clear that with
#: real margin, or an actively-polling, perfectly healthy publish
#: attempt would be wrongly flagged as orphaned and pulled out from
#: under the worker still legitimately processing it.
STALE_AFTER = timedelta(minutes=15)

#: The scheduler's own claim query (jobs/flyer_lady.py) selects status
#: IN (pending, failed) -- an allowlist, so this new value is excluded
#: from automatic pickup without that query needing to change.
NEEDS_CHECK = "needs_check"

#: Platforms whose publish flow creates an async container/creation
#: object with its own id before the real post exists -- as opposed to
#: Facebook feed/story and Google Business Profile's publishers, which
#: make one synchronous call that either creates the real, final post or
#: doesn't, with nothing intermediate to check.
CONTAINER_BASED_PLATFORMS = frozenset({"instagram_feed", "instagram_story"})


def _check_platform_status(post: SpecialPost) -> str | None:
    """For a container-based platform, attempts to determine whether the
    container the crashed worker was creating actually completed.
    Returns "published", "failed", or None when nothing can be
    determined.

    Always returns None today. InstagramPublisher.publish() (see
    flyer_lady/platforms/instagram_publisher.py) creates its container,
    polls it, and publishes it, all within one call -- it is never
    given the post or session it would need to persist that container
    id anywhere, so by the time a post is found stale here, no
    reference to check against was ever recorded. That is a real,
    separate gap in the publisher itself, out of scope for this change
    (which touches recovery of an orphaned post, not the publisher's
    own internals) -- this function exists specifically so closing that
    gap later (persist the container id, then look it up here) requires
    no change to where or how stale-post recovery calls it. Until then
    it correctly reports "unknown", and the caller falls back to the
    same needs_check outcome Facebook/Google Business Profile always
    get.
    """
    return None


def reconcile_stale_publishing_posts(
    session, location_id: int, *, stale_after: timedelta = STALE_AFTER, now: datetime | None = None
) -> list[int]:
    """Finds posts stuck at status="publishing" for longer than
    `stale_after` and resolves them -- never by calling a publisher
    again. Returns the ids moved to needs_check specifically (not
    including any resolved as a confirmed success). Does not commit;
    the caller owns that, matching _claim_due_posts() in
    jobs/flyer_lady.py, which this is meant to run alongside.
    """
    now = now or datetime.now(timezone.utc)
    cutoff = now - stale_after

    stale_posts = session.scalars(
        select(SpecialPost).where(
            SpecialPost.location_id == location_id,
            SpecialPost.status == "publishing",
            (SpecialPost.publishing_started_at.is_(None) | (SpecialPost.publishing_started_at <= cutoff)),
        )
    ).all()

    needs_check_ids: list[int] = []
    for post in stale_posts:
        if post.platform in CONTAINER_BASED_PLATFORMS:
            result = _check_platform_status(post)
            if result == "published":
                post.status = "published"
                post.published_at = now
                post.error_message = None
                continue
            # A confirmed failure is still a real, known outcome -- but
            # "do not automatically publish it again" is followed
            # exactly: it is not silently handed back to the retry
            # pipeline, it goes to needs_check like everything else
            # below, with that outcome recorded for whoever reviews it.
            if result == "failed":
                post.error_message = "Platform reported this container as failed or expired."

        post.status = NEEDS_CHECK
        post.next_attempt_at = None
        needs_check_ids.append(post.id)

    return needs_check_ids
