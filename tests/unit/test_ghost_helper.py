"""The Ghost answers like a helper: the right topic for real questions, with steps,
working page/guide buttons and follow-up suggestions."""
import uuid

import pytest

from routes.ghost_help import FALLBACK, TOPICS, match

CASES = [
    ("What can you do?", "hello"), ("hello!", "hello"), ("help", "hello"),
    ("How many unanswered messages do I have?", "inbox"),  # used to return the WhatsApp status
    ("How do I add a booking?", "manual_booking"),           # used to return today's booking count
    ("How do I connect WhatsApp?", "whatsapp"), ("How do I connect Facebook?", "facebook"),
    ("How do I boost a post?", "boost"), ("How do I set up Google Ads?", "google"),
    ("Where do I add my card?", "card"), ("What are my results?", "results"),
    ("How do I mark a car as collected?", "ready_collected"), ("my customer didn't get the message", "message_not_sent"),
    ("I need to find a previous booking", "booking_history"), ("can I change my opening hours", "settings"),
    ("this is weird", "fallback"),  # "this" must not match "hi"
]


@pytest.mark.parametrize("question,topic", CASES)
def test_questions_reach_the_right_topic(question, topic):
    assert match(question).key == topic


def test_every_topic_is_helpful():
    for t in TOPICS + (FALLBACK,):
        assert t.next, f"{t.key} has no follow-up suggestions"
        for s in t.next:
            assert match(s).key != "fallback", f"{t.key} suggests {s!r}, which the Ghost can't answer"


def test_buttons_point_to_real_pages_and_guide_sections():
    import phanta_app
    from tests.unit.test_dashboard_actions import _register_and_onboard
    app = phanta_app.app
    app.config["TESTING"] = True
    client = app.test_client()
    _, csrf_from = _register_and_onboard(client, "ghosthelp" + uuid.uuid4().hex[:6])
    token = csrf_from("/dashboard")
    guides = {}
    for t in TOPICS:
        reply = client.post("/api/ghost/ask", json={"question": t.next[0] if t.key == "fallback" else t.phrases[0]},
                            headers={"X-CSRFToken": token}).get_json()
        for link in reply["links"]:
            path, _, anchor = link["url"].partition("#")
            resp = client.get(path)
            assert resp.status_code == 200, (t.key, link)
            if anchor:
                html = guides.setdefault(path, resp.get_data(as_text=True))
                assert f'id="{anchor}"' in html, (t.key, link)
