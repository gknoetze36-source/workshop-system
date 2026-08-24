"""Regression test for a bug found 2026-08-25: ai/service_advisor/
runtime.py's build_service_advisor() never passed usage_repo to
AIDispatcher, so every real Service Advisor conversation's token counts,
estimated cost, latency, and any retry/failure were silently discarded --
ai_usage_log exists specifically to record this (with its own RLS policy,
migration 0021), and AIUsageRepository was already built and ready; it
just never received the session it needed.
"""
from database import initialize_database, execute_db, query_db, utc_now, get_session


def test_build_service_advisor_wires_usage_logging(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-dummy-key-for-construction-only")
    initialize_database(run_migrations=False)
    execute_db(
        "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
        ("AI Usage Owner", "aiusage@test.example", utc_now(), utc_now()),
    )
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", ("aiusage@test.example",), one=True)["id"]
    execute_db(
        "INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at) VALUES (%s,%s,'workshop',TRUE,%s,%s)",
        (owner_id, "AI Usage Workshop", utc_now(), utc_now()),
    )
    location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]

    from integrations.ai.providers.base_provider import AIProvider, AIResponse
    from ai.service_advisor.runtime import build_service_advisor

    class FakeProvider(AIProvider):
        name = "openai"

        def complete(self, request):
            return AIResponse(text="hi", provider="openai", model=request.model, input_tokens=100, output_tokens=50)

    session = get_session()
    try:
        advisor = build_service_advisor(session)
        # build_service_advisor constructs its own real OpenAIProvider; swap
        # in the fake one so this test doesn't need a real API key, without
        # touching the dispatcher wiring under test.
        advisor.dispatcher.providers["openai"] = FakeProvider()

        assert advisor.dispatcher.usage_repo is not None, \
            "build_service_advisor() must wire a usage_repo into the dispatcher"

        from integrations.ai.providers.base_provider import AIRequest
        advisor.dispatcher.complete(
            AIRequest(messages=[{"role": "user", "content": "hi"}], model="gpt-4"),
            task_type="conversation", location_id=location_id,
        )
        session.commit()
    finally:
        session.close()

    logged = query_db(
        "SELECT location_id, task_type, input_tokens, output_tokens, success FROM ai_usage_log WHERE location_id=%s",
        (location_id,), one=True,
    )
    assert logged is not None, "a real Service Advisor call must be recorded in ai_usage_log"
    assert logged["input_tokens"] == 100
    assert logged["output_tokens"] == 50
