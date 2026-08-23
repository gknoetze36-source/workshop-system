from integrations.ai.moderation.output_guard import OutputGuard


def test_output_guard_rejects_any_workshop_price():
    result = OutputGuard().validate("The repair will be R 3500.")
    assert not result.allowed
    assert "workshop pricing" in result.reasons[0]


def test_output_guard_rejects_price_even_if_legacy_allowed_prices_is_supplied():
    result = OutputGuard().validate("The repair will be R 9999.", allowed_prices=[3500])
    assert not result.allowed


def test_output_guard_blocks_approval_assertion_when_checkpoint_required():
    result = OutputGuard().validate("Your repair is approved.", approval_required=True)
    assert not result.allowed
