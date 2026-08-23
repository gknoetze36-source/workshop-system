from decimal import Decimal
from integrations.paystack.payments.transaction_service import TransactionService

def test_amount_to_subunits():
    assert TransactionService.to_subunits(Decimal("125.50")) == 12550
