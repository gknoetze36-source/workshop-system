"""
Monthly billing scheduler
Run daily: checks if franchises need billing and charges them
"""

import os
from datetime import datetime, timedelta
from database import fetch_all, fetch_one, execute_db, utc_now, initialize_database
from services.paystack import create_invoice, verify_payment
import logging

logger = logging.getLogger(__name__)

def get_franchises_due_for_billing():
    """Get franchises that need to be billed today"""
    today = datetime.now().strftime("%d")
    
    return fetch_all("""
        SELECT f.*, 
               (SELECT COUNT(*) FROM billing_records 
                WHERE franchise_id=f.id AND DATE(created_at)=DATE(NOW())) as billed_today
        FROM franchises f
        WHERE f.active = 1 
          AND f.subscription_status = 'active'
          AND (f.billing_day IS NULL OR f.billing_day = %s)
          AND billed_today = 0
    """, (today,))

def calculate_invoice_amount(franchise_id):
    """Calculate monthly invoice for franchise"""
    franchise = fetch_one(
        "SELECT monthly_base_price, monthly_message_limit, messages_used, overage_price_per_message FROM franchises WHERE id=%s",
        (franchise_id,)
    )
    
    base_price = franchise.get("monthly_base_price", 0)
    messages_used = franchise.get("messages_used", 0)
    message_limit = franchise.get("monthly_message_limit", 1000)
    overage_price = franchise.get("overage_price_per_message", 0.5)
    
    # Calculate overage
    overage_messages = max(0, messages_used - message_limit)
    overage_cost = overage_messages * overage_price
    
    total = base_price + overage_cost
    
    return {
        "base_price": base_price,
        "messages_used": messages_used,
        "message_limit": message_limit,
        "overage_messages": overage_messages,
        "overage_cost": overage_cost,
        "total_amount": total,
    }

def bill_franchise(franchise_id):
    """Bill a single franchise"""
    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
    
    if not franchise:
        logger.error(f"Franchise {franchise_id} not found")
        return False
    
    # Calculate amount
    invoice = calculate_invoice_amount(franchise_id)
    total_amount = invoice["total_amount"]
    
    if total_amount == 0:
        logger.info(f"No billing needed for franchise {franchise_id}")
        return True
    
    # Create billing record
    billing_id = execute_db("""
        INSERT INTO billing_records 
        (franchise_id, amount_due, base_price, overage_messages, overage_cost,
         billing_period_start, billing_period_end, status, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        franchise_id,
        total_amount,
        invoice["base_price"],
        invoice["overage_messages"],
        invoice["overage_cost"],
        (datetime.now() - timedelta(days=30)).isoformat(),
        datetime.now().isoformat(),
        "pending",
        utc_now(),
        utc_now()
    ))
    
    # Create Paystack invoice
    try:
        paystack_response = create_invoice(
            franchise_email=franchise["contact_email"],
            amount=int(total_amount * 100),  # Paystack uses kobo
            description=f"Monthly subscription + {invoice['overage_messages']} extra messages",
            reference=f"billing_{billing_id}",
        )
        
        # Update billing record with Paystack reference
        execute_db("""
            UPDATE billing_records 
            SET paystack_reference=%s, updated_at=%s
            WHERE id=%s
        """, (paystack_response["reference"], utc_now(), billing_id))
        
        logger.info(f"Created invoice for franchise {franchise_id}: {paystack_response['reference']}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to create Paystack invoice for franchise {franchise_id}: {e}")
        execute_db("""
            UPDATE billing_records 
            SET status=%s, updated_at=%s
            WHERE id=%s
        """, ("failed", utc_now(), billing_id))
        return False

def process_monthly_billing():
    """Process billing for all franchises due"""
    logger.info("Starting monthly billing process")
    
    franchises = get_franchises_due_for_billing()
    logger.info(f"Found {len(franchises)} franchises to bill")
    
    success_count = 0
    for franchise in franchises:
        if bill_franchise(franchise["id"]):
            success_count += 1
    
    logger.info(f"Billing complete: {success_count}/{len(franchises)} successful")
    return success_count == len(franchises)

def reset_monthly_usage():
    """Reset message count at end of month"""
    logger.info("Resetting monthly usage counters")
    
    execute_db("""
        UPDATE franchises 
        SET messages_used = 0, updated_at = %s
        WHERE active = 1
    """, (utc_now(),))
    
    logger.info("Usage counters reset")

if __name__ == "__main__":
    logger.basicConfig(level=logging.INFO)
    initialize_database()
    process_monthly_billing()
    