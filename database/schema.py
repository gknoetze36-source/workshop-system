from .query import _run




def _get_columns(connection, backend, table_name):
    if backend == "postgres":
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = %s
                """,
                (table_name,),
            )
            return {row[0] for row in cursor.fetchall()}
        finally:
            cursor.close()

    cursor = connection.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cursor.fetchall()}


def _create_tables(connection, backend):
    primary_key = "SERIAL PRIMARY KEY" if backend == "postgres" else "INTEGER PRIMARY KEY AUTOINCREMENT"
    integer_boolean = "BOOLEAN" if backend == "postgres" else "INTEGER"

    for query in [
        f"""
        CREATE TABLE IF NOT EXISTS workshops (
            id {"UUID PRIMARY KEY" if backend == "postgres" else "TEXT PRIMARY KEY"},
            name TEXT NOT NULL,
            slug TEXT NOT NULL,
            logo_url TEXT,
            phone_number TEXT,
            whatsapp_phone_number_id TEXT,
            subscription_plan TEXT DEFAULT 'starter',
            is_active {integer_boolean} DEFAULT TRUE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS users (
            id {primary_key},
            username TEXT,
            password TEXT,
            password_hash TEXT,
            full_name TEXT,
            email TEXT,
            phone TEXT,
            company TEXT,
            role TEXT,
            last_login TEXT,
            active {integer_boolean} DEFAULT TRUE,
            must_reset_password {integer_boolean} DEFAULT FALSE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS customers (
            id {primary_key},
            first_name TEXT,
            surname TEXT,
            full_name TEXT,
            phone TEXT,
            email TEXT,
            accepts_whatsapp {integer_boolean} DEFAULT TRUE,
            metadata_json TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS vehicles (
            id {primary_key},
            customer_id INTEGER,
            make TEXT NOT NULL,
            model TEXT NOT NULL,
            year INTEGER,
            vehicle_vin TEXT UNIQUE,
            license_plate TEXT,
            current_mileage INTEGER,
            fuel_type TEXT,
            last_service_date TEXT,
            last_service_mileage INTEGER,
            next_service_due_date TEXT,
            next_service_due_mileage INTEGER,
            service_notes TEXT,
            metadata_json TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,

        f"""
        CREATE TABLE IF NOT EXISTS services (
            id {primary_key},
            name TEXT NOT NULL,
            category TEXT,
            duration_minutes INTEGER DEFAULT 60,
            price_amount REAL DEFAULT 0,
            active {integer_boolean} DEFAULT TRUE,
            description TEXT,
            display_order INTEGER DEFAULT 0,
            metadata_json TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS bookings (
            id {primary_key},
            booking_reference TEXT,
            customer_id INTEGER,
            service_id INTEGER,
            company TEXT,
            first_name TEXT,
            surname TEXT,
            customer_email TEXT,
            phone TEXT,
            preferred_contact_method TEXT,
            make TEXT,
            model TEXT,
            vehicle_year TEXT,
            fuel_type TEXT,
            vehicle_vin TEXT,
            service TEXT,
            service_level TEXT,
            current_mileage TEXT,
            scheduled_date TEXT,
            date TEXT,
            status TEXT,
            service_due_date TEXT,
            work_to_be_done TEXT,
            public_notes TEXT,
            internal_notes TEXT,
            source TEXT,
            quote_declined TEXT,
            contacted TEXT,
            missed_followup_count INTEGER DEFAULT 0,
            last_missed_followup_at TEXT,
            last_customer_reply_at TEXT,
            whatsapp_opt_in {integer_boolean} DEFAULT FALSE,
            privacy_consent_at TEXT,
            reminder_opt_in {integer_boolean} DEFAULT TRUE,
            completed_at TEXT,
            created_at TEXT,
            updated_at TEXT,
            legacy_source_key TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS reminder_campaigns (
            id {primary_key},
            booking_id INTEGER,
            reminder_kind TEXT,
            due_date TEXT,
            campaign_round INTEGER,
            scheduled_for TEXT,
            status TEXT,
            message_subject TEXT,
            message_body TEXT,
            last_channel_used TEXT,
            send_count INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            sent_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS communication_logs (
            id {primary_key},
            booking_id INTEGER,
            reminder_id INTEGER,
            user_id INTEGER,
            channel TEXT,
            recipient TEXT,
            subject TEXT,
            body TEXT,
            status TEXT,
            external_target TEXT,
            created_at TEXT,
            sent_at TEXT
        )
        """,
        # --- LEGACY, workshop_id-keyed, no longer written to ---------------
        # whatsapp_numbers, messaging_accounts, and webhook_events predate the
        # owner/location tenant model. Nothing in the active codebase INSERTs
        # into them any more (confirmed by audit) — messaging_provider.py
        # resolves WhatsApp connections through location_id via
        # MetaBusinessConnection instead. Kept here only so we don't drop
        # tables that may hold historical data on an existing deployment
        # without a deliberate migration decision. Do not build new features
        # against these three tables — use location_id-scoped tables instead.
        f"""
        CREATE TABLE IF NOT EXISTS whatsapp_numbers (
            id {primary_key},
            workshop_id {"UUID" if backend == "postgres" else "TEXT"},
            phone_number TEXT NOT NULL,
            whatsapp_phone_number_id TEXT NOT NULL,
            access_token TEXT NOT NULL,
            webhook_verify_token TEXT NOT NULL,
            is_active {integer_boolean} DEFAULT TRUE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS messaging_accounts (
            id {primary_key},
            workshop_id {"UUID" if backend == "postgres" else "TEXT"},
            provider TEXT NOT NULL,
            channel TEXT NOT NULL,
            account_id TEXT,
            sender_id TEXT,
            business_account_id TEXT,
            whatsapp_business_account_id TEXT,
            phone_number_id TEXT,
            access_token TEXT,
            token_expiry TEXT,
            auth_secret TEXT,
            webhook_verify_token TEXT,
            webhook_secret TEXT,
            embedded_signup_state TEXT DEFAULT 'not_started',
            coexistence_status TEXT DEFAULT 'not_started',
            is_active {integer_boolean} DEFAULT TRUE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS webhook_events (
            id {primary_key},
            provider TEXT NOT NULL,
            event_id TEXT NOT NULL,
            workshop_id {"UUID" if backend == "postgres" else "TEXT"},
            phone_number_id TEXT,
            event_type TEXT,
            created_at TEXT
        )
        """,
        # --- end legacy workshop_id tables ----------------------------------
        f"""
        CREATE TABLE IF NOT EXISTS service_prices (
            id {primary_key},
            service_name TEXT,
            service_category TEXT,
            price_amount REAL DEFAULT 0,
            active {integer_boolean} DEFAULT TRUE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS chatbot_messages (
            id {primary_key},
            customer_name TEXT,
            customer_phone TEXT,
            customer_email TEXT,
            channel TEXT,
            direction TEXT,
            message_text TEXT,
            suggested_service TEXT,
            matched_price REAL,
            status TEXT,
            processed {integer_boolean} DEFAULT FALSE,
            privacy_notice_sent {integer_boolean} DEFAULT FALSE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS booking_inquiries (
            id {primary_key},
            booking_id INTEGER,
            customer_name TEXT,
            customer_phone TEXT,
            customer_email TEXT,
            source_channel TEXT,
            user_state TEXT,
            service_type TEXT,
            last_message_text TEXT,
            last_user_interaction_at TEXT,
            last_followup_at TEXT,
            followup_stage INTEGER DEFAULT 0,
            next_followup_at TEXT,
            followups_sent_count INTEGER DEFAULT 0,
            replies_after_followup_count INTEGER DEFAULT 0,
            bookings_from_followups_count INTEGER DEFAULT 0,
            stop_reason TEXT,
            declined {integer_boolean} DEFAULT FALSE,
            closed_at TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS inquiry_followup_events (
            id {primary_key},
            inquiry_id INTEGER,
            followup_stage INTEGER,
            channel TEXT,
            message_subject TEXT,
            message_body TEXT,
            status TEXT,
            sent_at TEXT,
            created_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS chatbot_usage_daily (
            id {primary_key},
            usage_date TEXT,
            message_count INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS chatbot_usage_monthly (
            id {primary_key},
            usage_month TEXT,
            message_count INTEGER DEFAULT 0,
            message_limit INTEGER DEFAULT 2000,
            extra_messages INTEGER DEFAULT 0,
            base_price REAL DEFAULT 0,
            overage_price REAL DEFAULT 0.5,
            overage_cost REAL DEFAULT 0,
            total_due REAL DEFAULT 0,
            payment_status TEXT DEFAULT 'Unpaid',
            paid_at TEXT,
            payment_reference TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS credential_audit (
            id {primary_key},
            user_id INTEGER,
            username TEXT,
            actor_user_id INTEGER,
            event_type TEXT,
            note TEXT,
            created_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS audit_logs (
            id {primary_key},
            user_id INTEGER,
            actor_user_id INTEGER,
            action TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT,
            details_json TEXT,
            created_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS paystack_webhook_events (
            id {primary_key},
            event_id TEXT,
            reference TEXT,
            event_type TEXT,
            received_at TEXT,
            processed_at TEXT,
            status TEXT,
            payload_json TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS industry_templates (
            id {primary_key},
            industry TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            default_plan TEXT DEFAULT 'basic',
            default_message_limit INTEGER DEFAULT 2000,
            active {integer_boolean} DEFAULT TRUE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS automation_templates (
            id {primary_key},
            industry TEXT NOT NULL,
            name TEXT NOT NULL,
            event_type TEXT NOT NULL,
            trigger_timing TEXT,
            default_delay_minutes INTEGER DEFAULT 0,
            default_message TEXT,
            channel_priority TEXT DEFAULT 'whatsapp',
            active {integer_boolean} DEFAULT TRUE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS automation_rules (
            id {primary_key},
            template_id INTEGER,
            name TEXT NOT NULL,
            event_type TEXT NOT NULL,
            conditions_json TEXT,
            action_json TEXT,
            delay_minutes INTEGER DEFAULT 0,
            active {integer_boolean} DEFAULT TRUE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS scheduled_jobs (
            id {primary_key},
            automation_rule_id INTEGER,
            job_type TEXT NOT NULL,
            payload_json TEXT,
            scheduled_for TEXT,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 3,
            locked_at TEXT,
            completed_at TEXT,
            last_error TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS automation_logs (
            id {primary_key},
            automation_rule_id INTEGER,
            scheduled_job_id INTEGER,
            event_type TEXT,
            status TEXT,
            message TEXT,
            created_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS failed_jobs (
            id {primary_key},
            scheduled_job_id INTEGER,
            error_message TEXT,
            payload_json TEXT,
            failed_at TEXT,
            resolved {integer_boolean} DEFAULT FALSE,
            resolved_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS billing_records (
            id {primary_key},
            amount REAL DEFAULT 0,
            base_amount REAL DEFAULT 0,
            usage_amount REAL DEFAULT 0,
            status TEXT DEFAULT 'unpaid',
            billing_period TEXT,
            payment_reference_id TEXT,
            payment_link TEXT,
            paid_at TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS usage_daily (
            id {primary_key},
            usage_date TEXT,
            messages_used INTEGER DEFAULT 0,
            extra_messages INTEGER DEFAULT 0,
            extra_cost REAL DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS onboarding_sessions (
            id {primary_key},
            industry TEXT,
            selected_plan TEXT,
            status TEXT DEFAULT 'started',
            current_step TEXT,
            started_at TEXT,
            completed_at TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS onboarding_answers (
            id {primary_key},
            session_id INTEGER,
            question_key TEXT,
            answer_value TEXT,
            created_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS service_requirements (
            id {primary_key},
            vehicle_make TEXT NOT NULL,
            vehicle_type TEXT,
            mileage_range_min INTEGER,
            mileage_range_max INTEGER,
            service_name TEXT NOT NULL,
            service_category TEXT,
            description TEXT,
            estimated_duration_minutes INTEGER,
            base_price REAL,
            is_recommended {integer_boolean} DEFAULT TRUE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS onboarding_state (
            id {primary_key},
            setup_progress INTEGER DEFAULT 0,
            payment_completed {integer_boolean} DEFAULT FALSE,
            whatsapp_connected {integer_boolean} DEFAULT FALSE,
            services_created {integer_boolean} DEFAULT FALSE,
            automations_enabled {integer_boolean} DEFAULT FALSE,
            go_live_ready {integer_boolean} DEFAULT FALSE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS feature_flags (
            id {primary_key},
            feature_key TEXT NOT NULL,
            enabled {integer_boolean} DEFAULT FALSE,
            created_at TEXT,
            updated_at TEXT
        )
        """,
    ]:
        _run(connection, backend, query)


def _ensure_columns(connection, backend):
    desired_columns = {
        "workshops": {
            "id": "UUID" if backend == "postgres" else "TEXT",
            "name": "TEXT",
            "slug": "TEXT",
            "logo_url": "TEXT",
            "phone_number": "TEXT",
            "whatsapp_phone_number_id": "TEXT",
            "subscription_plan": "TEXT DEFAULT 'starter'",
            "is_active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "users": {
            "password_hash": "TEXT",
            "full_name": "TEXT",
            "email": "TEXT",
            "phone": "TEXT",
            "last_login": "TEXT",
            "company": "TEXT",
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "must_reset_password": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "bookings": {
            "booking_reference": "TEXT",
            "customer_id": "INTEGER",
            "service_id": "INTEGER",
            "company": "TEXT",
            "first_name": "TEXT",
            "surname": "TEXT",
            "customer_email": "TEXT",
            "phone": "TEXT",
            "preferred_contact_method": "TEXT",
            "make": "TEXT",
            "model": "TEXT",
            "vehicle_year": "TEXT",
            "fuel_type": "TEXT",
            "vehicle_vin": "TEXT",
            "service": "TEXT",
            "service_level": "TEXT",
            "current_mileage": "TEXT",
            "scheduled_date": "TEXT",
            "date": "TEXT",
            "work_to_be_done": "TEXT",
            "public_notes": "TEXT",
            "internal_notes": "TEXT",
            "reminder_opt_in": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "completed_at": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
            "legacy_source_key": "TEXT",
            "status": "TEXT DEFAULT 'Pending'",
            "service_due_date": "TEXT",
            "source": "TEXT DEFAULT 'Website'",
            "quote_declined": "TEXT DEFAULT 'No'",
            "contacted": "TEXT DEFAULT 'No'",
            "missed_followup_count": "INTEGER DEFAULT 0",
            "last_missed_followup_at": "TEXT",
            "last_customer_reply_at": "TEXT",
            "whatsapp_opt_in": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "privacy_consent_at": "TEXT",
            "registration_number": "TEXT",
            "colour": "TEXT",
        },
        "reminder_campaigns": {
            "booking_id": "INTEGER",
            "reminder_kind": "TEXT",
            "due_date": "TEXT",
            "campaign_round": "INTEGER",
            "scheduled_for": "TEXT",
            "status": "TEXT",
            "message_subject": "TEXT",
            "message_body": "TEXT",
            "last_channel_used": "TEXT",
            "send_count": "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
            "sent_at": "TEXT",
        },
        "communication_logs": {
            "booking_id": "INTEGER",
            "reminder_id": "INTEGER",
            "user_id": "INTEGER",
            "channel": "TEXT",
            "recipient": "TEXT",
            "subject": "TEXT",
            "body": "TEXT",
            "status": "TEXT",
            "external_target": "TEXT",
            "created_at": "TEXT",
            "sent_at": "TEXT",
        },
        "services": {
            "description": "TEXT",
            "display_order": "INTEGER DEFAULT 0",
            "metadata_json": "TEXT",
        },
        "service_prices": {
            "service_name": "TEXT",
            "service_category": "TEXT",
            "price_amount": "REAL DEFAULT 0",
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "chatbot_messages": {
            "customer_name": "TEXT",
            "customer_phone": "TEXT",
            "customer_email": "TEXT",
            "channel": "TEXT",
            "direction": "TEXT",
            "message_text": "TEXT",
            "suggested_service": "TEXT",
            "matched_price": "REAL",
            "status": "TEXT",
            "processed": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "privacy_notice_sent": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "customers": {
            "first_name": "TEXT",
            "surname": "TEXT",
            "full_name": "TEXT",
            "phone": "TEXT",
            "email": "TEXT",
            "accepts_whatsapp": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "metadata_json": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "whatsapp_numbers": {
            "workshop_id": "UUID" if backend == "postgres" else "TEXT",
            "phone_number": "TEXT",
            "whatsapp_phone_number_id": "TEXT",
            "access_token": "TEXT",
            "webhook_verify_token": "TEXT",
            "is_active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "messaging_accounts": {
            "workshop_id": "UUID" if backend == "postgres" else "TEXT",
            "provider": "TEXT",
            "channel": "TEXT",
            "account_id": "TEXT",
            "sender_id": "TEXT",
            "business_account_id": "TEXT",
            "whatsapp_business_account_id": "TEXT",
            "phone_number_id": "TEXT",
            "access_token": "TEXT",
            "token_encryption_version": "TEXT",
            "token_rotated_at": "TEXT",
            "token_expiry": "TEXT",
            "auth_secret": "TEXT",
            "webhook_verify_token": "TEXT",
            "webhook_secret": "TEXT",
            "embedded_signup_state": "TEXT DEFAULT 'not_started'",
            "coexistence_status": "TEXT DEFAULT 'not_started'",
            "is_active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "webhook_events": {
            "provider": "TEXT",
            "event_id": "TEXT",
            "workshop_id": "UUID" if backend == "postgres" else "TEXT",
            "phone_number_id": "TEXT",
            "event_type": "TEXT",
            "created_at": "TEXT",
        },
        "services": {
            "name": "TEXT",
            "category": "TEXT",
            "duration_minutes": "INTEGER DEFAULT 60",
            "price_amount": "REAL DEFAULT 0",
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "metadata_json": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "booking_inquiries": {
            "booking_id": "INTEGER",
            "customer_name": "TEXT",
            "customer_phone": "TEXT",
            "customer_email": "TEXT",
            "source_channel": "TEXT",
            "user_state": "TEXT",
            "service_type": "TEXT",
            "last_message_text": "TEXT",
            "last_user_interaction_at": "TEXT",
            "last_followup_at": "TEXT",
            "followup_stage": "INTEGER DEFAULT 0",
            "next_followup_at": "TEXT",
            "followups_sent_count": "INTEGER DEFAULT 0",
            "replies_after_followup_count": "INTEGER DEFAULT 0",
            "bookings_from_followups_count": "INTEGER DEFAULT 0",
            "stop_reason": "TEXT",
            "declined": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "closed_at": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "inquiry_followup_events": {
            "inquiry_id": "INTEGER",
            "followup_stage": "INTEGER",
            "channel": "TEXT",
            "message_subject": "TEXT",
            "message_body": "TEXT",
            "status": "TEXT",
            "sent_at": "TEXT",
            "created_at": "TEXT",
        },
        "chatbot_usage_daily": {
            "usage_date": "TEXT",
            "message_count": "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "chatbot_usage_monthly": {
            "usage_month": "TEXT",
            "message_count": "INTEGER DEFAULT 0",
            "message_limit": "INTEGER DEFAULT 2000",
            "extra_messages": "INTEGER DEFAULT 0",
            "base_price": "REAL DEFAULT 0",
            "overage_price": "REAL DEFAULT 0.5",
            "overage_cost": "REAL DEFAULT 0",
            "total_due": "REAL DEFAULT 0",
            "payment_status": "TEXT DEFAULT 'Unpaid'",
            "paid_at": "TEXT",
            "payment_reference": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "credential_audit": {
            "user_id": "INTEGER",
            "username": "TEXT",
            "actor_user_id": "INTEGER",
            "event_type": "TEXT",
            "note": "TEXT",
            "created_at": "TEXT",
        },
        "audit_logs": {
            "user_id": "INTEGER",
            "actor_user_id": "INTEGER",
            "action": "TEXT",
            "entity_type": "TEXT",
            "entity_id": "TEXT",
            "details_json": "TEXT",
            "created_at": "TEXT",
        },
        "paystack_webhook_events": {
            "event_id": "TEXT",
            "reference": "TEXT",
            "event_type": "TEXT",
            "received_at": "TEXT",
            "processed_at": "TEXT",
            "status": "TEXT",
            "payload_json": "TEXT",
        },
        "industry_templates": {
            "industry": "TEXT",
            "name": "TEXT",
            "description": "TEXT",
            "default_plan": "TEXT DEFAULT 'basic'",
            "default_message_limit": "INTEGER DEFAULT 2000",
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "automation_templates": {
            "industry": "TEXT",
            "name": "TEXT",
            "event_type": "TEXT",
            "trigger_timing": "TEXT",
            "default_delay_minutes": "INTEGER DEFAULT 0",
            "default_message": "TEXT",
            "channel_priority": "TEXT DEFAULT 'whatsapp'",
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "automation_rules": {
            "template_id": "INTEGER",
            "name": "TEXT",
            "event_type": "TEXT",
            "conditions_json": "TEXT",
            "action_json": "TEXT",
            "delay_minutes": "INTEGER DEFAULT 0",
            "active": "BOOLEAN DEFAULT TRUE" if backend == "postgres" else "INTEGER DEFAULT 1",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "scheduled_jobs": {
            "automation_rule_id": "INTEGER",
            "job_type": "TEXT",
            "payload_json": "TEXT",
            "scheduled_for": "TEXT",
            "status": "TEXT DEFAULT 'pending'",
            "attempts": "INTEGER DEFAULT 0",
            "max_attempts": "INTEGER DEFAULT 3",
            "locked_at": "TEXT",
            "completed_at": "TEXT",
            "last_error": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "automation_logs": {
            "automation_rule_id": "INTEGER",
            "scheduled_job_id": "INTEGER",
            "event_type": "TEXT",
            "status": "TEXT",
            "message": "TEXT",
            "created_at": "TEXT",
        },
        "failed_jobs": {
            "scheduled_job_id": "INTEGER",
            "error_message": "TEXT",
            "payload_json": "TEXT",
            "failed_at": "TEXT",
            "resolved": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "resolved_at": "TEXT",
        },
        "billing_records": {
            "amount": "REAL DEFAULT 0",
            "base_amount": "REAL DEFAULT 0",
            "usage_amount": "REAL DEFAULT 0",
            "status": "TEXT DEFAULT 'unpaid'",
            "billing_period": "TEXT",
            "payment_reference_id": "TEXT",
            "payment_link": "TEXT",
            "paid_at": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "usage_daily": {
            "usage_date": "TEXT",
            "messages_used": "INTEGER DEFAULT 0",
            "extra_messages": "INTEGER DEFAULT 0",
            "extra_cost": "REAL DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "onboarding_sessions": {
            "industry": "TEXT",
            "selected_plan": "TEXT",
            "status": "TEXT DEFAULT 'started'",
            "current_step": "TEXT",
            "started_at": "TEXT",
            "completed_at": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "onboarding_answers": {
            "session_id": "INTEGER",
            "question_key": "TEXT",
            "answer_value": "TEXT",
            "created_at": "TEXT",
        },
        "onboarding_state": {
            "setup_progress": "INTEGER DEFAULT 0",
            "payment_completed": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "whatsapp_connected": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "services_created": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "automations_enabled": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "go_live_ready": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
        "feature_flags": {
            "feature_key": "TEXT",
            "enabled": "BOOLEAN DEFAULT FALSE" if backend == "postgres" else "INTEGER DEFAULT 0",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        },
    }

    for table_name, columns in desired_columns.items():
        existing = _get_columns(connection, backend, table_name)
        for column_name, definition in columns.items():
            if column_name not in existing:
                _run(connection, backend, f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


