from .query import _run



def _ensure_unique_username_index(connection, backend):
    _run(connection, backend, "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)")


def _ensure_indexes(connection, backend):
    index_queries = [
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_workshops_slug ON workshops(slug)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_locations_slug ON locations(slug)",
        "CREATE INDEX IF NOT EXISTS idx_users_location ON users(location_id)",
        "CREATE INDEX IF NOT EXISTS idx_users_owner ON users(owner_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_bookings_reference ON bookings(booking_reference)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_bookings_legacy_source ON bookings(legacy_source_key)",
        "CREATE INDEX IF NOT EXISTS idx_bookings_scope ON bookings(location_id, scheduled_date)",
        "CREATE INDEX IF NOT EXISTS idx_bookings_customer ON bookings(location_id, customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_customers_scope ON customers(location_id, phone, email)",
        "CREATE INDEX IF NOT EXISTS idx_services_scope ON services(location_id, name)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_reminder_unique_round ON reminder_campaigns(booking_id, reminder_kind, campaign_round)",
        "CREATE INDEX IF NOT EXISTS idx_communication_logs_scope ON communication_logs(location_id, channel)",
        "CREATE INDEX IF NOT EXISTS idx_service_prices_scope ON service_prices(location_id, service_name)",
        "CREATE INDEX IF NOT EXISTS idx_chatbot_messages_scope ON chatbot_messages(location_id, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_whatsapp_numbers_phone_id ON whatsapp_numbers(whatsapp_phone_number_id)",
        "CREATE INDEX IF NOT EXISTS idx_whatsapp_numbers_workshop ON whatsapp_numbers(workshop_id, is_active)",
        "CREATE INDEX IF NOT EXISTS idx_messaging_accounts_scope ON messaging_accounts(workshop_id, provider, channel, is_active)",
        "CREATE INDEX IF NOT EXISTS idx_messaging_accounts_phone_id ON messaging_accounts(provider, phone_number_id, is_active)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_messaging_meta_active_workshop ON messaging_accounts(workshop_id, provider) WHERE provider='meta' AND is_active=TRUE",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_messaging_meta_active_phone ON messaging_accounts(provider, phone_number_id) WHERE provider='meta' AND is_active=TRUE AND phone_number_id IS NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_events_provider_event ON webhook_events(provider, event_id)",
        "CREATE INDEX IF NOT EXISTS idx_webhook_events_scope ON webhook_events(workshop_id, provider, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_booking_inquiries_scope ON booking_inquiries(location_id, user_state, next_followup_at)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_booking_inquiries_contact ON booking_inquiries(location_id, customer_phone, source_channel)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_inquiry_followup_events_unique ON inquiry_followup_events(inquiry_id, followup_stage)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_chatbot_usage_daily_scope ON chatbot_usage_daily(location_id, usage_date)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_chatbot_usage_monthly_scope ON chatbot_usage_monthly(location_id, usage_month)",
        "CREATE INDEX IF NOT EXISTS idx_credential_audit_scope ON credential_audit(location_id, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_audit_logs_scope ON audit_logs(location_id, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_audit_logs_entity ON audit_logs(entity_type, entity_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_paystack_webhook_events_event ON paystack_webhook_events(event_id)",
        "CREATE INDEX IF NOT EXISTS idx_paystack_webhook_events_reference ON paystack_webhook_events(reference)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_industry_templates_industry ON industry_templates(industry)",
        "CREATE INDEX IF NOT EXISTS idx_automation_templates_industry ON automation_templates(industry, event_type)",
        "CREATE INDEX IF NOT EXISTS idx_automation_rules_scope ON automation_rules(location_id, event_type, active)",
        "CREATE INDEX IF NOT EXISTS idx_automation_rules_location_scope ON automation_rules(location_id, event_type, active)",
        "CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_due ON scheduled_jobs(status, scheduled_for)",
        "CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_scope ON scheduled_jobs(location_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_automation_logs_scope ON automation_logs(location_id, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_failed_jobs_scope ON failed_jobs(location_id, resolved, failed_at)",
        "CREATE INDEX IF NOT EXISTS idx_billing_records_scope ON billing_records(location_id, billing_period, status)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_usage_daily_scope ON usage_daily(location_id, usage_date)",
        "CREATE INDEX IF NOT EXISTS idx_onboarding_sessions_scope ON onboarding_sessions(location_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_onboarding_answers_session ON onboarding_answers(session_id, question_key)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_onboarding_state_location ON onboarding_state(location_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_feature_flags_scope ON feature_flags(location_id, feature_key)",
    ]
    for query in index_queries:
        _run(connection, backend, query)

