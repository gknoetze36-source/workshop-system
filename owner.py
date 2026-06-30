"""
Owner model for Workshop System Version 2.

This file defines the Owner foundation as per the Version 2 architecture.
It is not connected to the existing application logic and is intended for
future migration work. The existing Franchise model remains unchanged.
"""

class Owner:
    """
    Represents a business entity that owns and operates one or more service locations.
    """

    def __init__(self,
                 id=None,
                 name=None,
                 slug=None,
                 contact_email=None,
                 contact_phone=None,
                 notes=None,
                 industry=None,
                 subscription_status=None,
                 subscription_start=None,
                 subscription_end=None,
                 setup_fee=None,
                 plan_code=None,
                 branch_limit=None,
                 user_limit=None,
                 automation_enabled=None,
                 chatbot_enabled=None,
                 reporting_enabled=None,
                 custom_integrations_enabled=None,
                 priority_support_enabled=None,
                 monthly_base_price=None,
                 monthly_message_limit=None,
                 messages_used=None,
                 overage_price_per_message=None,
                 billing_day=None,
                 active=None,
                 created_at=None,
                 updated_at=None,
                 # Additional Version 2 specific fields
                 ai_settings=None,
                 meta_business_account_id=None,
                 service_catalogue_id=None):
        """
        Initialize an Owner instance.

        Args:
            id: Unique identifier
            name: Business name
            slug: URL-friendly identifier
            contact_email: Primary contact email
            contact_phone: Primary contact phone number
            notes: Additional notes about the business
            industry: Business industry type
            subscription_status: Current subscription status (active, trialing, cancelled, etc.)
            subscription_start: Start date of current subscription period
            subscription_end: End date of current subscription period
            setup_fee: One-time setup fee
            plan_code: Subscription plan code (e.g., 'basic', 'growth', 'premium')
            branch_limit: Maximum number of locations allowed
            user_limit: Maximum number of users allowed
            automation_enabled: Whether automation features are enabled
            chatbot_enabled: Whether chatbot features are enabled
            reporting_enabled: Whether reporting features are enabled
            custom_integrations_enabled: Whether custom integrations are enabled
            priority_support_enabled: Whether priority support is enabled
            monthly_base_price: Base monthly subscription price
            monthly_message_limit: Included messages per month
            messages_used: Messages used in current period
            overage_price_per_message: Price per message over the limit
            billing_day: Day of month for billing
            active: Whether the owner account is active
            created_at: Timestamp when the owner was created
            updated_at: Timestamp when the owner was last updated
            ai_settings: AI configuration (optional): Meta Business Account ID)
        service_catalogue_id: Reference to the service catalogue (if separated)
        """
        self.id = id
        self.name = name
        self.slug = slug
        self.contact_email = contact_email
        self.contact_phone = contact_phone
        self.notes = notes
        self.industry = industry
        self.subscription_status = subscription_status
        self.subscription_start = subscription_start
        self.subscription_end = subscription_end
        self.setup_fee = setup_fee
        self.plan_code = plan_code
        self.branch_limit = branch_limit
        self.user_limit = user_limit
        self.automation_enabled = automation_enabled
        self.chatbot_enabled = chatbot_enabled
        self.reporting_enabled = reporting_enabled
        self.custom_integrations_enabled = custom_integrations_enabled
        self.priority_support_enabled = priority_support_enabled
        self.monthly_base_price = monthly_base_price
        self.monthly_message_limit = monthly_message_limit
        self.messages_used = messages_used
        self.overage_price_per_message = overage_price_per_message
        self.billing_day = billing_day
        self.active = active
        self.created_at = created_at
        self.updated_at = updated_at
        self.ai_settings = ai_settings
        self.meta_business_account_id = meta_business_account_id
        self.service_catalogue_id = service_catalogue_id

    def __repr__(self):
        return f"<Owner {self.id}: {self.name}>"
