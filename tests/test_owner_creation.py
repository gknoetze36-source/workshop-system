import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import json
import tempfile
import secrets

# Set environment variables for testing
os.environ.setdefault("SECRET_KEY", "test-secret-key")
os.environ.setdefault("SQLITE_PATH", "./test.db")
os.environ.setdefault("SKIP_DATABASE_INIT", "false")
os.environ.setdefault("META_APP_ID", "test-meta-app-id")
os.environ.setdefault("META_APP_SECRET", "test-meta-app-secret")
os.environ.setdefault("META_ACCESS_TOKEN", "test-meta-access-token")
os.environ.setdefault("WHATSAPP_BUSINESS_ACCOUNT_ID", "test-waba-id")
os.environ.setdefault("WHATSAPP_PHONE_NUMBER_ID", "test-phone-number-id")
os.environ.setdefault("VERIFY_TOKEN", "test-verify-token")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")
os.environ.setdefault("MESSAGING_TOKEN_ENCRYPTION_KEY", "BW8p_0QsLsxstYCqdNLdtYntSYxEHcWCXwF4beHxRlQ=")
os.environ.setdefault("PAYSTACK_SECRET_KEY", "test-paystack-secret")
os.environ.setdefault("PAYSTACK_WEBHOOK_SECRET", "test-paystack-webhook-secret")
os.environ.setdefault("FRONTEND_API_TOKEN", "test-frontend-token")
os.environ.setdefault("SESSION_COOKIE_SECURE", "false")
os.environ.setdefault("DEFAULT_RATE_LIMIT", "100 per hour")
os.environ.setdefault("RATELIMIT_STORAGE_URI", "memory://")
os.environ.setdefault("LOG_LEVEL", "DEBUG")
# Set super admin password for login
os.environ.setdefault("SUPERADMIN_PASSWORD", "SuperAdminPassword123!")

# Import the app after setting environment variables
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')
from app import app as flask_app
from database import initialize_database, execute_db, query_db
import platform_helpers

class OwnerCreationTestBase(unittest.TestCase):
    """Base test class for owner creation tests."""

    def setUp(self):
        """Set up test client and initialize database."""
        self.app = flask_app
        self.app.config['TESTING'] = True
        self.app.config['WTF_CSRF_ENABLED'] = False
        self.client = self.app.test_client()

        # Initialize database (since we set SKIP_DATABASE_INIT=true, we need to initialize)
        # We'll use an in-memory SQLite database and run the schema
        self.db_state = initialize_database()
        print(f"Database initialized: {self.db_state}")

        # Log in as super admin to get session
        self.login_as_super_admin()

    def login_as_super_admin(self):
        """Log in as super admin by setting session directly."""
        with self.client.session_transaction() as sess:
            sess['user_id'] = 1
            sess['_fresh'] = True

    def logout(self):
        """Log out the current user."""
        self.client.get('/logout')

    def create_franchise_via_manage(self, name="Test Franchise", plan_code="basic", setup_fee=0, monthly_base_price=100, monthly_message_limit=2000, overage_price_per_message=0.1, industry="workshop"):
        """Helper to create a franchise via /manage/franchises endpoint."""
        data = {
            "name": name,
            "plan_code": plan_code,
            "setup_fee": setup_fee,
            "monthly_base_price": monthly_base_price,
            "monthly_message_limit": monthly_message_limit,
            "overage_price_per_message": overage_price_per_message,
            "industry": industry,
            "contact_email": "owner@example.com",
            "contact_phone": "+27820000000",
            "notes": "Test franchise",
            "public_base_url": "",
            "inbound_webhook_token": secrets.token_urlsafe(16)
        }
        response = self.client.post('/manage/franchises', data=data, follow_redirects=True)
        print(f"CREATE FRANCHISE RESPONSE: status={response.status_code}, data={response.data[:200]}", flush=True)
        return response

    def create_client_via_new_client(self, business_name="Test Client", plan_code="basic", industry="workshop"):
        """Helper to create a client via /new-client endpoint."""
        data = {
            "business_name": business_name,
            "plan_code": plan_code,
            "industry": industry,
            "owner_name": "Test Owner",
            "owner_email": "owner@example.com",
            "owner_phone": "+27820000000",
            "branch_name": "Main Branch",
            "branch_code": "MAIN",
            "branch_location": "Test City",
            "branch_email": "branch@example.com",
            "branch_phone": "+27820000001",
            "staff_username": ["staff1"],
            "staff_password": ["staffpass1"],
            "staff_full_name": ["Staff One"],
            "staff_email": ["staff1@example.com"],
            "staff_phone": ["+27820000002"],
            "staff_role": ["reception"],
            "whatsapp_status": ""  # Not connecting WhatsApp for now
        }
        response = self.client.post('/new-client', data=data, follow_redirects=True)
        return response

    def provision_franchise(self, franchise_id, industry="workshop", plan_code="basic"):
        """Helper to provision a franchise via /manage/franchises/<fid>/provision endpoint."""
        data = {
            "industry": industry,
            "plan_code": plan_code,
            "monthly_message_limit": "2000"
        }
        response = self.client.post(f'/manage/franchises/{franchise_id}/provision', data=data, follow_redirects=True)
        return response

    def get_franchise_from_db(self, name):
        """Get a franchise from the database by name."""
        return query_db("SELECT * FROM franchises WHERE name = %s", (name,), one=True)

    def get_branches_for_franchise(self, franchise_id):
        """Get branches for a franchise."""
        return query_db("SELECT * FROM branches WHERE franchise_id = %s", (franchise_id,))

    def get_users_for_franchise(self, franchise_id):
        """Get users for a franchise."""
        return query_db("SELECT * FROM users WHERE franchise_id = %s", (franchise_id,))

    def get_services_for_franchise(self, franchise_id):
        """Get services for a franchise."""
        return query_db("SELECT * FROM service_prices WHERE franchise_id = %s", (franchise_id,))

    def get_feature_flags_for_franchise(self, franchise_id):
        """Get feature flags for a franchise."""
        return query_db("SELECT * FROM feature_flags WHERE franchise_id = %s", (franchise_id,))

    def get_onboarding_state_for_franchise(self, franchise_id):
        """Get onboarding state for a franchise."""
        return query_db("SELECT * FROM onboarding_state WHERE franchise_id = %s", (franchise_id,))

    def get_usage_records_for_franchise(self, franchise_id):
        """Get usage records for a franchise."""
        daily = query_db("SELECT * FROM usage_daily WHERE franchise_id = %s", (franchise_id,))
        monthly = query_db("SELECT * FROM chatbot_usage_monthly WHERE franchise_id = %s", (franchise_id,))
        return {"daily": daily, "monthly": monthly}

    def get_billing_records_for_franchise(self, franchise_id):
        """Get billing records for a franchise."""
        return query_db("SELECT * FROM billing_records WHERE franchise_id = %s", (franchise_id,))

    def get_automation_rules_for_franchise(self, franchise_id):
        """Get automation rules for a franchise."""
        return query_db("SELECT * FROM automation_rules WHERE franchise_id = %s", (franchise_id,))

    def test_super_admin_login_and_access_manage_franchises(self):
        '''Test that super admin can log in and access the manage franchises page.'''
        # Access the manage franchises page (should be accessible after login)
        response = self.client.get('/manage/franchises')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Franchises', response.data)  # Check for some text in the page

    def test_create_franchise_via_manage_endpoint(self):
        '''Test creating a franchise via the /manage/franchises POST endpoint.'''
        # Get initial franchise count
        initial_count = len(query_db('SELECT * FROM franchises'))

        # Create a new franchise
        response = self.create_franchise_via_manage(
            name='Test Franchise Create',
            plan_code='growth',
            setup_fee=500,
            monthly_base_price=200,
            monthly_message_limit=5000,
            overage_price_per_message=0.15
        )

        # Should redirect to manage franchises page with success message
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Test Franchise Create created.', response.data)

        # Check that franchise count increased by 1
        final_count = len(query_db('SELECT * FROM franchises'))
        self.assertEqual(final_count, initial_count + 1)

        # Verify the franchise was inserted with correct data
        franchise = self.get_franchise_from_db('Test Franchise Create')
        self.assertIsNotNone(franchise)
        self.assertEqual(franchise['name'], 'Test Franchise Create')
        self.assertEqual(franchise['plan_code'], 'growth')
        self.assertEqual(franchise['setup_fee'], 500)
        self.assertEqual(franchise['monthly_base_price'], 200)
        self.assertEqual(franchise['monthly_message_limit'], 5000)
        self.assertEqual(franchise['overage_price_per_message'], 0.15)
        self.assertEqual(franchise['industry'], 'workshop')
        self.assertEqual(franchise['active'], 1)  # Should be active

    def test_access_control_requires_super_admin(self):
        '''Test that endpoints require super admin role.'''
        # Log out first
        self.logout()

        # Try to access manage franchises page without login
        response = self.client.get('/manage/franchises', follow_redirects=False)
        # Should redirect to login page
        self.assertEqual(response.status_code, 302)  # Redirect
        self.assertIn('/login', response.location)

        # Try to POST to create franchise without login
        response = self.client.post('/manage/franchises', data={}, follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn('/login', response.location)

        # Try to access new-client without login
        response = self.client.get('/new-client', follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn('/login', response.location)

        # Try to POST to new-client without login
        response = self.client.post('/new-client', data={}, follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn('/login', response.location)

        # Log in as super admin to create franchise
        self.login_as_super_admin()

        # Create a franchise to test provision endpoint
        response = self.create_franchise_via_manage(name='Test for Auth', plan_code='basic')
        franchise = self.get_franchise_from_db('Test for Auth')

        # Log out to test provision without login
        self.logout()

        # Try to provision without login
        response = self.client.post(f'/manage/franchises/{franchise["id"]}/provision', data={}, follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        self.assertIn('/login', response.location)
    # -- TEST METHODS START --
    def tearDown(self):
        """Clean up after tests."""
        pass

if __name__ == '__main__':
    unittest.main()