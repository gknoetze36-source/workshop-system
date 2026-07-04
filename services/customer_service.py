"""
Customer Service for Workshop System Version 2.

This service contains all business logic for the Customer entity.
It depends only on the Customer Repository.
"""

from repositories.customer_repository import (
    get_customer_by_id,
    get_customer_by_id_and_franchise,
    get_customer_by_phone_and_franchise,
    get_customer_by_email_and_franchise,
    get_customer_count_by_franchise,
)


def get_customer_by_id(customer_id):
    """Retrieve a customer by their ID."""
    return get_customer_by_id(customer_id)


def get_customer_by_id_and_franchise(customer_id, franchise_id):
    """Retrieve a customer by ID, ensuring it belongs to the given franchise."""
    return get_customer_by_id_and_franchise(customer_id, franchise_id)


def get_customer_by_phone_and_franchise(phone, franchise_id):
    """Retrieve the most recent customer for a given phone and franchise."""
    return get_customer_by_phone_and_franchise(phone, franchise_id)


def get_customer_by_email_and_franchise(email, franchise_id):
    """Retrieve the most recent customer for a given email and franchise."""
    return get_customer_by_email_and_franchise(email, franchise_id)


def get_customer_count_by_franchise(franchise_id):
    """Return the number of customers in a franchise."""
    return get_customer_count_by_franchise(franchise_id)
