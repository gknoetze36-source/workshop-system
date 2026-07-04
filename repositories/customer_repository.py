"""
Customer Repository for Workshop System Version 2.

This repository handles all database operations for the Customer entity.
It interacts with the existing 'customers' table.
"""

from database import query_db


def get_customer_by_id(customer_id):
    """Retrieve a customer by their ID."""
    sql = "SELECT * FROM customers WHERE id = %s"
    return query_db(sql, (customer_id,), one=True)


def get_customer_by_id_and_franchise(customer_id, franchise_id):
    """Retrieve a customer by ID, ensuring it belongs to the given franchise."""
    sql = "SELECT * FROM customers WHERE id = %s AND franchise_id = %s"
    return query_db(sql, (customer_id, franchise_id), one=True)


def get_customer_by_phone_and_franchise(phone, franchise_id):
    """Retrieve the most recent customer for a given phone and franchise."""
    sql = """
        SELECT * FROM customers
        WHERE phone = %s AND franchise_id = %s
        ORDER BY id DESC LIMIT 1
    """
    return query_db(sql, (phone, franchise_id), one=True)


def get_customer_by_email_and_franchise(email, franchise_id):
    """Retrieve the most recent customer for a given email and franchise."""
    sql = """
        SELECT * FROM customers
        WHERE lower(email) = lower(%s) AND franchise_id = %s
        ORDER BY id DESC LIMIT 1
    """
    return query_db(sql, (email, franchise_id), one=True)


def get_customer_count_by_franchise(franchise_id):
    """Return the number of customers in a franchise."""
    sql = "SELECT COUNT(*) AS total FROM customers WHERE franchise_id = %s"
    result = query_db(sql, (franchise_id,), one=True)
    return result['total'] if result else 0
