#!/usr/bin/env python3
"""
Workshop System MCP Server
Exposes core workshop functionality via MCP (Model Context Protocol)
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# Add the workshop system to the path so we can import its modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    CallToolRequest,
    CallToolResult,
    ListToolsRequest,
    ListToolsResult,
    Tool,
    TextContent,
    ImageContent,
    EmbeddedResource,
)
from pydantic import AnyUrl

# Import workshop system modules
from database import execute_db, query_db, initialize_database, utc_now
from ai_engine import classify_message
from platform_helpers import (
    fetch_service_prices,
    fetch_visible_bookings,
    daily_usage_summary,
    inquiry_metrics,
    franchise_counts,
    human_date,
    boolish,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("workshop-mcp")

# Initialize the MCP server
server = Server("workshop-system")

@server.list_tools()
async def handle_list_tools() -> ListToolsResult:
    """List available tools."""
    return ListToolsResult(
        tools=[
            Tool(
                name="booking_availability",
                description="Check available booking time slots for a service and date",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "service_id": {"type": "string", "description": "Service ID"},
                        "branch_id": {"type": "string", "description": "Branch ID"},
                        "date": {"type": "string", "description": "Date in YYYY-MM-DD format"},
                        "duration": {"type": "integer", "description": "Service duration in minutes (default: 60)"}
                    },
                    "required": ["service_id", "branch_id", "date"]
                }
            ),
            Tool(
                name="customer_search",
                description="Search for customers by phone number or name",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Phone number or name to search for"},
                        "limit": {"type": "integer", "description": "Maximum results to return (default: 10)"}
                    },
                    "required": ["query"]
                }
            ),
            Tool(
                name="get_billing_info",
                description="Get billing information for a franchise",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "franchise_id": {"type": "string", "description": "Franchise ID"},
                        "period_start": {"type": "string", "description": "Start date in YYYY-MM-DD format"},
                        "period_end": {"type": "string", "description": "End date in YYYY-MM-DD format"}
                    },
                    "required": ["franchise_id"]
                }
            ),
            Tool(
                name="classify_message",
                description="Classify a customer message using the AI engine",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "message": {"type": "string", "description": "Customer message to classify"}
                    },
                    "required": ["message"]
                }
            ),
            Tool(
                name="get_dashboard_stats",
                description="Get dashboard statistics and metrics",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "franchise_id": {"type": "string", "description": "Franchise ID (optional, for franchise-specific stats)"}
                    }
                }
            )
        ]
    )

@server.call_tool()
async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> CallToolResult:
    """Handle tool calls."""
    try:
        if name == "booking_availability":
            return await handle_booking_availability(arguments)
        elif name == "customer_search":
            return await handle_customer_search(arguments)
        elif name == "get_billing_info":
            return await handle_get_billing_info(arguments)
        elif name == "classify_message":
            return await handle_classify_message(arguments)
        elif name == "get_dashboard_stats":
            return await handle_get_dashboard_stats(arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")
    except Exception as e:
        logger.error(f"Error calling tool {name}: {e}")
        return CallToolResult(
            content=[TextContent(type="text", text=f"Error: {str(e)}")],
            isError=True
        )

async def handle_booking_availability(arguments: Dict[str, Any]) -> CallToolResult:
    """Check booking availability."""
    service_id = arguments["service_id"]
    branch_id = arguments["branch_id"]
    date_str = arguments["date"]
    duration = arguments.get("duration", 60)

    try:
        # Parse the date
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        # Check if date is in the past
        if target_date < datetime.now().date():
            return CallToolResult(
                content=[TextContent(type="text", text="Cannot check availability for past dates")],
                isError=True
            )

        # Get service price/duration info
        service_price = None
        try:
            service_price = query_db("""
                SELECT id, name, duration_minutes, price
                FROM service_prices
                WHERE id = %s AND active = 1
            """, (service_id,), one=True)
        except:
            pass  # Service price table might not exist or have different structure

        # If we couldn't get service info, use the provided duration
        if not service_price:
            service_price = {"id": service_id, "name": "Unknown Service", "duration_minutes": duration, "price": 0}

        service_duration = service_price.get("duration_minutes", duration)

        # Get existing bookings for this date and branch
        existing_bookings = query_db("""
            SELECT start_time, end_time
            FROM bookings
            WHERE branch_id = %s
            AND DATE(start_time) = %s
            AND status NOT IN ('cancelled', 'no_show')
            ORDER BY start_time
        """, (branch_id, target_date))

        # Define business hours (9 AM to 5 PM)
        business_start = datetime.combine(target_date, datetime.min.time().replace(hour=9))
        business_end = datetime.combine(target_date, datetime.min.time().replace(hour=17))

        # Generate available slots
        slot_duration = timedelta(minutes=service_duration)
        current_time = business_start
        available_slots = []

        while current_time + slot_duration <= business_end:
            slot_end = current_time + slot_duration

            # Check if this slot conflicts with any existing booking
            is_available = True
            for booking in existing_bookings:
                booking_start = booking["start_time"]
                booking_end = booking["end_time"]

                # Check for overlap
                if (current_time < booking_end and slot_end > booking_start):
                    is_available = False
                    break

            if is_available:
                available_slots.append({
                    "start": current_time.isoformat(),
                    "end": slot_end.isoformat(),
                    "display": f"{current_time.strftime('%H:%M')} - {slot_end.strftime('%H:%M')}"
                })

            current_time += timedelta(minutes=30)  # 30-minute increments for slot checking

        result = {
            "service": {
                "id": service_price["id"],
                "name": service_price["name"],
                "duration_minutes": service_duration,
                "price": service_price.get("price", 0)
            },
            "date": date_str,
            "branch_id": branch_id,
            "available_slots": available_slots,
            "total_available": len(available_slots)
        }

        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(result, indent=2))]
        )

    except ValueError as e:
        return CallToolResult(
            content=[TextContent(type="text", text=f"Invalid date format. Use YYYY-MM-DD: {str(e)}")],
            isError=True
        )
    except Exception as e:
        logger.error(f"Error checking booking availability: {e}")
        return CallToolResult(
            content=[TextContent(type="text", text=f"Error checking availability: {str(e)}")],
            isError=True
        )

async def handle_customer_search(arguments: Dict[str, Any]) -> CallToolResult:
    """Search for customers."""
    query = arguments["query"]
    limit = arguments.get("limit", 10)

    try:
        # Clean up the query
        query = query.strip()
        if not query:
            return CallToolResult(
                content=[TextContent(type="text", text="Search query cannot be empty")],
                isError=True
            )

        # Search by phone number or name
        customers = query_db("""
            SELECT id, phone, name, email, created_at,
                   (SELECT COUNT(*) FROM bookings WHERE customer_id = c.id) as booking_count
            FROM customers c
            WHERE (phone LIKE %s OR name LIKE %s)
            ORDER BY created_at DESC
            LIMIT %s
        """, (f"%{query}%", f"%{query}%", limit))

        # Format the results
        formatted_customers = []
        for customer in customers:
            formatted_customers.append({
                "id": customer["id"],
                "phone": customer["phone"],
                "name": customer["name"] or "Unknown",
                "email": customer["email"],
                "created_at": utc_now() if customer["created_at"] else None,  # Fix: use proper datetime conversion
                "booking_count": customer["booking_count"]
            })

        result = {
            "query": query,
            "customers": formatted_customers,
            "total_found": len(formatted_customers)
        }

        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(result, indent=2))]
        )

    except Exception as e:
        logger.error(f"Error searching customers: {e}")
        return CallToolResult(
            content=[TextContent(type="text", text=f"Error searching customers: {str(e)}")],
            isError=True
        )

async def handle_get_billing_info(arguments: Dict[str, Any]) -> CallToolResult:
    """Get billing information for a franchise."""
    franchise_id = arguments["franchise_id"]
    period_start = arguments.get("period_start")
    period_end = arguments.get("period_end")

    try:
        # Validate franchise exists
        franchise = query_db("SELECT * FROM franchises WHERE id = %s", (franchise_id,), one=True)
        if not franchise:
            return CallToolResult(
                content=[TextContent(type="text", text=f"Franchise {franchise_id} not found")],
                isError=True
            )

        # Set default period to last 30 days if not specified
        if not period_end:
            period_end = datetime.now().strftime("%Y-%m-%d")
        if not period_start:
            start_date = datetime.now() - timedelta(days=30)
            period_start = start_date.strftime("%Y-%m-%d")

        # Validate date format
        try:
            datetime.strptime(period_start, "%Y-%m-%d")
            datetime.strptime(period_end, "%Y-%m-%d")
        except ValueError:
            return CallToolResult(
                content=[TextContent(type="text", text="Invalid date format. Use YYYY-MM-DD")],
                isError=True
            )

        # Get billing records for the period
        billing_records = query_db("""
            SELECT id, amount_due, base_price, overage_messages, overage_cost,
                   billing_period_start, billing_period_end, status, created_at, updated_at,
                   paystack_reference
            FROM billing_records
            WHERE franchise_id = %s
            AND DATE(created_at) BETWEEN %s AND %s
            ORDER BY created_at DESC
        """, (franchise_id, period_start, period_end))

        # Get current franchise info
        current_info = {
            "monthly_base_price": franchise.get("monthly_base_price", 0),
            "monthly_message_limit": franchise.get("monthly_message_limit", 1000),
            "messages_used": franchise.get("messages_used", 0),
            "overage_price_per_message": franchise.get("overage_price_per_message", 0),
            "contact_email": franchise.get("contact_email"),
            "subscription_status": franchise.get("subscription_status"),
            "active": franchise.get("active")
        }

        # Calculate current period invoice
        from billing_scheduler import calculate_invoice_amount
        current_invoice = calculate_invoice_amount(franchise_id)

        # Format billing records
        formatted_records = []
        for record in billing_records:
            formatted_records.append({
                "id": record["id"],
                "amount_due": float(record["amount_due"]) if record["amount_due"] else 0,
                "base_price": float(record["base_price"]) if record["base_price"] else 0,
                "overage_messages": record["overage_messages"],
                "overage_cost": float(record["overage_cost"]) if record["overage_cost"] else 0,
                "billing_period_start": record["billing_period_start"].isoformat() if record["billing_period_start"] else None,
                "billing_period_end": record["billing_period_end"].isoformat() if record["billing_period_end"] else None,
                "status": record["status"],
                "created_at": record["created_at"].isoformat() if record["created_at"] else None,
                "updated_at": record["updated_at"].isoformat() if record["updated_at"] else None,
                "paystack_reference": record["paystack_reference"]
            })

        result = {
            "franchise": {
                "id": franchise["id"],
                "name": franchise.get("name"),
                "contact_email": franchise.get("contact_email"),
                "subscription_status": franchise.get("subscription_status"),
                "active": franchise.get("active")
            },
            "current_info": current_info,
            "current_period_invoice": current_invoice,
            "billing_period": {
                "start": period_start,
                "end": period_end
            },
            "billing_records": formatted_records,
            "total_records": len(formatted_records)
        }

        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(result, indent=2))]
        )

    except Exception as e:
        logger.error(f"Error getting billing info: {e}")
        return CallToolResult(
            content=[TextContent(type="text", text=f"Error getting billing info: {str(e)}")],
            isError=True
        )

async def handle_classify_message(arguments: Dict[str, Any]) -> CallToolResult:
    """Classify a customer message using the AI engine."""
    message = arguments["message"]

    try:
        if not message or not message.strip():
            return CallToolResult(
                content=[TextContent(type="text", text="Message cannot be empty")],
                isError=True
            )

        # Classify the message
        classification = classify_message(message)

        result = {
            "message": message,
            "classification": classification,
            "timestamp": datetime.now().isoformat()
        }

        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(result, indent=2))]
        )

    except Exception as e:
        logger.error(f"Error classifying message: {e}")
        return CallToolResult(
            content=[TextContent(type="text", text=f"Error classifying message: {str(e)}")],
            isError=True
        )

async def handle_get_dashboard_stats(arguments: Dict[str, Any]) -> CallToolResult:
    """Get dashboard statistics and metrics."""
    franchise_id = arguments.get("franchise_id")

    try:
        # Get overall stats
        stats = {}

        # Franchise counts
        try:
            franchise_count_data = franchise_counts()
            stats["franchise_counts"] = franchise_count_data
        except Exception as e:
            logger.warning(f"Could not get franchise counts: {e}")
            stats["franchise_counts"] = {"error": str(e)}

        # Daily usage summary
        try:
            usage_data = daily_usage_summary()
            stats["daily_usage"] = usage_data
        except Exception as e:
            logger.warning(f"Could not get daily usage: {e}")
            stats["daily_usage"] = {"error": str(e)}

        # Inquiry metrics
        try:
            inquiry_data = inquiry_metrics()
            stats["inquiry_metrics"] = inquiry_data
        except Exception as e:
            logger.warning(f"Could not get inquiry metrics: {e}")
            stats["inquiry_metrics"] = {"error": str(e)}

        # If franchise_id is provided, get franchise-specific stats
        if franchise_id:
            try:
                franchise = query_db("SELECT * FROM franchises WHERE id = %s", (franchise_id,), one=True)
                if franchise:
                    stats["franchise_info"] = {
                        "id": franchise["id"],
                        "name": franchise.get("name"),
                        "contact_email": franchise.get("contact_email"),
                        "subscription_status": franchise.get("subscription_status"),
                        "active": franchise.get("active"),
                        "messages_used": franchise.get("messages_used", 0),
                        "monthly_message_limit": franchise.get("monthly_message_limit", 1000),
                        "monthly_base_price": franchise.get("monthly_base_price", 0)
                    }

                    # Get recent bookings for this franchise
                    recent_bookings = query_db("""
                        SELECT b.id, b.reference, b.start_time, b.end_time, b.status,
                               c.name as customer_name, c.phone as customer_phone
                        FROM bookings b
                        LEFT JOIN customers c ON b.customer_id = c.id
                        WHERE b.franchise_id = %s
                        ORDER BY b.start_time DESC
                        LIMIT 10
                    """, (franchise_id,))

                    stats["recent_bookings"] = [
                        {
                            "id": booking["id"],
                            "reference": booking["reference"],
                            "start_time": booking["start_time"].isoformat() if booking["start_time"] else None,
                            "end_time": booking["end_time"].isoformat() if booking["end_time"] else None,
                            "status": booking["status"],
                            "customer_name": booking["customer_name"] or "Unknown",
                            "customer_phone": booking["customer_phone"]
                        }
                        for booking in recent_bookings
                    ]
                else:
                    stats["franchise_info"] = {"error": f"Franchise {franchise_id} not found"}
            except Exception as e:
                logger.warning(f"Could not get franchise-specific stats: {e}")
                stats["franchise_info"] = {"error": str(e)}

        result = {
            "timestamp": datetime.now().isoformat(),
            "stats": stats
        }

        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(result, indent=2))]
        )

    except Exception as e:
        logger.error(f"Error getting dashboard stats: {e}")
        return CallToolResult(
            content=[TextContent(type="text", text=f"Error getting dashboard stats: {str(e)}")],
            isError=True
        )

async def main():
    """Run the MCP server."""
    try:
        # Initialize the database connection
        initialize_database()
        logger.info("Database initialized")

        # Run the server
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options()
            )
    except Exception as e:
        logger.error(f"Error running MCP server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())