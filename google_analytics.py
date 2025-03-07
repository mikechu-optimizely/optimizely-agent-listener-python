#!/usr/bin/env python
"""
Google Analytics Integration Module
----------------------------------
This module handles sending Optimizely Agent notification data to Google Analytics.
"""

import os
import logging
import requests
import time
from typing import Dict, Any

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
REQUEST_TIMEOUT = 10  # seconds
MAX_RETRIES = 3


def get_ga_config() -> Dict[str, str]:
    """
    Get Google Analytics configuration from environment variables.

    Returns:
        Dict containing GA configuration with the following keys:
        - measurement_id: The GA4 measurement ID
        - api_secret: The GA4 API secret
        - endpoint_url: The GA4 API endpoint URL
    """
    measurement_id = os.environ.get("GA_MEASUREMENT_ID", "")
    api_secret = os.environ.get("GA_API_SECRET", "")

    # Check if required configuration is set
    if not measurement_id or not api_secret:
        logger.warning(
            "Google Analytics is not fully configured. Required: GA_MEASUREMENT_ID, GA_API_SECRET"
        )

    # Default endpoint (US/Global)
    default_endpoint = "https://www.google-analytics.com/mp/collect"

    # Get endpoint URL from environment or use default
    endpoint_url = os.environ.get("GA_ENDPOINT_URL", default_endpoint)

    return {
        "measurement_id": measurement_id,
        "api_secret": api_secret,
        "endpoint_url": endpoint_url,
    }


def transform_optimizely_data(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform Optimizely notification data into Google Analytics 4 format.

    Args:
        event_data: The notification event data from Optimizely

    Returns:
        Dict containing the transformed data in GA4 format
    """
    # Validate input
    if not isinstance(event_data, dict):
        logger.error(f"Invalid event_data type: {type(event_data)}")
        return {"name": "error", "params": {"error": "invalid_input"}}

    # Get notification type
    notification_type = event_data.get("type", "unknown")

    # Base event data
    ga_event = {
        "name": f"optimizely_{notification_type}",
        "params": {
            "notification_type": notification_type,
        },
    }

    # Add user properties if available
    user_id = event_data.get("userId")
    if user_id:
        ga_event["params"]["user_id"] = str(user_id)

    # Add user attributes if available
    attributes = event_data.get("attributes", {})
    if attributes and isinstance(attributes, dict):
        for key, value in attributes.items():
            # Prefix attribute keys to avoid conflicts with GA4 reserved parameters
            param_key = f"attr_{key}"

            # Ensure values are of supported types (string, number, boolean)
            if isinstance(value, (str, int, float, bool)):
                ga_event["params"][param_key] = value
            else:
                # Convert other types to string
                ga_event["params"][param_key] = str(value)

    # Process based on notification type
    if notification_type == "decision":
        # Handle decision notification
        decision = event_data.get("decision", {})
        if decision and isinstance(decision, dict):
            # Add decision type if available
            decision_type = event_data.get("decisionType")
            if decision_type:
                ga_event["params"]["decision_type"] = decision_type

            # Add feature/experiment keys
            for key in [
                "featureKey",
                "experimentKey",
                "flagKey",
                "ruleKey",
                "variationKey",
            ]:
                if key in decision:
                    ga_event["params"][key] = decision[key]

            # Add enabled flag
            if "enabled" in decision:
                ga_event["params"]["enabled"] = decision["enabled"]

            # Add decision event dispatched flag
            if "decision_event_dispatched" in decision:
                ga_event["params"]["decision_event_dispatched"] = decision[
                    "decision_event_dispatched"
                ]

            # Process variables if available
            variables = decision.get("variables", {})
            if variables and isinstance(variables, dict):
                for var_key, var_value in variables.items():
                    # Prefix variable keys to avoid conflicts
                    param_key = f"var_{var_key}"

                    # Ensure values are of supported types
                    if isinstance(var_value, (str, int, float, bool)):
                        ga_event["params"][param_key] = var_value
                    else:
                        # Convert other types to string
                        ga_event["params"][param_key] = str(var_value)

    elif notification_type == "track":
        # Handle track notification

        # Add event key and name
        event_key = event_data.get("eventKey")
        if event_key:
            ga_event["params"]["event_key"] = event_key

        event_name = event_data.get("eventName")
        if event_name:
            ga_event["params"]["event_name"] = event_name

        # Add experiment IDs if available
        experiment_ids = event_data.get("experimentIds", [])
        if experiment_ids and isinstance(experiment_ids, list):
            ga_event["params"]["experiment_ids"] = ",".join(
                str(id) for id in experiment_ids
            )

        # Process event tags if available
        event_tags = event_data.get("eventTags", {})
        if event_tags and isinstance(event_tags, dict):
            # Handle revenue as a special case for GA4's "value" parameter
            if "revenue" in event_tags:
                try:
                    revenue = float(event_tags["revenue"])
                    ga_event["params"]["value"] = revenue
                except (ValueError, TypeError):
                    logger.warning(f"Invalid revenue value: {event_tags['revenue']}")

            # Process other event tags
            for tag_key, tag_value in event_tags.items():
                if tag_key != "revenue":  # Skip revenue as it's already handled
                    # Prefix tag keys to avoid conflicts
                    param_key = f"tag_{tag_key}"

                    # Ensure values are of supported types
                    if isinstance(tag_value, (str, int, float, bool)):
                        ga_event["params"][param_key] = tag_value
                    else:
                        # Convert other types to string
                        ga_event["params"][param_key] = str(tag_value)

    # Add timestamp if available (convert to microseconds for GA4)
    if "timestamp" in event_data:
        try:
            # Convert milliseconds to microseconds
            timestamp_micros = int(event_data["timestamp"]) * 1000
            ga_event["params"]["timestamp_micros"] = timestamp_micros
        except (ValueError, TypeError):
            logger.warning(f"Invalid timestamp: {event_data['timestamp']}")

    return ga_event


def send_to_google_analytics(event_data: Dict[str, Any]) -> bool:
    """
    Send Optimizely notification data to Google Analytics.

    Args:
        event_data: The notification event data from Optimizely

    Returns:
        Boolean indicating success or failure
    """
    # Validate input
    if not isinstance(event_data, dict):
        logger.error(f"Invalid event_data type: {type(event_data)}, expected dict")
        return False

    # Get GA configuration
    config = get_ga_config()

    # Check if GA is configured
    if not config["measurement_id"] or not config["api_secret"]:
        logger.warning("Google Analytics is not configured. Skipping event.")
        return False

    try:
        # Transform Optimizely data for GA
        ga_event = transform_optimizely_data(event_data)

        # Add required parameters for Realtime reporting
        if "params" in ga_event:
            # Add session_id if not present
            if "session_id" not in ga_event["params"]:
                ga_event["params"]["session_id"] = str(
                    int(time.time() * 1000)
                )  # Generate session_id in milliseconds

            # Add engagement_time_msec if not present
            if "engagement_time_msec" not in ga_event["params"]:
                ga_event["params"]["engagement_time_msec"] = 100

        # Prepare the payload
        payload = {
            "client_id": event_data.get(
                "userId", "optimizely-agent"
            ),  # Use userId as client_id if available
            "events": [ga_event],
        }

        # Add user_id if available
        user_id = event_data.get("userId")
        if user_id:
            payload["user_id"] = str(user_id)

        # Construct URL with required parameters
        url = f"{config['endpoint_url']}?measurement_id={config['measurement_id']}&api_secret={config['api_secret']}"

        # Implement retries for resilience
        for attempt in range(MAX_RETRIES):
            try:
                # Send data to GA with timeout
                response = requests.post(
                    url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=REQUEST_TIMEOUT,
                )

                if response.status_code == 204 or response.status_code == 200:
                    logger.info(
                        f"Successfully sent to Google Analytics: {ga_event['name']}"
                    )

                    # Log response details if available
                    if response.text:
                        logger.debug(f"GA Response: {response.text}")

                    return True
                elif response.status_code == 429:  # Rate limiting
                    if attempt < MAX_RETRIES - 1:
                        # Exponential backoff: 1s, 2s, 4s
                        backoff = 2**attempt
                        logger.warning(
                            f"Rate limited by Google Analytics. Retrying in {backoff}s..."
                        )
                        time.sleep(backoff)
                        continue
                    else:
                        logger.error(
                            f"Failed to send to Google Analytics after {MAX_RETRIES} retries: {response.status_code} - {response.text}"
                        )
                        return False
                else:
                    logger.error(
                        f"Failed to send to Google Analytics: {response.status_code} - {response.text}"
                    )
                    return False

            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(
                        f"Timeout connecting to Google Analytics. Retry {attempt+1}/{MAX_RETRIES}"
                    )
                    continue
                else:
                    logger.error("Google Analytics request timed out after all retries")
                    return False
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error sending to Google Analytics: {str(e)}")
                return False

    except Exception as e:
        logger.error(f"Error sending to Google Analytics: {str(e)}")
        return False


# For testing the module directly
if __name__ == "__main__":
    # Configure logging for testing
    logging.basicConfig(level=logging.DEBUG)
    logger.info("Running Google Analytics integration module test")

    # Sample Optimizely decision notification (feature flag)
    # TODO: Check the shapes of these events
    decision_notification = {
        "type": "decision",
        "userId": "test-user-123",
        "decisionType": "flag",
        "attributes": {
            "device": "mobile",
            "location": "australia",
            "browser": "chrome",
        },
        "decision": {
            "featureKey": "homepage_redesign",
            "flagKey": "homepage_redesign",
            "ruleKey": "experiment_rule",
            "variationKey": "variation_1",
            "enabled": True,
            "decision_event_dispatched": True,
            "variables": {
                "button_color": "blue",
                "show_promo": True,
                "discount_percentage": 15,
            },
        },
    }

    # Sample Optimizely track notification
    track_notification = {
        "type": "track",
        "userId": "test-user-123",
        "eventKey": "purchase_completed",
        "eventName": "Purchase Completed",
        "attributes": {
            "device": "mobile",
            "location": "australia",
            "browser": "chrome",
        },
        "experimentIds": ["exp_1", "exp_2"],
        "eventTags": {
            "revenue": 99.99,
            "items": 3,
            "category": "travel",
            "destination": "bali",
        },
    }

    # Test both notification types
    logger.info("Testing decision notification...")
    decision_result = send_to_google_analytics(decision_notification)
    logger.info(
        f"Decision notification test result: {'Success' if decision_result else 'Failed'}"
    )

    logger.info("\nTesting track notification...")
    track_result = send_to_google_analytics(track_notification)
    logger.info(
        f"Track notification test result: {'Success' if track_result else 'Failed'}"
    )

    # Test invalid input
    logger.info("\nTesting invalid input...")
    invalid_result = send_to_google_analytics("not a dictionary")
    logger.info(
        f"Invalid input test result: {'Success' if not invalid_result else 'Failed'}"
    )

    logger.info("\nGoogle Analytics integration module test completed")
