#!/usr/bin/env python
"""
Amplitude Integration Module
---------------------------
This module handles sending Optimizely Agent notification data to Amplitude.
"""

import os
import logging
import requests
import hashlib
import json
from typing import Dict, Any
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Amplitude API endpoint
AMPLITUDE_ENDPOINT = "https://api2.amplitude.com/2/httpapi"


def get_amplitude_config() -> Dict[str, str]:
    """
    Get Amplitude configuration from environment variables.

    Returns:
        Dict containing Amplitude configuration
    """
    return {
        "api_key": os.environ.get("AMPLITUDE_API_KEY", ""),
        "tracking_url": os.environ.get("AMPLITUDE_TRACKING_URL", AMPLITUDE_ENDPOINT),
    }


def extract_user_id(event_data: Dict[str, Any]) -> str:
    """
    Extract the user ID from Optimizely notification data.

    Args:
        event_data: The notification event data from Optimizely

    Returns:
        The user ID as a string
    """
    # Different notification types store user ID in different locations
    notification_type = event_data.get("type", "unknown")

    # Direct userId field (common in decision events)
    if "userId" in event_data:
        return event_data["userId"]

    # For decision events, check in userContext
    if notification_type == "decision" and "userContext" in event_data:
        user_context = event_data["userContext"]
        if isinstance(user_context, dict) and "userId" in user_context:
            return user_context["userId"]

    # For track events, check in user object
    if notification_type == "track" and "user" in event_data:
        user = event_data["user"]
        if isinstance(user, dict) and "id" in user:
            return user["id"]

    # If we can't find a user ID, use a default
    return "anonymous"


def generate_insert_id(event_data: Dict[str, Any]) -> str:
    """
    Generate a deterministic insert_id for event deduplication.
    
    The insert_id is a hash of key event data that uniquely identifies this event,
    ensuring that if the same event is sent multiple times, Amplitude will only count it once.
    
    Args:
        event_data: The notification event data from Optimizely
        
    Returns:
        A unique hash string to use as insert_id
    """
    # Extract key identifying information
    notification_type = event_data.get("type", "unknown")
    user_id = extract_user_id(event_data)
    
    # Create a dictionary of key fields that identify this specific event
    dedup_data = {
        "type": notification_type,
        "user_id": user_id,
        "timestamp": event_data.get("timestamp", ""),
    }
    
    # Add type-specific identifiers
    if notification_type == "decision":
        if "decision" in event_data:
            decision = event_data["decision"]
            dedup_data.update({
                "feature_key": decision.get("featureKey", ""),
                "rule_key": decision.get("ruleKey", ""),
                "variation_key": decision.get("variationKey", ""),
            })
    elif notification_type == "track":
        dedup_data.update({
            "event_key": event_data.get("eventKey", ""),
        })
    
    # Generate a stable hash of this data
    hash_input = json.dumps(dedup_data, sort_keys=True).encode()
    return hashlib.md5(hash_input).hexdigest()


def transform_optimizely_data(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform Optimizely notification data into a format suitable for Amplitude.

    Args:
        event_data: The notification event data from Optimizely

    Returns:
        Dict containing transformed data for Amplitude
    """
    # Get notification type
    notification_type = event_data.get("type", "unknown")

    # Extract user ID from the appropriate location in the event data
    user_id = extract_user_id(event_data)

    # Add user properties if available
    user_properties = {}
    if "userContext" in event_data and "attributes" in event_data["userContext"]:
        user_properties = event_data["userContext"]["attributes"]

    # Base event properties
    event_properties = {
        "notification_type": notification_type,
    }

    # Handle different notification types
    if notification_type == "decision":
        if "decision" in event_data:
            decision = event_data["decision"]
            event_properties.update(
                {
                    "feature_key": decision.get("featureKey", ""),
                    "rule_key": decision.get("ruleKey", ""),
                    "variation_key": decision.get("variationKey", ""),
                }
            )

            # Add variables if available
            if "variables" in decision and decision["variables"]:
                event_properties["variables"] = decision["variables"]

    elif notification_type == "track":
        event_properties.update(
            {
                "event_key": event_data.get("eventKey", ""),
            }
        )

        # Add event tags if available
        if "eventTags" in event_data and event_data["eventTags"]:
            event_properties.update(event_data["eventTags"])

    # Generate an insert_id for event deduplication
    insert_id = generate_insert_id(event_data)

    # Final Amplitude event format
    amplitude_event = {
        "event_type": f"optimizely_{notification_type}",
        "user_id": user_id,
        "user_properties": user_properties,
        "event_properties": event_properties,
        "time": int(datetime.now().timestamp() * 1000),  # Current time in milliseconds
        "insert_id": insert_id,  # Add insert_id for deduplication
        # Add additional Body Parameters if needed
        # https://amplitude.com/docs/apis/analytics/http-v2#body-parameters
    }

    # Log the user ID and insert_id being sent
    logger.debug(f"Sending to Amplitude with user_id: {user_id}, insert_id: {insert_id}")

    return amplitude_event


def send_to_amplitude(event_data: Dict[str, Any]) -> bool:
    """
    Send Optimizely notification data to Amplitude.

    Args:
        event_data: The notification event data from Optimizely

    Returns:
        Boolean indicating success or failure
    """
    # Get Amplitude configuration
    config = get_amplitude_config()

    # Check if Amplitude is configured
    if not config["api_key"]:
        logger.warning("Amplitude is not configured. Skipping event.")
        return False

    try:
        # Transform Optimizely data for Amplitude
        amplitude_event = transform_optimizely_data(event_data)

        # Prepare the payload according to Amplitude HTTP V2 API
        # https://amplitude.com/docs/apis/analytics/http-v2
        payload = {
            "api_key": config["api_key"],
            "events": [amplitude_event],
            "options": {},  # Optional configuration parameters
            "client_upload_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }

        # Send data to Amplitude
        response = requests.post(
            config["tracking_url"],
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        if response.status_code == 200:
            response_data = response.json()
            logger.info(
                f"Successfully sent to Amplitude: {amplitude_event['event_type']} - Server response: {response_data}"
            )
            return True
        else:
            logger.error(
                f"Failed to send to Amplitude: {response.status_code} - {response.text}"
            )
            return False

    except Exception as e:
        logger.error(f"Error sending to Amplitude: {str(e)}")
        return False


# For testing the module directly
if __name__ == "__main__":
    # Sample Optimizely decision notification
    sample_notification = {
        "type": "decision",
        "userId": "test-user",
        "decision": {
            "featureKey": "feature_flag_1",
            "ruleKey": "rule_1",
            "variationKey": "variation_1",
            "variables": {"variable_1": "value_1"},
        },
    }

    # Test sending to Amplitude
    send_to_amplitude(sample_notification)
