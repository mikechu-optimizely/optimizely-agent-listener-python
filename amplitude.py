#!/usr/bin/env python
"""
Amplitude Integration Module
---------------------------
This module handles sending Optimizely Agent notification data to Amplitude.

This module provides functionality to:
1. Transform Optimizely notification data into Amplitude-compatible format
2. Send events to Amplitude's HTTP V2 API
3. Support event deduplication via insert_id
4. Handle different Optimizely notification types (decision, track, etc.)
"""

import os
import logging
import requests
import hashlib
import json
from typing import Dict, Any, Tuple
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
REQUEST_TIMEOUT = 10  # seconds
MAX_RETRIES = 3


def get_amplitude_config() -> Dict[str, str]:
    """
    Get Amplitude configuration from environment variables.
    
    Returns:
        Dict containing Amplitude configuration with the following keys:
        - api_key: The Amplitude API key
        - tracking_url: The Amplitude API endpoint URL
    
    Raises:
        Warning logs if API key is not set
    """
    api_key = os.environ.get("AMPLITUDE_API_KEY", "")
    
    # Check if API key is set
    if not api_key:
        logger.warning("AMPLITUDE_API_KEY environment variable is not set")
    
    # Default endpoint (US/Global)
    default_endpoint = "https://api2.amplitude.com/2/httpapi"
    
    # Get the tracking URL from environment or use the default
    tracking_url = os.environ.get("AMPLITUDE_TRACKING_URL", default_endpoint)
    
    return {
        "api_key": api_key,
        "tracking_url": tracking_url,
    }


def extract_user_id(event_data: Dict[str, Any]) -> str:
    """
    Extract the user ID from Optimizely notification data.

    Args:
        event_data: The notification event data from Optimizely

    Returns:
        The user ID as a string
    """
    if not isinstance(event_data, dict):
        logger.warning(f"Invalid event_data type: {type(event_data)}, expected dict")
        return "anonymous"
        
    # Different notification types store user ID in different locations
    notification_type = event_data.get("type", "unknown")

    # Direct userId field (common in decision events)
    if "userId" in event_data:
        return str(event_data["userId"])

    # For decision events, check in userContext
    if notification_type == "decision" and "userContext" in event_data:
        user_context = event_data["userContext"]
        if isinstance(user_context, dict) and "userId" in user_context:
            return str(user_context["userId"])

    # For track events, check in user object
    if notification_type == "track" and "user" in event_data:
        user = event_data["user"]
        if isinstance(user, dict) and "id" in user:
            return str(user["id"])

    # If we can't find a user ID, use a default
    return "anonymous"


def extract_notification_specific_data(event_data: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    Extract notification type-specific data from Optimizely event data.
    
    This helper function centralizes the logic for extracting data based on notification type,
    which is used in multiple places throughout the module.
    
    Args:
        event_data: The notification event data from Optimizely
        
    Returns:
        Tuple containing (notification_type, extracted_data_dict)
    """
    if not isinstance(event_data, dict):
        logger.warning(f"Invalid event_data type: {type(event_data)}, expected dict")
        return "unknown", {}
        
    notification_type = event_data.get("type", "unknown")
    extracted_data = {}
    
    # Handle decision notifications
    # Structure based on: https://docs.developers.optimizely.com/feature-experimentation/docs/decision-notification-listener
    if notification_type == "decision":
        # Extract decision info
        if "decision" in event_data:
            decision = event_data["decision"]
            if not isinstance(decision, dict):
                logger.warning(f"Invalid decision type: {type(decision)}, expected dict")
                return notification_type, {}
                
            # Extract standard decision fields
            extracted_data = {
                "feature_key": decision.get("featureKey", ""),
                "rule_key": decision.get("ruleKey", ""),
                "variation_key": decision.get("variationKey", ""),
                "enabled": decision.get("enabled", False),
                "flag_key": decision.get("flagKey", decision.get("featureKey", "")),
            }
            
            # Add decision_event_dispatched if available
            if "decision_event_dispatched" in decision:
                extracted_data["decision_event_dispatched"] = decision.get("decision_event_dispatched", False)
            
            # Add variables if available
            if "variables" in decision and decision["variables"]:
                extracted_data["variables"] = decision["variables"]
                
        # Extract decision type if available (flag, experiment, etc.)
        if "decisionType" in event_data:
            extracted_data["decision_type"] = event_data["decisionType"]
            
    # Handle track notifications
    # Structure based on: https://docs.developers.optimizely.com/feature-experimentation/docs/track-notification-listener
    elif notification_type == "track":
        # Basic track event data
        extracted_data = {
            "event_key": event_data.get("eventKey", ""),
            "event_name": event_data.get("eventName", event_data.get("eventKey", "")),
        }
        
        # Add experiment IDs if available
        if "experimentIds" in event_data:
            extracted_data["experiment_ids"] = event_data.get("experimentIds", [])
            
        # Add event tags if available
        if "eventTags" in event_data:
            if isinstance(event_data["eventTags"], dict):
                # Store event tags both as a separate field and also merge them into the main properties
                extracted_data["event_tags"] = event_data["eventTags"]
                extracted_data.update(event_data["eventTags"])
            else:
                logger.warning(f"Invalid eventTags type: {type(event_data['eventTags'])}, expected dict")
                
        # Add revenue data if available in event tags
        if "eventTags" in event_data and isinstance(event_data["eventTags"], dict):
            if "revenue" in event_data["eventTags"]:
                try:
                    extracted_data["revenue"] = float(event_data["eventTags"]["revenue"])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid revenue value: {event_data['eventTags']['revenue']}")
    
    return notification_type, extracted_data


def extract_user_properties(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract user properties from Optimizely event data.
    
    Args:
        event_data: The notification event data from Optimizely
        
    Returns:
        Dictionary of user properties
    """
    if not isinstance(event_data, dict):
        return {}
        
    user_properties = {}
    if "userContext" in event_data and isinstance(event_data["userContext"], dict):
        if "attributes" in event_data["userContext"]:
            attributes = event_data["userContext"]["attributes"]
            if isinstance(attributes, dict):
                user_properties = attributes
            else:
                logger.warning(f"Invalid attributes type: {type(attributes)}, expected dict")
    return user_properties


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
    notification_type, type_specific_data = extract_notification_specific_data(event_data)
    user_id = extract_user_id(event_data)
    
    # Create a dictionary of key fields that identify this specific event
    dedup_data = {
        "type": notification_type,
        "user_id": user_id,
        "timestamp": event_data.get("timestamp", ""),
    }
    
    # Add type-specific identifiers
    if type_specific_data:
        if notification_type == "decision":
            dedup_data.update({
                "feature_key": type_specific_data.get("feature_key", ""),
                "rule_key": type_specific_data.get("rule_key", ""),
                "variation_key": type_specific_data.get("variation_key", ""),
            })
        elif notification_type == "track":
            dedup_data.update({
                "event_key": type_specific_data.get("event_key", ""),
            })
    
    # Generate a stable hash of this data
    hash_obj = hashlib.md5()
    
    # Sort keys for deterministic output
    for key in sorted(dedup_data.keys()):
        value = dedup_data[key]
        # Handle different value types appropriately
        if isinstance(value, dict):
            value = json.dumps(value, sort_keys=True)
        hash_obj.update(f"{key}:{value}".encode())
        
    return hash_obj.hexdigest()


def transform_optimizely_data(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform Optimizely notification data into a format suitable for Amplitude.

    Args:
        event_data: The notification event data from Optimizely

    Returns:
        Dict containing transformed data for Amplitude
    """
    # Extract common data using our helper functions
    notification_type, type_specific_data = extract_notification_specific_data(event_data)
    user_id = extract_user_id(event_data)
    user_properties = extract_user_properties(event_data)
    
    # Base event properties
    event_properties = {
        "notification_type": notification_type,
        **type_specific_data  # Spread the type-specific data into event properties
    }

    # Generate an insert_id for event deduplication
    insert_id = generate_insert_id(event_data)

    # Get current timestamp in milliseconds
    current_time = int(datetime.now().timestamp() * 1000)

    # Final Amplitude event format
    amplitude_event = {
        "event_type": f"optimizely_{notification_type}",
        "user_id": user_id,
        "user_properties": user_properties,
        "event_properties": event_properties,
        "time": current_time,
        "insert_id": insert_id,
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
    # Validate input
    if not isinstance(event_data, dict):
        logger.error(f"Invalid event_data type: {type(event_data)}, expected dict")
        return False
        
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
            "options": {},
            "client_upload_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }

        # Implement retries for resilience
        for attempt in range(MAX_RETRIES):
            try:
                # Send data to Amplitude with timeout
                response = requests.post(
                    config["tracking_url"],
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=REQUEST_TIMEOUT
                )
                
                if response.status_code == 200:
                    response_data = response.json()
                    logger.info(
                        f"Successfully sent to Amplitude: {amplitude_event['event_type']} - Server response: {response_data}"
                    )
                    return True
                elif response.status_code == 429:  # Rate limiting
                    if attempt < MAX_RETRIES - 1:
                        # Exponential backoff: 1s, 2s, 4s
                        import time
                        backoff = 2 ** attempt
                        logger.warning(f"Rate limited by Amplitude. Retrying in {backoff}s...")
                        time.sleep(backoff)
                        continue
                    else:
                        logger.error(f"Failed to send to Amplitude after {MAX_RETRIES} retries: {response.status_code} - {response.text}")
                        return False
                else:
                    logger.error(
                        f"Failed to send to Amplitude: {response.status_code} - {response.text}"
                    )
                    return False
                    
            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"Timeout connecting to Amplitude. Retry {attempt+1}/{MAX_RETRIES}")
                    continue
                else:
                    logger.error("Amplitude request timed out after all retries")
                    return False
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error sending to Amplitude: {str(e)}")
                return False

    except Exception as e:
        logger.error(f"Error sending to Amplitude: {str(e)}")
        return False


# For testing the module directly
if __name__ == "__main__":
    # Configure logging for testing
    logging.basicConfig(level=logging.DEBUG)
    logger.info("Running Amplitude integration module test")
    
    # Sample Optimizely decision notification (feature flag)
    decision_notification = {
        "type": "decision",
        "userId": "test-user-123",
        "decisionType": "flag",
        "attributes": {
            "device": "mobile",
            "location": "australia",
            "browser": "chrome"
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
                "discount_percentage": 15
            }
        }
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
            "browser": "chrome"
        },
        "experimentIds": ["exp_1", "exp_2"],
        "eventTags": {
            "revenue": 99.99,
            "items": 3,
            "category": "travel",
            "destination": "bali"
        }
    }
    
    # Test both notification types
    logger.info("Testing decision notification...")
    decision_result = send_to_amplitude(decision_notification)
    logger.info(f"Decision notification test result: {'Success' if decision_result else 'Failed'}")
    
    logger.info("\nTesting track notification...")
    track_result = send_to_amplitude(track_notification)
    logger.info(f"Track notification test result: {'Success' if track_result else 'Failed'}")
    
    # Test invalid input
    logger.info("\nTesting invalid input...")
    invalid_result = send_to_amplitude("not a dictionary")
    logger.info(f"Invalid input test result: {'Success' if not invalid_result else 'Failed'}")
    
    logger.info("\nAmplitude integration module test completed")
