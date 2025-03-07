#!/usr/bin/env python
"""
Google Analytics Integration Module
----------------------------------
This module handles sending Optimizely Agent notification data to Google Analytics.
"""

import os
import json
import logging
import requests
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
        logger.warning("Google Analytics is not fully configured. Required: GA_MEASUREMENT_ID, GA_API_SECRET")
    
    # Default endpoint (US/Global)
    default_endpoint = "https://www.google-analytics.com/mp/collect"
    
    # Get endpoint URL from environment or use default
    endpoint_url = os.environ.get("GA_ENDPOINT_URL", default_endpoint)
    
    return {
        "measurement_id": measurement_id,
        "api_secret": api_secret,
        "endpoint_url": endpoint_url
    }

def transform_optimizely_data(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform Optimizely notification data into a format suitable for Google Analytics.
    
    Args:
        event_data: The notification event data from Optimizely
        
    Returns:
        Dict containing transformed data for GA
    """
    if not isinstance(event_data, dict):
        logger.warning(f"Invalid event_data type: {type(event_data)}, expected dict")
        return {"name": "optimizely_unknown", "params": {}}
        
    # Get notification type
    notification_type = event_data.get("type", "unknown")
    
    # Extract user ID with proper validation
    user_id = event_data.get("userId", "")
    if not isinstance(user_id, str):
        user_id = str(user_id)
    
    # Base event data
    ga_event = {
        "name": f"optimizely_{notification_type}",
        "params": {
            "notification_type": notification_type,
            "user_id": user_id
        }
    }
    
    # Handle different notification types
    if notification_type == "decision":
        # Extract decision type if available (flag, experiment, etc.)
        if "decisionType" in event_data:
            ga_event["params"]["decision_type"] = event_data["decisionType"]
            
        if "decision" in event_data:
            decision = event_data["decision"]
            if not isinstance(decision, dict):
                logger.warning(f"Invalid decision type: {type(decision)}, expected dict")
                return ga_event
                
            # Extract standard decision fields
            ga_event["params"].update({
                "feature_key": decision.get("featureKey", ""),
                "rule_key": decision.get("ruleKey", ""),
                "variation_key": decision.get("variationKey", ""),
                "flag_key": decision.get("flagKey", decision.get("featureKey", "")),
            })
            
            # Add enabled status if available
            if "enabled" in decision:
                ga_event["params"]["enabled"] = decision.get("enabled", False)
                
            # Add decision_event_dispatched if available
            if "decision_event_dispatched" in decision:
                ga_event["params"]["decision_event_dispatched"] = decision.get("decision_event_dispatched", False)
            
            # Add variables if available
            if "variables" in decision and decision["variables"]:
                # Convert variables to a string for GA
                ga_event["params"]["variables"] = json.dumps(decision["variables"])
                
                # Also add individual variables as separate parameters for better reporting
                for var_key, var_value in decision["variables"].items():
                    # Prefix with 'var_' to avoid potential conflicts
                    param_key = f"var_{var_key}"
                    # Convert complex values to strings
                    if isinstance(var_value, (dict, list)):
                        ga_event["params"][param_key] = json.dumps(var_value)
                    else:
                        ga_event["params"][param_key] = var_value
    
    elif notification_type == "track":
        # Basic track event data
        ga_event["params"].update({
            "event_key": event_data.get("eventKey", ""),
            "event_name": event_data.get("eventName", event_data.get("eventKey", "")),
        })
        
        # Add experiment IDs if available
        if "experimentIds" in event_data:
            experiment_ids = event_data.get("experimentIds", [])
            if experiment_ids:
                ga_event["params"]["experiment_ids"] = json.dumps(experiment_ids)
        
        # Add event tags if available
        if "eventTags" in event_data and event_data["eventTags"]:
            if isinstance(event_data["eventTags"], dict):
                # Store event tags as a JSON string
                ga_event["params"]["event_tags"] = json.dumps(event_data["eventTags"])
                
                # Also add individual event tags as separate parameters for better reporting
                for tag_key, tag_value in event_data["eventTags"].items():
                    # Prefix with 'tag_' to avoid potential conflicts
                    param_key = f"tag_{tag_key}"
                    # Convert complex values to strings
                    if isinstance(tag_value, (dict, list)):
                        ga_event["params"][param_key] = json.dumps(tag_value)
                    else:
                        ga_event["params"][param_key] = tag_value
                        
                # Special handling for revenue
                if "revenue" in event_data["eventTags"]:
                    try:
                        revenue = float(event_data["eventTags"]["revenue"])
                        # GA4 uses 'value' for revenue/conversion value
                        ga_event["params"]["value"] = revenue
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid revenue value: {event_data['eventTags']['revenue']}")
    
    # Add user attributes if available
    if "attributes" in event_data and isinstance(event_data["attributes"], dict):
        for attr_key, attr_value in event_data["attributes"].items():
            # Prefix with 'attr_' to avoid potential conflicts
            param_key = f"attr_{attr_key}"
            # Convert complex values to strings
            if isinstance(attr_value, (dict, list)):
                ga_event["params"][param_key] = json.dumps(attr_value)
            else:
                ga_event["params"][param_key] = attr_value
    
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
        
        # Prepare the payload
        payload = {
            "client_id": "optimizely-agent",  # Can be customized or derived from user ID
            "events": [ga_event]
        }
        
        # Determine endpoint
        url = f"{config['endpoint_url']}?measurement_id={config['measurement_id']}&api_secret={config['api_secret']}"
        
        # Implement retries for resilience
        for attempt in range(MAX_RETRIES):
            try:
                # Send data to GA with timeout
                response = requests.post(
                    url, 
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=REQUEST_TIMEOUT
                )
                
                if response.status_code == 204 or response.status_code == 200:
                    logger.info(f"Successfully sent to Google Analytics: {ga_event['name']}")
                    
                    # Log response details if available
                    if response.text:
                        logger.debug(f"GA Response: {response.text}")
                        
                    return True
                elif response.status_code == 429:  # Rate limiting
                    if attempt < MAX_RETRIES - 1:
                        # Exponential backoff: 1s, 2s, 4s
                        import time
                        backoff = 2 ** attempt
                        logger.warning(f"Rate limited by Google Analytics. Retrying in {backoff}s...")
                        time.sleep(backoff)
                        continue
                    else:
                        logger.error(f"Failed to send to Google Analytics after {MAX_RETRIES} retries: {response.status_code} - {response.text}")
                        return False
                else:
                    logger.error(f"Failed to send to Google Analytics: {response.status_code} - {response.text}")
                    return False
                    
            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"Timeout connecting to Google Analytics. Retry {attempt+1}/{MAX_RETRIES}")
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
    decision_result = send_to_google_analytics(decision_notification)
    logger.info(f"Decision notification test result: {'Success' if decision_result else 'Failed'}")
    
    logger.info("\nTesting track notification...")
    track_result = send_to_google_analytics(track_notification)
    logger.info(f"Track notification test result: {'Success' if track_result else 'Failed'}")
    
    # Test invalid input
    logger.info("\nTesting invalid input...")
    invalid_result = send_to_google_analytics("not a dictionary")
    logger.info(f"Invalid input test result: {'Success' if not invalid_result else 'Failed'}")
    
    logger.info("\nGoogle Analytics integration module test completed")
