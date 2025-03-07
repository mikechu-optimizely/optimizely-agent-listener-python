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
from typing import Dict, Any, Optional

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Analytics Measurement Protocol endpoints
GA4_ENDPOINT = "https://www.google-analytics.com/mp/collect"
GA4_DEBUG_ENDPOINT = "https://www.google-analytics.com/debug/mp/collect"

def get_ga_config() -> Dict[str, str]:
    """
    Get Google Analytics configuration from environment variables.
    
    Returns:
        Dict containing GA configuration
    """
    return {
        "measurement_id": os.environ.get("GA_MEASUREMENT_ID", ""),
        "api_secret": os.environ.get("GA_API_SECRET", ""),
        "debug_mode": os.environ.get("GA_DEBUG_MODE", "false").lower() == "true"
    }

def transform_optimizely_data(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform Optimizely notification data into a format suitable for Google Analytics.
    
    Args:
        event_data: The notification event data from Optimizely
        
    Returns:
        Dict containing transformed data for GA
    """
    # Get notification type
    notification_type = event_data.get("type", "unknown")
    
    # Base event data
    ga_event = {
        "name": f"optimizely_{notification_type}",
        "params": {
            "notification_type": notification_type,
        }
    }
    
    # Handle different notification types
    if notification_type == "decision":
        if "decision" in event_data:
            decision = event_data["decision"]
            ga_event["params"].update({
                "feature_key": decision.get("featureKey", ""),
                "rule_key": decision.get("ruleKey", ""),
                "variation_key": decision.get("variationKey", ""),
                "user_id": event_data.get("userId", "")
            })
            
            # Add variables if available
            if "variables" in decision and decision["variables"]:
                ga_event["params"]["variables"] = json.dumps(decision["variables"])
    
    elif notification_type == "track":
        ga_event["params"].update({
            "event_key": event_data.get("eventKey", ""),
            "user_id": event_data.get("userId", "")
        })
        
        # Add event tags if available
        if "eventTags" in event_data and event_data["eventTags"]:
            ga_event["params"]["event_tags"] = json.dumps(event_data["eventTags"])
    
    return ga_event

def send_to_google_analytics(event_data: Dict[str, Any]) -> bool:
    """
    Send Optimizely notification data to Google Analytics.
    
    Args:
        event_data: The notification event data from Optimizely
        
    Returns:
        Boolean indicating success or failure
    """
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
        endpoint = GA4_DEBUG_ENDPOINT if config["debug_mode"] else GA4_ENDPOINT
        url = f"{endpoint}?measurement_id={config['measurement_id']}&api_secret={config['api_secret']}"
        
        # Send data to GA
        response = requests.post(url, json=payload)
        
        if response.status_code == 204 or response.status_code == 200:
            logger.info(f"Successfully sent to Google Analytics: {ga_event['name']}")
            
            # Log validation messages if in debug mode
            if config["debug_mode"] and response.text:
                logger.debug(f"GA Debug Response: {response.text}")
                
            return True
        else:
            logger.error(f"Failed to send to Google Analytics: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error sending to Google Analytics: {str(e)}")
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
            "variables": {
                "variable_1": "value_1"
            }
        }
    }
    
    # Test sending to GA
    send_to_google_analytics(sample_notification)
