#!/usr/bin/env python
"""
Amplitude Integration Module
---------------------------
This module handles sending Optimizely Agent notification data to Amplitude.
"""

import os
import json
import logging
import requests
from typing import Dict, Any, Optional
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
        "tracking_url": os.environ.get("AMPLITUDE_TRACKING_URL", AMPLITUDE_ENDPOINT)
    }

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
    
    # Get user ID, using a default if not available
    user_id = event_data.get("userId", "anonymous")
    
    # Base event properties
    event_properties = {
        "notification_type": notification_type,
    }
    
    # Handle different notification types
    if notification_type == "decision":
        if "decision" in event_data:
            decision = event_data["decision"]
            event_properties.update({
                "feature_key": decision.get("featureKey", ""),
                "rule_key": decision.get("ruleKey", ""),
                "variation_key": decision.get("variationKey", ""),
            })
            
            # Add variables if available
            if "variables" in decision and decision["variables"]:
                event_properties["variables"] = decision["variables"]
    
    elif notification_type == "track":
        event_properties.update({
            "event_key": event_data.get("eventKey", ""),
        })
        
        # Add event tags if available
        if "eventTags" in event_data and event_data["eventTags"]:
            event_properties.update(event_data["eventTags"])
    
    # Final Amplitude event format
    amplitude_event = {
        "event_type": f"optimizely_{notification_type}",
        "user_id": user_id,
        "event_properties": event_properties,
        "time": int(datetime.now().timestamp() * 1000)  # Current time in milliseconds
    }
    
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
        
        # Prepare the payload
        payload = {
            "api_key": config["api_key"],
            "events": [amplitude_event],
            "client_upload_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        }
        
        # Send data to Amplitude asynchronously
        response = requests.post(
            config["tracking_url"], 
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            logger.info(f"Successfully sent to Amplitude: {amplitude_event['event_type']}")
            return True
        else:
            logger.error(f"Failed to send to Amplitude: {response.status_code} - {response.text}")
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
            "variables": {
                "variable_1": "value_1"
            }
        }
    }
    
    # Test sending to Amplitude
    send_to_amplitude(sample_notification)
