#!/usr/bin/env python
"""
Notification Processor Module
---------------------------
This module handles the processing of Optimizely notification events.
"""

import json
import logging
from typing import Dict, Any, Optional, List, Tuple

from google_analytics import send_to_google_analytics
from amplitude import send_to_amplitude

# Set up logging
logger = logging.getLogger(__name__)

class NotificationProcessor:
    """
    Processes Optimizely notification events and forwards them to analytics platforms.
    """
    
    def __init__(self, ga_enabled: bool = True, amplitude_enabled: bool = True):
        """
        Initialize the notification processor.
        
        Args:
            ga_enabled: Whether to enable Google Analytics forwarding
            amplitude_enabled: Whether to enable Amplitude forwarding
        """
        self.ga_enabled = ga_enabled
        self.amplitude_enabled = amplitude_enabled
        
    async def process_notification(self, event):
        """
        Process a notification event from the Optimizely Agent.
        
        Args:
            event: The event object from the SSE client
            
        Returns:
            A tuple of (success, event_data)
        """
        try:
            # Parse the event data
            event_data = json.loads(event.data)
            
            # Extract user ID for logging
            user_id = "unknown"
            if "UserContext" in event_data and "ID" in event_data["UserContext"]:
                user_id = event_data["UserContext"]["ID"]
            elif "userId" in event_data:
                user_id = event_data["userId"]
            
            # Extract notification type
            notification_type = "unknown"
            if "Type" in event_data:
                notification_type = event_data["Type"]
            elif "type" in event_data:
                notification_type = event_data["type"]
            
            logger.info(f"Received notification type: {notification_type} for user: {user_id}")
            
            # Process based on notification type
            if notification_type == "flag" and "DecisionInfo" in event_data:
                # This is a feature flag decision
                decision_info = event_data["DecisionInfo"]
                flag_key = decision_info.get("flagKey", "unknown")
                variation_key = decision_info.get("variationKey", "unknown")
                
                logger.info(f"Feature flag decision: {flag_key} -> {variation_key}")
                
                # Log variables if they exist
                if "variables" in decision_info:
                    logger.info(f"Variables: {decision_info['variables']}")
            
            # Forward to analytics platforms
            success_count = 0
            total_platforms = 0
            
            # Send to Google Analytics if enabled
            if self.ga_enabled:
                total_platforms += 1
                try:
                    ga_result = await send_to_google_analytics(event_data)
                    if ga_result:
                        success_count += 1
                        logger.debug(f"Successfully sent to Google Analytics: {user_id}")
                    else:
                        logger.warning(f"Failed to send to Google Analytics: {user_id}")
                except Exception as e:
                    logger.error(f"Error sending to Google Analytics: {str(e)}")
            
            # Send to Amplitude if enabled
            if self.amplitude_enabled:
                total_platforms += 1
                try:
                    amplitude_result = await send_to_amplitude(event_data)
                    if amplitude_result:
                        success_count += 1
                        logger.debug(f"Successfully sent to Amplitude: {user_id}")
                    else:
                        logger.warning(f"Failed to send to Amplitude: {user_id}")
                except Exception as e:
                    logger.error(f"Error sending to Amplitude: {str(e)}")
            
            # Log the processing result
            logger.info(f"Notification processed - Success: {success_count}/{total_platforms}")
            
            # If no platforms are configured, log a warning
            if total_platforms == 0:
                logger.info("Event received but no analytics services are configured for forwarding")
            
            return True, event_data
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse notification data: {str(e)}")
            return False, None
        except Exception as e:
            logger.error(f"Error processing notification: {str(e)}")
            return False, None
    
    def check_analytics_config(self) -> List[str]:
        """
        Check the analytics configuration and return warnings.
        
        Returns:
            A list of warning messages
        """
        warnings = []
        
        # Check Google Analytics configuration
        if self.ga_enabled:
            import os
            ga_measurement_id = os.getenv("GA_MEASUREMENT_ID")
            ga_api_secret = os.getenv("GA_API_SECRET")
            
            if not ga_measurement_id or ga_measurement_id == "your_ga_measurement_id":
                warnings.append("Google Analytics tracking disabled - GA_MEASUREMENT_ID not set or contains placeholder value")
                self.ga_enabled = False
            
            if not ga_api_secret or ga_api_secret == "your_ga_api_secret":
                warnings.append("Google Analytics tracking disabled - GA_API_SECRET not set or contains placeholder value")
                self.ga_enabled = False
        
        # Check Amplitude configuration
        if self.amplitude_enabled:
            import os
            amplitude_api_key = os.getenv("AMPLITUDE_API_KEY")
            
            if not amplitude_api_key or amplitude_api_key == "your_amplitude_api_key":
                warnings.append("Amplitude tracking disabled - AMPLITUDE_API_KEY not set or contains placeholder value")
                self.amplitude_enabled = False
        
        # Add a summary warning if no platforms are configured
        if not self.ga_enabled and not self.amplitude_enabled:
            warnings.append("No analytics platforms are properly configured. Events will be received but not forwarded.")
        
        return warnings
