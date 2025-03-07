#!/usr/bin/env python
"""
Optimizely Agent Notification Center
-----------------------------------
This script listens for Server-Sent Events (SSE) from the Optimizely Agent and
forwards the notifications to Google Analytics and Amplitude.
"""

import json
import os
import sys
import threading
import logging
from sseclient import SSEClient
from urllib.parse import urljoin
import time
from pathlib import Path

# Import the custom modules for analytics tracking
from google_analytics import send_to_google_analytics
from amplitude import send_to_amplitude

# Try to load environment variables from .env file if it exists (for local development)
env_path = Path('.') / '.env'
if env_path.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("Loaded environment variables from .env file")
    except ImportError:
        print("dotenv package not installed. Install it with: pip install python-dotenv")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_notification(notification):
    """
    Process the notification and send it to analytics platforms.
    
    Args:
        notification: The notification event from Optimizely Agent
    """
    try:
        event_data = json.loads(notification.data)
        logger.info(f"Received notification type: {event_data.get('type', 'unknown')}")
        
        # Send to Google Analytics
        send_to_google_analytics(event_data)
        
        # Send to Amplitude
        send_to_amplitude(event_data)
        
    except json.JSONDecodeError:
        logger.error(f"Failed to decode notification data: {notification.data}")
    except Exception as e:
        logger.error(f"Error processing notification: {str(e)}")

def listen_for_notifications(sdk_key, base_url, filter_type=None):
    """
    Listen for notifications from the Optimizely Agent.
    
    Args:
        sdk_key: The Optimizely SDK key
        base_url: The base URL of the Optimizely Agent
        filter_type: Optional filter for notification types (e.g., 'decision')
    """
    # Build the notification URL
    notification_endpoint = '/v1/notifications/event-stream'
    if filter_type:
        notification_endpoint += f'?filter={filter_type}'
    
    notification_url = urljoin(base_url, notification_endpoint)
    
    # Set up headers with SDK key
    headers = {
        'X-Optimizely-Sdk-Key': sdk_key,
        'Accept': 'text/event-stream'
    }
    
    logger.info(f"Connecting to SSE endpoint: {notification_url}")
    
    retry_count = 0
    max_retries = 10
    retry_delay = 5  # seconds
    
    while retry_count < max_retries:
        try:
            # Connect to the SSE stream
            messages = SSEClient(notification_url, headers=headers)
            
            # Reset retry counter after successful connection
            retry_count = 0
            
            # Process each notification
            for msg in messages:
                if msg.data:
                    process_notification(msg)
        
        except Exception as e:
            retry_count += 1
            logger.error(f"Connection error (retry {retry_count}/{max_retries}): {str(e)}")
            
            if retry_count < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached. Exiting notification listener.")
                break

def main():
    """
    Main entry point for the notification center.
    """
    # Get configuration from environment variables
    sdk_key = os.environ.get('OPTIMIZELY_SDK_KEY')
    agent_url = os.environ.get('OPTIMIZELY_AGENT_URL', 'http://localhost:8080')
    filter_type = os.environ.get('NOTIFICATION_FILTER')
    
    if not sdk_key:
        logger.error("OPTIMIZELY_SDK_KEY environment variable is required")
        sys.exit(1)
    
    logger.info("Starting Optimizely notification listener")
    logger.info(f"Agent URL: {agent_url}")
    
    # Start the notification listener in the main thread
    listen_for_notifications(sdk_key, agent_url, filter_type)

if __name__ == "__main__":
    main()