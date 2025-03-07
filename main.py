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
import logging
from urllib.parse import urljoin
import time
from pathlib import Path

# Import the custom modules for analytics tracking
from google_analytics import send_to_google_analytics
from amplitude import send_to_amplitude

# Set up logging first, so we can log any issues with environment loading
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Try to load environment variables from .env file if it exists (for local development)
env_path = Path(".") / ".env"
if env_path.exists():
    try:
        from dotenv import load_dotenv

        load_dotenv()
        logger.info("Loaded environment variables from .env file")
    except ImportError:
        logger.warning(
            "dotenv package not installed. Install it with: pip install python-dotenv"
        )


def process_notification(notification):
    """
    Process the notification and send it to analytics platforms.

    Args:
        notification: The notification event from Optimizely Agent
    """
    try:
        event_data = json.loads(notification.data)
        notification_type = event_data.get("type", "unknown")
        logger.info(f"Received notification type: {notification_type}")

        # Track processing metrics
        success_count = 0

        # Send to Google Analytics
        ga_result = send_to_google_analytics(event_data)
        if ga_result:
            success_count += 1
            logger.debug(f"Successfully sent to Google Analytics: {notification_type}")

        # Send to Amplitude
        amp_result = send_to_amplitude(event_data)
        if amp_result:
            success_count += 1
            logger.debug(f"Successfully sent to Amplitude: {notification_type}")

        logger.info(f"Notification processed - Success: {success_count}/2")

    except json.JSONDecodeError:
        logger.error(f"Failed to decode notification data: {notification.data}")
    except Exception as e:
        logger.error(f"Error processing notification: {str(e)}")


def listen_for_notifications(sdk_key, agent_url, filter_type=None):
    """
    Listen for notifications from the Optimizely Agent.

    Args:
        sdk_key: The Optimizely SDK key
        agent_url: The full URL to the Optimizely Agent notification endpoint
        filter_type: Optional filter for notification types (e.g., 'decision')
    """
    # Add filter to URL if specified
    notification_url = agent_url
    if filter_type:
        # Check if URL already has query parameters
        if "?" in agent_url:
            notification_url = f"{agent_url}&filter={filter_type}"
        else:
            notification_url = f"{agent_url}?filter={filter_type}"

    # Set up headers with SDK key
    headers = {"X-Optimizely-Sdk-Key": sdk_key, "Accept": "text/event-stream"}

    logger.info(f"Connecting to SSE endpoint: {notification_url}")
    logger.info(f"Using SDK key: {sdk_key}")

    retry_count = 0
    max_retries = 10
    retry_delay = 5  # seconds
    backoff_factor = 2  # For exponential backoff

    while retry_count < max_retries:
        try:
            # Connect to the SSE stream using requests directly
            import requests
            logger.info(f"Sending request with headers: {headers}")
            response = requests.get(notification_url, headers=headers, stream=True)
            
            # Check if the connection was successful
            if response.status_code != 200:
                logger.error(f"Server response: {response.status_code} {response.reason}")
                logger.error(f"Response content: {response.text[:500]}")  # Log first 500 chars of response
                raise Exception(f"Failed to connect to SSE endpoint: {response.status_code} {response.reason}")
            
            # Reset retry counter after successful connection
            retry_count = 0
            logger.info("Successfully connected to Optimizely Agent")
            
            # Process the SSE stream manually
            buffer = ""
            for line in response.iter_lines(decode_unicode=True):
                if line:
                    # Add the line to the buffer
                    buffer += line + "\n"
                    
                    # If we've reached the end of an event (empty line), process it
                    if line == "":
                        if "data:" in buffer:
                            # Extract the data part
                            data_parts = [part for part in buffer.split("\n") if part.startswith("data:")]
                            if data_parts:
                                data = data_parts[0].replace("data:", "").strip()
                                # Create a simple message object with a data attribute
                                class Message:
                                    def __init__(self, data):
                                        self.data = data
                                
                                msg = Message(data)
                                process_notification(msg)
                        
                        # Reset the buffer for the next event
                        buffer = ""

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Shutting down...")
            break
        except Exception as e:
            retry_count += 1
            logger.error(
                f"Connection error (retry {retry_count}/{max_retries}): {str(e)}"
            )

            if retry_count < max_retries:
                # Use exponential backoff for retries
                current_delay = retry_delay * (backoff_factor ** (retry_count - 1))
                logger.info(f"Retrying in {current_delay} seconds...")
                time.sleep(current_delay)
            else:
                logger.error("Max retries reached. Exiting notification listener.")
                break


def is_placeholder_value(value):
    """
    Check if a value appears to be a placeholder from .env.sample

    Args:
        value: The value to check

    Returns:
        True if the value appears to be a placeholder, False otherwise
    """
    if not value:
        return True

    placeholder_patterns = [
        "your_",
        "YOUR_",
        "_here",
        "_HERE",
        "example",
        "EXAMPLE",
        "placeholder",
        "PLACEHOLDER",
    ]

    return any(pattern in value for pattern in placeholder_patterns)


def main():
    """
    Main entry point for the notification center.
    """
    # Get configuration from environment variables
    sdk_key = os.environ.get("OPTIMIZELY_SDK_KEY")
    agent_url = os.environ.get(
        "OPTIMIZELY_AGENT_URL", "http://localhost:8080/v1/notifications/event-stream"
    )
    filter_type = os.environ.get("NOTIFICATION_FILTER")

    # Check if SDK key is valid
    if not sdk_key or is_placeholder_value(sdk_key):
        logger.error(
            "OPTIMIZELY_SDK_KEY environment variable is required and cannot be a placeholder value"
        )
        sys.exit(1)

    # Validate agent URL format
    if not agent_url.startswith(("http://", "https://")):
        logger.error(
            f"Invalid OPTIMIZELY_AGENT_URL format: {agent_url}. URL must start with http:// or https://"
        )
        sys.exit(1)

    logger.info("Starting Optimizely notification listener")
    logger.info(f"Agent URL: {agent_url}")

    # Log analytics configuration status
    ga_measurement_id = os.environ.get("GA_MEASUREMENT_ID")
    ga_api_secret = os.environ.get("GA_API_SECRET")
    ga_endpoint = os.environ.get("GA_ENDPOINT_URL")
    amplitude_key = os.environ.get("AMPLITUDE_API_KEY")
    amplitude_endpoint = os.environ.get("AMPLITUDE_TRACKING_URL")

    # Check Google Analytics configuration
    ga_enabled = (
        ga_measurement_id
        and ga_api_secret
        and not is_placeholder_value(ga_measurement_id)
        and not is_placeholder_value(ga_api_secret)
    )
    if ga_enabled:
        logger.info(
            f"Google Analytics tracking enabled (Endpoint: {ga_endpoint or 'default'})"
        )
    else:
        if not ga_measurement_id or is_placeholder_value(ga_measurement_id):
            logger.warning(
                "Google Analytics tracking disabled - GA_MEASUREMENT_ID not set or contains placeholder value"
            )
        if not ga_api_secret or is_placeholder_value(ga_api_secret):
            logger.warning(
                "Google Analytics tracking disabled - GA_API_SECRET not set or contains placeholder value"
            )

    # Check Amplitude configuration
    amplitude_enabled = amplitude_key and not is_placeholder_value(amplitude_key)
    if amplitude_enabled:
        logger.info(
            f"Amplitude tracking enabled (Endpoint: {amplitude_endpoint or 'default'})"
        )
    else:
        logger.warning(
            "Amplitude tracking disabled - AMPLITUDE_API_KEY not set or contains placeholder value"
        )

    # Check if at least one analytics platform is enabled
    if not ga_enabled and not amplitude_enabled:
        logger.warning(
            "No analytics platforms are properly configured. Events will be received but not forwarded."
        )

    if filter_type:
        logger.info(f"Notification filter: {filter_type}")
    else:
        logger.info("No notification filter set - listening for all notification types")

    try:
        # Start the notification listener in the main thread
        listen_for_notifications(sdk_key, agent_url, filter_type)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
