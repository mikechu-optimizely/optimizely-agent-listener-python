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
import asyncio
from pathlib import Path
from google_analytics import send_to_google_analytics
from amplitude import send_to_amplitude
import sseclient
import requests
import aiohttp
from aiohttp_sse_client import client as sse_client

# Set up logging first, so we can log any issues with environment loading
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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


async def process_notification(notification):
    """
    Process a notification from the Optimizely Agent.
    
    Args:
        notification: The notification message object with a data attribute
    """
    try:
        # Parse the notification data
        event_data = json.loads(notification.data)
        
        # Determine notification type
        notification_type = "unknown"
        if "Type" in event_data:
            notification_type = event_data["Type"]
        elif "type" in event_data:
            notification_type = event_data["type"]
            
        logger.info(f"Received notification type: {notification_type}")
        
        # Log more detailed information about the event
        if notification_type == "flag" or "variationKey" in notification.data:
            # This is a feature flag decision event
            if "DecisionInfo" in event_data:
                decision_info = event_data["DecisionInfo"]
                flag_key = decision_info.get("flagKey", "unknown")
                variation_key = decision_info.get("variationKey", "unknown")
                logger.info(f"Feature flag decision: {flag_key} -> {variation_key}")
                
                # Log variables if present
                if "variables" in decision_info:
                    logger.info(f"Variables: {decision_info['variables']}")
        
        # Track success count for analytics services
        success_count = 0
        total_services = 0
        
        # Send to Google Analytics if configured
        ga_measurement_id = os.environ.get("GA_MEASUREMENT_ID")
        ga_api_secret = os.environ.get("GA_API_SECRET")
        if ga_measurement_id and ga_api_secret and not is_placeholder_value(ga_measurement_id) and not is_placeholder_value(ga_api_secret):
            total_services += 1
            try:
                # Create a task for sending to Google Analytics
                result = await send_to_google_analytics(event_data)
                if result:
                    success_count += 1
                    logger.debug(f"Successfully sent to Google Analytics: {notification_type}")
            except Exception as e:
                logger.error(f"Error sending to Google Analytics: {str(e)}")
        
        # Send to Amplitude if configured
        amplitude_api_key = os.environ.get("AMPLITUDE_API_KEY")
        if amplitude_api_key and not is_placeholder_value(amplitude_api_key):
            total_services += 1
            try:
                # Create a task for sending to Amplitude
                result = await send_to_amplitude(event_data)
                if result:
                    success_count += 1
                    logger.debug(f"Successfully sent to Amplitude: {notification_type}")
            except Exception as e:
                logger.error(f"Error sending to Amplitude: {str(e)}")
        
        logger.info(f"Notification processed - Success: {success_count}/{total_services}")
        
        # If no analytics services are configured, just log that we received the event
        if total_services == 0:
            logger.info("Event received but no analytics services are configured for forwarding")
            
        return True
    except Exception as e:
        logger.error(f"Error processing notification: {str(e)}")
        return False


async def listen_for_notifications(sdk_key, agent_url, filter_type=None):
    """
    Listen for notifications from the Optimizely Agent using async SSE client.
    
    Args:
        sdk_key: The Optimizely SDK key
        agent_url: The full URL to the Optimizely Agent notification endpoint
        filter_type: Optional filter for notification types (e.g., 'decision')
    """
    # Set up headers with SDK key
    headers = {
        "X-Optimizely-Sdk-Key": sdk_key, 
        "Accept": "text/event-stream",
        "User-Agent": "OptimizelyNotificationListener/1.0",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive"
    }
    
    # Construct the notification URL with filter if provided
    notification_url = agent_url
    if filter_type:
        notification_url = f"{agent_url}?filter={filter_type}"
        logger.info(f"Notification filter: {filter_type}")
    else:
        logger.info("No notification filter set - listening for all notification types")

    logger.info(f"Connecting to SSE endpoint: {notification_url}")
    logger.info(f"Using SDK key: {sdk_key}")
    
    retry_count = 0
    max_retries = 10
    
    # Create a ClientSession that will be used for all requests
    async with aiohttp.ClientSession() as session:
        # Setup heartbeat detection
        last_activity_time = time.time()
        heartbeat_interval = 2  # seconds
        
        while retry_count < max_retries:
            try:
                logger.info(f"Sending request with headers: {headers}")
                
                # Connect to the SSE endpoint using aiohttp-sse-client
                async with sse_client.EventSource(notification_url, headers=headers, session=session, timeout=60) as event_source:
                    # Reset retry counter after successful connection
                    retry_count = 0
                    logger.info("Successfully connected to Optimizely Agent")
                    logger.info("Waiting for events...")
                    
                    # Process events as they arrive
                    async for event in event_source:
                        # Update last activity time
                        last_activity_time = time.time()
                        
                        if event.data:
                            logger.debug(f"Received event data: {event.data[:100]}...")  # Log first 100 chars
                            
                            try:
                                # Check if this is a decide API event
                                if "decide" in event.data.lower() or "variationKey" in event.data:
                                    logger.info("Detected a decide API event!")
                                
                                # Process the notification - the event object already has a data attribute
                                # which is what process_notification expects
                                await process_notification(event)
                            except Exception as e:
                                logger.error(f"Error processing notification: {str(e)}")
                        else:
                            # Empty data might be a heartbeat or keep-alive
                            logger.debug("Received event with empty data (possible heartbeat)")
                        
                        # Check for heartbeat timeout
                        current_time = time.time()
                        if current_time - last_activity_time > heartbeat_interval:
                            logger.warning(f"No activity for {heartbeat_interval} seconds, checking connection...")
                            # Send a ping to the Optimizely Agent health endpoint to keep the connection alive
                            try:
                                # Extract base URL for health check
                                base_url = agent_url.split('/v1/')[0]
                                health_url = f"{base_url}/health"
                                logger.info(f"Sending ping to health endpoint: {health_url}")
                                
                                # Use the session to make the request
                                async with session.get(health_url, timeout=5) as response:
                                    logger.info(f"Health ping response: {response.status}")
                                
                                # Reset last activity time
                                last_activity_time = time.time()
                            except Exception as ping_error:
                                logger.error(f"Error sending health ping: {str(ping_error)}")
                                # Force a reconnection
                                raise Exception("Connection inactive, forcing reconnection")
            
            except asyncio.CancelledError:
                logger.info("Async task was cancelled. Shutting down...")
                break
                
            except (aiohttp.ClientError, aiohttp.ClientPayloadError, aiohttp.ClientResponseError) as e:
                retry_count += 1
                logger.error(f"SSE client error (retry {retry_count}/{max_retries}): {str(e)}")
                logger.info("This is likely due to the server closing the connection unexpectedly.")
                
                # Shorter wait time for SSE client errors as it's usually temporary
                wait_time = 2 * retry_count
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                retry_count += 1
                logger.error(f"Connection error (retry {retry_count}/{max_retries}): {str(e)}")
                
                # Check if the Agent is still running
                try:
                    # Extract base URL for health check
                    base_url = agent_url.split('/v1/')[0]
                    health_url = f"{base_url}/health"
                    logger.info(f"Checking if Optimizely Agent is still running: {health_url}")
                    
                    async with session.get(health_url, timeout=5) as response:
                        logger.info(f"Agent health check response: {response.status}")
                        
                        if response.status == 200:
                            logger.info("Optimizely Agent is still running. Will reconnect.")
                        else:
                            logger.error("Optimizely Agent health check failed. Agent may be down.")
                except Exception as health_error:
                    logger.error(f"Error checking Agent health: {str(health_error)}")
                
                # Exponential backoff for retries
                wait_time = 5 * retry_count
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
                
        logger.error("Max retries exceeded. Exiting.")


async def test_agent_connection(sdk_key, agent_base_url):
    """Test the connection to the Optimizely Agent by checking its health endpoint."""
    # Extract the base URL (remove the notification path)
    base_url = agent_base_url.split('/v1/')[0]
    health_url = f"{base_url}/health"
    
    logger.info(f"Testing connection to Optimizely Agent health endpoint: {health_url}")
    
    try:
        async with aiohttp.ClientSession() as session:
            # Test the health endpoint
            async with session.get(health_url) as response:
                logger.info(f"Health endpoint response: {response.status} {response.reason}")
                
                if response.status == 200:
                    response_text = await response.text()
                    logger.info(f"Response content: {response_text[:500]}")
                    logger.info("Optimizely Agent is healthy!")
                    
                    # Now test the configuration endpoint
                    config_url = f"{base_url}/v1/config"
                    headers = {"X-Optimizely-Sdk-Key": sdk_key}
                    
                    logger.info(f"Testing configuration endpoint: {config_url}")
                    
                    async with session.get(config_url, headers=headers) as config_response:
                        logger.info(f"Config endpoint response: {config_response.status} {config_response.reason}")
                        
                        if config_response.status == 200:
                            logger.info("Successfully retrieved configuration!")
                            return True
                        else:
                            config_text = await config_response.text()
                            logger.error(f"Failed to retrieve configuration: {config_text[:500]}")
                            return False
                else:
                    logger.error("Optimizely Agent health check failed!")
                    return False
            
    except Exception as e:
        logger.error(f"Error connecting to Optimizely Agent: {str(e)}")
        return False


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


async def main():
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
    amplitude_api_key = os.environ.get("AMPLITUDE_API_KEY")
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
    amplitude_enabled = amplitude_api_key and not is_placeholder_value(
        amplitude_api_key
    )
    if amplitude_enabled:
        logger.info(
            f"Amplitude tracking enabled (Endpoint: {amplitude_endpoint or 'default'})"
        )
    else:
        logger.warning(
            "Amplitude tracking disabled - AMPLITUDE_API_KEY not set or contains placeholder value"
        )

    # Warn if no analytics platforms are configured
    if not ga_enabled and not amplitude_enabled:
        logger.warning(
            "No analytics platforms are properly configured. Events will be received but not forwarded."
        )

    # Test the connection to the Optimizely Agent before starting
    connection_ok = await test_agent_connection(sdk_key, agent_url)
    if not connection_ok:
        logger.error("Failed to connect to Optimizely Agent. Exiting.")
        sys.exit(1)

    try:
        # Start listening for notifications
        await listen_for_notifications(sdk_key, agent_url, filter_type)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error in notification listener: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
