#!/usr/bin/env python
"""
Notification Listener Module
---------------------------
This module handles the connection to the Optimizely Agent notification stream
and processes incoming events.
"""

import asyncio
import json
import logging
import time
from enum import Enum, auto
from typing import Dict, Any, Optional, Callable, Awaitable
import aiohttp
from aiosseclient import aiosseclient

# Set up logging
logger = logging.getLogger(__name__)

class NotificationType(str, Enum):
    """Enum representing the types of notifications from Optimizely Agent."""
    DECISION = "decision"
    TRACK = "track"
    UNKNOWN = "unknown"


def determine_notification_type(event_data):
    """
    Determine the notification type based on the event data structure.
    
    Args:
        event_data: The parsed event data from the SSE stream
        
    Returns:
        NotificationType: The type of notification
    """    
    # Determine type based on payload structure
    if"ConversionEvent" in event_data:
        # This is a track event
        return NotificationType.TRACK
    elif "DecisionInfo" in event_data:
        # This is a decision event
        return NotificationType.DECISION
    
    # Default case
    return NotificationType.UNKNOWN

class NotificationListener:
    """
    Listens for notifications from the Optimizely Agent using async SSE client.
    
    This class manages the connection to the Optimizely Agent notification stream,
    handles reconnection logic, and processes incoming events.
    """
    
    def __init__(
        self, 
        sdk_key: str, 
        agent_base_url: str, 
        filter_type: Optional[str] = None,
        max_retries: int = 10,
        heartbeat_interval: float = 2.0,
        event_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None
    ):
        """
        Initialize the notification listener.
        
        Args:
            sdk_key: The Optimizely SDK key
            agent_base_url: The base URL of the Optimizely Agent (e.g., http://localhost:8080)
            filter_type: Optional filter for notification types (e.g., 'decision')
            max_retries: Maximum number of connection retry attempts
            heartbeat_interval: Interval (in seconds) to check for heartbeats
            event_callback: Async callback function to process events
        """
        self.sdk_key = sdk_key
        self.agent_base_url = agent_base_url
        self.filter_type = filter_type
        self.max_retries = max_retries
        self.heartbeat_interval = heartbeat_interval
        self.event_callback = event_callback
        self.running = False
        self.session = None
        self.task = None
        
        # Construct the notification URL with filter if provided
        self.notification_url = f"{agent_base_url}/v1/notifications/event-stream"
        if filter_type:
            self.notification_url = f"{self.notification_url}?filter={filter_type}"
            logger.debug(f"Notification filter: {filter_type}")
        else:
            logger.debug("No notification filter set - listening for all notification types")
    
    async def start(self):
        """
        Start listening for notifications.
        
        This method starts an async task to listen for notifications.
        """
        if self.running:
            logger.warning("Notification listener is already running")
            return
        
        self.running = True
        self.session = aiohttp.ClientSession()
        self.task = asyncio.create_task(self._listen_loop())
        logger.info("Notification listener started")
    
    async def stop(self):
        """
        Stop listening for notifications.
        
        This method stops the notification listener and cleans up resources.
        """
        if not self.running:
            logger.warning("Notification listener is not running")
            return
        
        self.running = False
        
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
        
        if self.session:
            await self.session.close()
            await asyncio.sleep(0.1)  # wait for the session to close
            self.session = None
        
        logger.info("Notification listener stopped")
    
    def _determine_notification_type(self, event_data):
        """
        Determine the notification type based on the event data structure.
        
        Args:
            event_data: The parsed event data from the SSE stream
            
        Returns:
            NotificationType: The type of notification
        """
        return determine_notification_type(event_data)
    
    async def _listen_loop(self):
        """
        Main loop for listening to notifications.
        
        This method handles the connection to the Optimizely Agent notification stream,
        processes incoming events, and manages reconnection logic.
        """
        retry_count = 0
        
        # Set up headers with SDK key
        headers = {
            "X-Optimizely-Sdk-Key": self.sdk_key, 
            "Accept": "text/event-stream",
            "User-Agent": "OptimizelyNotificationListener/1.0",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
        
        logger.info(f"Connecting to SSE endpoint: {self.notification_url}")
        logger.info(f"Using SDK key: {self.sdk_key}")
        
        while self.running and retry_count < self.max_retries:
            try:
                logger.debug(f"Sending request with headers: {headers}")
                
                # Connect to the SSE endpoint using aiosseclient
                last_activity_time = time.time()
                
                async for event in aiosseclient(
                    self.notification_url,
                    headers=headers
                ):
                    # Reset retry counter after successful connection
                    retry_count = 0
                    
                    # Update last activity time
                    last_activity_time = time.time()
                    
                    if event.data:
                        # Extract user ID for logging
                        user_id = "unknown"
                        try:
                            event_data = json.loads(event.data)
                            if "UserContext" in event_data and "ID" in event_data["UserContext"]:
                                user_id = event_data["UserContext"]["ID"]
                            elif "userId" in event_data:
                                user_id = event_data["userId"]
                            
                            # Determine the notification type based on the event data
                            notification_type = self._determine_notification_type(event_data)
                            
                            # Add notification_type as a custom attribute to the event_data
                            event_data["notification_type"] = notification_type
                            
                            # Log a more detailed summary of the event
                            logger.debug(f"Received {notification_type} event for user {user_id}: {event.data[:100]}...")
                            
                            # For decision events, extract and log the flag key and variation
                            if notification_type == NotificationType.DECISION and "DecisionInfo" in event_data:
                                decision_info = event_data["DecisionInfo"]
                                flag_key = decision_info.get("flagKey", "unknown")
                                variation_key = decision_info.get("variationKey", "unknown")
                                logger.debug(f"Decision details - Flag: {flag_key}, Variation: {variation_key}, User: {user_id}")
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse event data as JSON: {event.data[:100]}...")
                        except Exception as e:
                            logger.error(f"Error extracting event details: {str(e)}")
                        
                        try:
                            # Log additional details based on notification type
                            if notification_type == NotificationType.TRACK:
                                event_key = event_data.get("EventKey", "unknown")
                                logger.debug(f"Track event details - Event: {event_key}, User: {user_id}")
                            
                            # Process the event with the callback if provided
                            if self.event_callback:
                                await self.event_callback(event)
                        except Exception as e:
                            logger.error(f"Error processing notification: {str(e)}")
                    else:
                        # Empty data might be a heartbeat or keep-alive
                        logger.debug("Received event with empty data (possible heartbeat)")
                    
                    # Check for heartbeat timeout
                    current_time = time.time()
                    if current_time - last_activity_time > self.heartbeat_interval:
                        logger.warning(f"No activity for {self.heartbeat_interval} seconds, checking connection...")
                        # Send a ping to the Optimizely Agent health endpoint to keep the connection alive
                        try:
                            health_url = f"{self.agent_base_url}/health"
                            logger.debug(f"Sending ping to health endpoint: {health_url}")
                            
                            # Use the session to make the request
                            async with self.session.get(health_url, timeout=5) as response:
                                logger.debug(f"Health ping response: {response.status}")
                            
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
                logger.error(f"SSE client error (retry {retry_count}/{self.max_retries}): {str(e)}")
                logger.info("This is likely due to the server closing the connection unexpectedly.")
                
                # Shorter wait time for SSE client errors as it's usually temporary
                wait_time = 2 * retry_count
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
                
                # Check if the agent is still running before retrying
                try:
                    # Extract base URL for health check
                    health_url = f"{self.agent_base_url}/health"
                    logger.debug(f"Checking if Optimizely Agent is still running: {health_url}")
                    
                    # Use the session to make the request
                    async with self.session.get(health_url, timeout=5) as response:
                        logger.debug(f"Agent health check response: {response.status}")
                        if response.status == 200:
                            logger.info("Optimizely Agent is still running. Will reconnect.")
                        else:
                            logger.error(f"Agent health check failed with status: {response.status}")
                            raise Exception("Agent health check failed")
                except Exception as health_check_error:
                    logger.error(f"Error checking agent health: {str(health_check_error)}")
                    # Continue with retry anyway, in case it's a temporary network issue
                
            except Exception as e:
                retry_count += 1
                logger.error(f"Connection error (retry {retry_count}/{self.max_retries}): {str(e)}")
                
                # Exponential backoff for general errors
                wait_time = min(5 * retry_count, 60)  # Cap at 60 seconds
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
        
        # If we've exhausted all retries, log an error
        if retry_count >= self.max_retries and self.running:
            logger.error("Max retries exceeded. Exiting.")
            self.running = False

async def test_agent_connection(sdk_key: str, agent_base_url: str) -> bool:
    """
    Test the connection to the Optimizely Agent.
    
    Args:
        sdk_key: The Optimizely SDK key
        agent_base_url: The base URL of the Optimizely Agent
        
    Returns:
        True if the connection is successful, False otherwise
    """
    # Test the health endpoint
    health_url = f"{agent_base_url}/health"
    logger.info(f"Testing connection to Optimizely Agent health endpoint: {health_url}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(health_url, timeout=5) as response:
                logger.info(f"Health endpoint response: {response.status} {response.reason}")
                
                if response.status == 200:
                    response_text = await response.text()
                    logger.info(f"Response content: {response_text}")
                    logger.info("Optimizely Agent is healthy!")
                    
                    # Also test the config endpoint to ensure the SDK key is valid
                    config_url = f"{agent_base_url}/v1/config"
                    headers = {"X-Optimizely-Sdk-Key": sdk_key}
                    
                    logger.info(f"Testing configuration endpoint: {config_url}")
                    async with session.get(config_url, headers=headers, timeout=5) as config_response:
                        logger.info(f"Config endpoint response: {config_response.status} {config_response.reason}")
                        
                        if config_response.status == 200:
                            logger.info("Successfully retrieved configuration!")
                            return True
                        else:
                            logger.error(f"Failed to retrieve configuration: {config_response.status} {config_response.reason}")
                            return False
                else:
                    logger.error(f"Health check failed: {response.status} {response.reason}")
                    return False
    except Exception as e:
        logger.error(f"Error connecting to Optimizely Agent: {str(e)}")
        return False
