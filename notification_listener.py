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
from aiohttp_sse_client import client as sse_client
import random

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
        self.stop_event = asyncio.Event()
        
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
        last_activity_time = time.time()
        
        while not self.stop_event.is_set():
            try:
                # Check if the Optimizely Agent is running before connecting
                agent_running = await self._check_agent_health()
                if not agent_running:
                    logger.error("Optimizely Agent is not running. Will retry in 10 seconds.")
                    await asyncio.sleep(10)
                    continue
                
                logger.info(f"Connecting to SSE endpoint: {self.notification_url}")
                
                # Set up headers with SDK key
                headers = {
                    "X-Optimizely-SDK-Key": self.sdk_key,
                    "Accept": "text/event-stream",
                    "Cache-Control": "no-cache"
                }
                
                # Use the sse_client library for handling SSE events
                async with sse_client.EventSource(
                    self.notification_url,
                    headers=headers,
                    timeout=30  # Timeout for initial connection
                ) as event_source:
                    async for event in event_source:
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
                                
                                # Create a temporary session for the health check
                                async with aiohttp.ClientSession() as session:
                                    async with session.get(health_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                                        if response.status == 200:
                                            logger.debug("Health check successful, connection is alive")
                                            last_activity_time = current_time  # Reset the activity timer
                                        else:
                                            logger.warning(f"Health check failed with status {response.status}")
                                            # Break out of the event loop to trigger a reconnection
                                            break
                            except Exception as e:
                                logger.error(f"Error during health check: {str(e)}")
                                # Break out of the event loop to trigger a reconnection
                                break
            
            except aiohttp.ClientPayloadError as e:
                if "TransferEncodingError" in str(e):
                    retry_count += 1
                    logger.error(f"Transfer encoding error detected (retry {retry_count}/{self.max_retries}): {str(e)}")
                    logger.info("This is likely due to the server closing the connection unexpectedly.")
                    
                    # Calculate backoff time with jitter
                    backoff_time = min(30, 2 ** retry_count) * (0.5 + random.random())
                    logger.info(f"Retrying in {backoff_time:.2f} seconds with exponential backoff...")
                    
                    # Check if the agent is still running before reconnecting
                    agent_running = await self._check_agent_health()
                    if agent_running:
                        logger.info("Optimizely Agent is still running. Will reconnect.")
                    else:
                        logger.error("Optimizely Agent is not running. Will retry after backoff.")
                    
                    await asyncio.sleep(backoff_time)
                else:
                    # Handle other ClientPayloadErrors
                    retry_count += 1
                    logger.error(f"Client payload error (retry {retry_count}/{self.max_retries}): {str(e)}")
                    await asyncio.sleep(min(30, retry_count * 5))
            
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                retry_count += 1
                logger.error(f"Connection error (retry {retry_count}/{self.max_retries}): {str(e)}")
                
                # For connection errors, use a simpler backoff strategy
                backoff_time = min(30, retry_count * 5)
                logger.info(f"Retrying in {backoff_time} seconds...")
                await asyncio.sleep(backoff_time)
            
            except Exception as e:
                retry_count += 1
                logger.error(f"Unexpected error (retry {retry_count}/{self.max_retries}): {str(e)}")
                
                # For unexpected errors, use a simpler backoff strategy
                backoff_time = min(30, retry_count * 5)
                logger.info(f"Retrying in {backoff_time} seconds...")
                await asyncio.sleep(backoff_time)
            
            # Check if we've exceeded the maximum number of retries
            if retry_count >= self.max_retries:
                logger.error(f"Maximum retry attempts ({self.max_retries}) reached. Stopping listener.")
                self.stop_event.set()
                break
    
    async def _check_agent_health(self):
        """
        Check if the Optimizely Agent is running.
        
        Args:
            None
        
        Returns:
            bool: True if the agent is running, False otherwise
        """
        try:
            health_url = f"{self.agent_base_url}/health"
            logger.debug(f"Checking if Optimizely Agent is still running: {health_url}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(health_url, timeout=3) as response:
                    logger.debug(f"Agent health check response: {response.status}")
                    if response.status == 200:
                        logger.info("Optimizely Agent is still running.")
                        return True
                    else:
                        logger.error(f"Agent health check failed with status: {response.status}")
                        return False
        except Exception as e:
            logger.error(f"Error checking agent health: {str(e)}")
            return False

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
