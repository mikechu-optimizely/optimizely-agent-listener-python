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
import uuid
from enum import Enum
from typing import Dict, Any, Optional, Callable, Awaitable
import httpx
import random
from datetime import datetime, timedelta

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
    if "ConversionEvent" in event_data:
        # This is a track event
        return NotificationType.TRACK
    elif "DecisionInfo" in event_data:
        # This is a decision event
        return NotificationType.DECISION
    
    # Default case
    return NotificationType.UNKNOWN


class EventCache:
    """
    A cache for tracking processed events to avoid duplicates.
    Uses LRU cache with time-based expiration.
    """
    
    def __init__(self, maxsize=1000, ttl_seconds=300):
        """
        Initialize the event cache.
        
        Args:
            maxsize: Maximum number of events to cache
            ttl_seconds: Time to live for cached events in seconds
        """
        self.maxsize = maxsize
        self.ttl_seconds = ttl_seconds
        self._cache = {}
        self._access_times = {}
    
    def add(self, event_id):
        """
        Add an event ID to the cache.
        
        Args:
            event_id: The event ID to add
            
        Returns:
            bool: True if the event was added, False if it was already in the cache
        """
        now = datetime.now()
        
        # Clean expired entries if cache is at capacity
        if len(self._cache) >= self.maxsize:
            self._clean_expired()
        
        # If still at capacity, remove least recently used
        if len(self._cache) >= self.maxsize:
            self._remove_lru()
        
        # Check if event is already in cache
        if event_id in self._cache:
            # Update access time
            self._access_times[event_id] = now
            return False
        
        # Add to cache
        self._cache[event_id] = now
        self._access_times[event_id] = now
        return True
    
    def _clean_expired(self):
        """Remove expired entries from the cache."""
        now = datetime.now()
        expiration_cutoff = now - timedelta(seconds=self.ttl_seconds)
        
        expired_keys = [
            k for k, v in self._cache.items() 
            if v < expiration_cutoff
        ]
        
        for k in expired_keys:
            del self._cache[k]
            del self._access_times[k]
    
    def _remove_lru(self):
        """Remove the least recently used entry from the cache."""
        if not self._access_times:
            return
        
        # Find the key with the oldest access time
        lru_key = min(self._access_times.items(), key=lambda x: x[1])[0]
        
        # Remove it
        del self._cache[lru_key]
        del self._access_times[lru_key]
    
    def __contains__(self, event_id):
        """
        Check if an event ID is in the cache.
        
        Args:
            event_id: The event ID to check
            
        Returns:
            bool: True if the event is in the cache, False otherwise
        """
        if event_id in self._cache:
            # Update access time
            self._access_times[event_id] = datetime.now()
            return True
        return False


class SSEClient:
    """
    A Server-Sent Events (SSE) client implementation using httpx.
    
    This class handles the connection to an SSE endpoint and parses the incoming events
    according to the SSE specification.
    """
    
    def __init__(
        self,
        url: str,
        headers: Dict[str, str] = None,
        timeout: Optional[int] = None,
        max_retries: int = 10,
        retry_delay: float = 1.0,
        connection_id: str = None,
    ):
        """
        Initialize the SSE client.
        
        Args:
            url: The URL of the SSE endpoint
            headers: Optional headers to include in the request
            timeout: Optional timeout for the request in seconds
            max_retries: Maximum number of connection retry attempts
            retry_delay: Initial delay between retries (will be used with exponential backoff)
            connection_id: Optional identifier for this connection
        """
        self.url = url
        self.headers = headers or {}
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection_id = connection_id or str(uuid.uuid4())[:8]
        
        # Ensure proper headers for SSE
        if "Accept" not in self.headers:
            self.headers["Accept"] = "text/event-stream"
        if "Cache-Control" not in self.headers:
            self.headers["Cache-Control"] = "no-cache, no-transform"
        if "Connection" not in self.headers:
            self.headers["Connection"] = "keep-alive"
        
        # Add a unique request ID for tracking
        self.headers["X-Request-ID"] = str(uuid.uuid4())
        
        # Add a user agent
        if "User-Agent" not in self.headers:
            self.headers["User-Agent"] = "OptimizelyAgentListener/1.0"
    
    async def connect(self, client: httpx.AsyncClient, callback: Callable[[Dict[str, Any]], Awaitable[None]]):
        """
        Connect to the SSE endpoint and process events.
        
        Args:
            client: The httpx AsyncClient to use for the connection
            callback: Async callback function to process events
        """
        retry_count = 0
        event_data = ""
        event_id = None
        event_type = None
        
        while retry_count < self.max_retries:
            try:
                logger.info(f"Connection {self.connection_id}: Connecting to SSE endpoint: {self.url}")
                
                # Use httpx streaming response
                async with client.stream("GET", self.url, headers=self.headers, timeout=self.timeout) as response:
                    if response.status_code != 200:
                        logger.error(f"Connection {self.connection_id}: Failed to connect to SSE endpoint: {response.status_code} {response.reason_phrase}")
                        raise httpx.HTTPStatusError(f"HTTP Error {response.status_code}", request=response.request, response=response)
                    
                    # Reset retry count on successful connection
                    retry_count = 0
                    logger.info(f"Connection {self.connection_id}: Successfully connected to SSE endpoint")
                    
                    # Process the stream line by line
                    async for line in response.aiter_lines():
                        # Reset event data if we receive an empty line (end of event)
                        if not line.strip():
                            if event_data:
                                # Process the complete event
                                await self._process_event(event_data, event_id, event_type, callback)
                                # Reset event data
                                event_data = ""
                                event_id = None
                                event_type = None
                            continue
                        
                        # Parse SSE fields
                        if line.startswith("id:"):
                            event_id = line[3:].strip()
                        elif line.startswith("event:"):
                            event_type = line[6:].strip()
                        elif line.startswith("data:"):
                            data = line[5:].strip()
                            event_data += data
                        elif line.startswith(":"):
                            # Comment line, used for heartbeats
                            logger.debug(f"Connection {self.connection_id}: SSE Comment: {line[1:].strip()}")
            
            except (httpx.HTTPError, httpx.TimeoutException) as e:
                retry_count += 1
                logger.error(f"Connection {self.connection_id}: Connection error (retry {retry_count}/{self.max_retries}): {str(e)}")
                
                # Calculate backoff time with jitter
                backoff_time = min(30, 2 ** retry_count) * (0.5 + random.random())
                logger.info(f"Connection {self.connection_id}: Retrying in {backoff_time:.2f} seconds with exponential backoff...")
                await asyncio.sleep(backoff_time)
            
            except Exception as e:
                retry_count += 1
                logger.error(f"Connection {self.connection_id}: Unexpected error (retry {retry_count}/{self.max_retries}): {str(e)}")
                
                # For unexpected errors, use a simpler backoff strategy
                backoff_time = min(30, retry_count * 5)
                logger.info(f"Connection {self.connection_id}: Retrying in {backoff_time} seconds...")
                await asyncio.sleep(backoff_time)
        
        logger.error(f"Connection {self.connection_id}: Maximum retry attempts ({self.max_retries}) reached. Giving up.")
    
    async def _process_event(self, data: str, event_id: Optional[str], event_type: Optional[str], callback: Callable):
        """
        Process a complete SSE event.
        
        Args:
            data: The event data
            event_id: The event ID (if any)
            event_type: The event type (if any)
            callback: The callback function to process the event
        """
        try:
            # Create an event object similar to what aiohttp_sse_client provides
            event = {
                "data": data,
                "id": event_id,
                "event": event_type,
                "connection_id": self.connection_id
            }
            
            # Log the event for debugging
            logger.debug(f"Connection {self.connection_id}: Processing event: {event}")
            
            # Call the callback with the event
            await callback(event)
        except Exception as e:
            logger.error(f"Connection {self.connection_id}: Error processing event: {str(e)}")


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
        event_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        pool_size: int = 3,
        event_cache_size: int = 1000,
        event_cache_ttl: int = 300
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
            pool_size: Number of parallel SSE connections to maintain
            event_cache_size: Size of the LRU cache for event deduplication
            event_cache_ttl: Time to live for cached events in seconds
        """
        self.sdk_key = sdk_key
        self.agent_base_url = agent_base_url
        self.filter_type = filter_type
        self.max_retries = max_retries
        self.heartbeat_interval = heartbeat_interval
        self.event_callback = event_callback
        self.pool_size = pool_size
        self.running = False
        self.clients = []
        self.tasks = []
        self.stop_event = asyncio.Event()
        self.event_cache = EventCache(maxsize=event_cache_size, ttl_seconds=event_cache_ttl)
        
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
        
        This method starts async tasks to listen for notifications.
        """
        if self.running:
            logger.warning("Notification listener is already running")
            return
        
        self.running = True
        
        # Configure httpx clients with optimized settings
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        timeout = httpx.Timeout(connect=10.0, read=None, write=10.0, pool=None)
        
        # Create a pool of clients
        for i in range(self.pool_size):
            client = httpx.AsyncClient(
                limits=limits,
                timeout=timeout,
                http2=True,  # Use HTTP/2 if available
                verify=True  # Verify SSL certificates
            )
            self.clients.append(client)
            
            # Start a listener task for each client
            task = asyncio.create_task(self._listen_loop(client, f"conn-{i+1}"))
            self.tasks.append(task)
        
        logger.info(f"Notification listener started with {self.pool_size} connections")
    
    async def stop(self):
        """
        Stop listening for notifications.
        
        This method stops the notification listener and cleans up resources.
        """
        if not self.running:
            logger.warning("Notification listener is not running")
            return
        
        self.running = False
        
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.tasks = []
        
        # Close all clients
        for client in self.clients:
            await client.aclose()
        self.clients = []
        
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
    
    async def _listen_loop(self, client: httpx.AsyncClient, connection_id: str):
        """
        Main loop for listening to notifications.
        
        This method handles the connection to the Optimizely Agent notification stream,
        processes incoming events, and manages reconnection logic.
        
        Args:
            client: The httpx client to use for connections
            connection_id: Identifier for this connection
        """
        while not self.stop_event.is_set():
            try:
                # Check if the Optimizely Agent is running before connecting
                agent_running = await self._check_agent_health(client)
                if not agent_running:
                    logger.error(f"Connection {connection_id}: Optimizely Agent is not running. Will retry in 10 seconds.")
                    await asyncio.sleep(10)
                    continue
                
                # Set up headers with SDK key
                headers = {
                    "X-Optimizely-SDK-Key": self.sdk_key,
                    "Accept": "text/event-stream",
                    "Cache-Control": "no-cache, no-transform",
                    "Connection": "keep-alive",
                    "Keep-Alive": "timeout=60, max=1000"
                }
                
                # Create an SSE client
                sse_client = SSEClient(
                    url=self.notification_url,
                    headers=headers,
                    timeout=None,  # No timeout for the streaming connection
                    max_retries=self.max_retries,
                    connection_id=connection_id
                )
                
                # Connect to the SSE endpoint and process events
                await sse_client.connect(client, self._process_event)
            
            except Exception as e:
                logger.error(f"Connection {connection_id}: Unexpected error in listen loop: {str(e)}")
                await asyncio.sleep(5)  # Brief pause before retry
    
    async def _process_event(self, event):
        """
        Process an SSE event.
        
        Args:
            event: The SSE event object
        """
        try:
            # Log the raw event for debugging
            logger.debug(f"Raw event received: {event}")
            
            # Check if this is a valid event
            if not isinstance(event, dict):
                logger.warning(f"Received invalid event type: {type(event)}")
                return
                
            # Check for event ID for deduplication
            event_id = event.get("id")
            connection_id = event.get("connection_id", "unknown")
            
            # Check if the event has data
            if "data" not in event or not event["data"]:
                logger.debug(f"Connection {connection_id}: Received event without data field or empty data")
                return
                
            # If no event ID, generate one based on content
            if not event_id:
                # Create a hash of the data as the event ID
                event_id = f"gen-{hash(event['data'])}"
                event["id"] = event_id
            
            # Skip processing if we've seen this event before
            if event_id:
                if event_id in self.event_cache:
                    logger.debug(f"Connection {connection_id}: Skipping duplicate event with ID: {event_id}")
                    return
                
                # Add to cache
                self.event_cache.add(event_id)
                logger.debug(f"Connection {connection_id}: Processing new event with ID: {event_id}")
            
            # Process the event data
            try:
                event_data = json.loads(event["data"])
                
                # Extract user ID for logging
                user_id = "unknown"
                if "UserContext" in event_data and "ID" in event_data["UserContext"]:
                    user_id = event_data["UserContext"]["ID"]
                elif "userId" in event_data:
                    user_id = event_data["userId"]
                
                # Determine the notification type based on the event data
                notification_type = self._determine_notification_type(event_data)
                
                # Add notification_type as a custom attribute to the event_data
                event_data["notification_type"] = notification_type
                
                # Log a more detailed summary of the event
                logger.debug(f"Connection {connection_id}: Received {notification_type} event for user {user_id}: {event['data'][:100]}...")
                
                # For decision events, extract and log the flag key and variation
                if notification_type == NotificationType.DECISION and "DecisionInfo" in event_data:
                    decision_info = event_data["DecisionInfo"]
                    flag_key = decision_info.get("flagKey", "unknown")
                    variation_key = decision_info.get("variationKey", "unknown")
                    logger.debug(f"Connection {connection_id}: Decision details - Flag: {flag_key}, Variation: {variation_key}, User: {user_id}")
                
                # Log additional details based on notification type
                if notification_type == NotificationType.TRACK:
                    event_key = event_data.get("EventKey", "unknown")
                    logger.debug(f"Connection {connection_id}: Track event details - Event: {event_key}, User: {user_id}")
                
                # Process the event with the callback if provided
                if self.event_callback:
                    # Create a new event object with the parsed data for the callback
                    callback_event = {
                        "data": event_data,
                        "id": event_id,
                        "event": event.get("event"),
                        "connection_id": connection_id,
                        "notification_type": notification_type
                    }
                    await self.event_callback(callback_event)
            except json.JSONDecodeError:
                logger.error(f"Connection {connection_id}: Failed to parse event data as JSON: {event['data'][:100]}...")
            except Exception as e:
                logger.error(f"Connection {connection_id}: Error extracting event details: {str(e)}")
        except Exception as e:
            logger.error(f"Error handling event: {str(e)}")
    
    async def _check_agent_health(self, client: httpx.AsyncClient):
        """
        Check if the Optimizely Agent is running.
        
        Args:
            client: The httpx client to use for the health check
            
        Returns:
            bool: True if the agent is running, False otherwise
        """
        try:
            health_url = f"{self.agent_base_url}/health"
            logger.debug(f"Checking if Optimizely Agent is still running: {health_url}")
            
            # Use the provided client for the health check
            response = await client.get(health_url, timeout=3.0)
            logger.debug(f"Agent health check response: {response.status_code}")
            
            if response.status_code == 200:
                logger.info("Optimizely Agent is still running.")
                return True
            else:
                logger.error(f"Agent health check failed with status: {response.status_code}")
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
        async with httpx.AsyncClient() as client:
            response = await client.get(health_url, timeout=5.0)
            logger.info(f"Health endpoint response: {response.status_code} {response.reason_phrase}")
            
            if response.status_code == 200:
                response_text = response.text
                logger.info(f"Response content: {response_text}")
                logger.info("Optimizely Agent is healthy!")
                
                # Also test the config endpoint to ensure the SDK key is valid
                config_url = f"{agent_base_url}/v1/config"
                headers = {"X-Optimizely-Sdk-Key": sdk_key}
                
                logger.info(f"Testing configuration endpoint: {config_url}")
                config_response = await client.get(config_url, headers=headers, timeout=5.0)
                logger.info(f"Config endpoint response: {config_response.status_code} {config_response.reason_phrase}")
                
                if config_response.status_code == 200:
                    logger.info("Successfully retrieved configuration!")
                    return True
                else:
                    logger.error(f"Failed to retrieve configuration: {config_response.status_code} {config_response.reason_phrase}")
                    return False
            else:
                logger.error(f"Health check failed: {response.status_code} {response.reason_phrase}")
                return False
    except Exception as e:
        logger.error(f"Error connecting to Optimizely Agent: {str(e)}")
        return False
