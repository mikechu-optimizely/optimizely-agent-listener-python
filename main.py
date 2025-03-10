#!/usr/bin/env python
"""
Optimizely Agent Notification Listener
-------------------------------------
This application listens for notifications from the Optimizely Agent and
forwards them to analytics platforms.
"""

import os
import asyncio
import json
import logging
import time
import signal
from pathlib import Path
import aiohttp
from dotenv import load_dotenv

# Import our modules
from logger_config import setup_logging
from event_buffer import EventBuffer
from notification_listener import NotificationListener, test_agent_connection
from notification_processor import NotificationProcessor

# Set up logging
logger = setup_logging()

# Load environment variables
env_path = Path('.') / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logger.info("Loaded environment variables from .env file")

# Get configuration from environment variables
OPTIMIZELY_SDK_KEY = os.getenv("OPTIMIZELY_SDK_KEY")
OPTIMIZELY_AGENT_BASE_URL = os.getenv("OPTIMIZELY_AGENT_BASE_URL", "http://localhost:8080")
AGENT_NOTIFICATIONS_ENDPOINT = f"{OPTIMIZELY_AGENT_BASE_URL}/v1/notifications/event-stream"

# Global flag to control the main loop
running = True

async def handle_event(event):
    """
    Handle an event received from the notification listener.
    
    This function is called by the notification listener for each event received.
    It adds the event to the buffer for processing.
    
    Args:
        event: The event object from the SSE client
    """
    try:
        # Parse the event data
        event_data = json.loads(event.data)
        
        # Add to the buffer
        await buffer.add_event(event_data)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse event data: {str(e)}")
    except Exception as e:
        logger.error(f"Error handling event: {str(e)}")

async def process_buffered_event(event_data):
    """
    Process a buffered event.
    
    This function is registered with the event buffer to process events.
    
    Args:
        event_data: The event data to process
        
    Returns:
        True if processing was successful, False otherwise
    """
    try:
        # Use the notification processor to process the event
        success, _ = await processor.process_notification(type('Event', (), {'data': json.dumps(event_data)}))
        return success
    except Exception as e:
        logger.error(f"Error processing buffered event: {str(e)}")
        return False

async def shutdown(signal=None):
    """
    Shutdown the application gracefully.
    
    Args:
        signal: The signal that triggered the shutdown
    """
    global running
    
    if signal:
        logger.info(f"Received exit signal {signal.name}...")
    
    logger.info("Shutting down...")
    
    # Stop the notification listener
    logger.info("Stopping notification listener...")
    await listener.stop()
    
    # Stop the event buffer
    logger.info("Stopping event buffer...")
    buffer.stop()
    
    # Set the running flag to False
    running = False

def signal_handler():
    """Register signal handlers for graceful shutdown."""
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_event_loop().add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(shutdown(s))
        )

async def main():
    """
    Main entry point for the application.
    
    This function initializes the application components and starts the main loop.
    """
    global buffer, listener, processor, running
    
    logger.info("Starting Optimizely notification listener")
    logger.info(f"Agent URL: {AGENT_NOTIFICATIONS_ENDPOINT}")
    
    # Create the notification processor
    processor = NotificationProcessor()
    
    # Check analytics configuration
    warnings = processor.check_analytics_config()
    for warning in warnings:
        logger.warning(warning)
    
    # Test the connection to the Optimizely Agent
    connection_ok = await test_agent_connection(OPTIMIZELY_SDK_KEY, OPTIMIZELY_AGENT_BASE_URL)
    if not connection_ok:
        logger.error("Failed to connect to Optimizely Agent. Exiting.")
        return
    
    # Create the event buffer
    buffer = EventBuffer(max_size=1000, max_retries=3)
    
    # Register the event processor with the buffer
    buffer.register_processor(process_buffered_event)
    
    # Start the event buffer processor
    asyncio.create_task(buffer.process_events())
    
    # Create the notification listener
    listener = NotificationListener(
        sdk_key=OPTIMIZELY_SDK_KEY,
        agent_url=AGENT_NOTIFICATIONS_ENDPOINT,
        event_callback=handle_event
    )
    
    # Start the notification listener
    await listener.start()
    
    # Register signal handlers for graceful shutdown
    signal_handler()
    
    # Main loop - keep the application running until shutdown is requested
    while running:
        await asyncio.sleep(1)
        
        # Log buffer stats periodically
        if running and time.time() % 60 < 1:  # Approximately once per minute
            stats = buffer.get_stats()
            logger.info(f"Buffer stats: {stats}")
    
    logger.info("Application shutdown complete")

if __name__ == "__main__":
    # Use asyncio.run() to run the main coroutine
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Exiting.")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
