#!/usr/bin/env python
"""
Event Buffer Module
-----------------
This module provides a buffer/queue system for event processing with retry capabilities.
"""

import asyncio
import logging
import time
from typing import Dict, Any, List, Callable, Awaitable
from collections import deque

# Set up logging
logger = logging.getLogger(__name__)

class EventBuffer:
    """
    A buffer for Optimizely events with retry capabilities.
    
    This class maintains a queue of events and processes them asynchronously,
    with configurable retry logic for failed events.
    """
    
    def __init__(
        self, 
        max_size: int = 1000, 
        max_retries: int = 3,
        retry_delay_base: float = 2.0,
        retry_delay_max: float = 60.0
    ):
        """
        Initialize the event buffer.
        
        Args:
            max_size: Maximum number of events to store in the buffer
            max_retries: Maximum number of retry attempts for failed events
            retry_delay_base: Base delay (in seconds) for retry backoff
            retry_delay_max: Maximum delay (in seconds) for retry backoff
        """
        self.queue = deque(maxlen=max_size)
        self.processing = False
        self.max_retries = max_retries
        self.retry_delay_base = retry_delay_base
        self.retry_delay_max = retry_delay_max
        self.failed_events = []
        self.processors = []
        
    async def add_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Add an event to the buffer.
        
        Args:
            event_data: The event data to buffer
            
        Returns:
            True if the event was added, False if the buffer is full
        """
        try:
            # Create a buffer item with the event data and metadata
            buffer_item = {
                "event_data": event_data,
                "timestamp": time.time(),
                "retry_count": 0,
                "last_error": None,
                "next_retry": None
            }
            
            # Add to the queue
            self.queue.append(buffer_item)
            
            return True
        except Exception as e:
            logger.error(f"Error adding event to buffer: {str(e)}")
            return False
    
    def register_processor(self, processor_func: Callable[[Dict[str, Any]], Awaitable[bool]]):
        """
        Register an async function to process events.
        
        Args:
            processor_func: An async function that takes an event_data dict and returns a boolean
                           indicating success or failure
        """
        self.processors.append(processor_func)
        logger.debug(f"Registered event processor: {processor_func.__name__}")
    
    async def process_events(self):
        """
        Process all events in the buffer.
        
        This method will continue running until explicitly stopped.
        """
        if self.processing:
            logger.debug("Event processing already running")
            return
            
        self.processing = True
        logger.info("Starting event buffer processor")
        
        try:
            while self.processing:
                # Process any events that are ready
                await self._process_batch()
                
                # Process any failed events that are ready for retry
                await self._retry_failed_events()
                
                # Sleep a bit to avoid consuming too much CPU
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error in event processing loop: {str(e)}")
        finally:
            self.processing = False
            logger.info("Event buffer processor stopped")
    
    async def _process_batch(self, batch_size: int = 10):
        """
        Process a batch of events from the queue.
        
        Args:
            batch_size: Maximum number of events to process in this batch
        """
        processed_count = 0
        
        while self.queue and processed_count < batch_size:
            # Get the next event from the queue
            buffer_item = self.queue.popleft()
            event_data = buffer_item["event_data"]
            
            # Process the event with all registered processors
            success = True
            for processor in self.processors:
                try:
                    result = await processor(event_data)
                    if not result:
                        success = False
                        buffer_item["last_error"] = f"Processor {processor.__name__} failed"
                except Exception as e:
                    success = False
                    buffer_item["last_error"] = str(e)
                    logger.error(f"Error in processor {processor.__name__}: {str(e)}")
            
            # If processing failed, add to failed events for retry
            if not success:
                buffer_item["retry_count"] += 1
                
                if buffer_item["retry_count"] <= self.max_retries:
                    # Calculate next retry time with exponential backoff
                    delay = min(
                        self.retry_delay_base * (2 ** (buffer_item["retry_count"] - 1)),
                        self.retry_delay_max
                    )
                    buffer_item["next_retry"] = time.time() + delay
                    
                    # Add to failed events
                    self.failed_events.append(buffer_item)
                    
                    # Log the retry
                    logger.warning(
                        f"Event processing failed, scheduled for retry {buffer_item['retry_count']}/{self.max_retries} "
                        f"in {delay:.1f}s"
                    )
                else:
                    # Log the permanent failure
                    logger.error(
                        f"Event processing failed after {self.max_retries} retries: {buffer_item['last_error']}"
                    )
            else:
                # Log successful processing
                # Use notification_type if it's already set, otherwise use our standard determination function
                if "notification_type" in event_data:
                    notification_type = event_data["notification_type"]
                else:
                    # Import here to avoid circular imports
                    from notification_listener import determine_notification_type
                    notification_type = determine_notification_type(event_data)
                    logger.warning(f"Redetermined notification type: {notification_type}")
                user_id = "unknown"
                if "UserContext" in event_data and "ID" in event_data["UserContext"]:
                    user_id = event_data["UserContext"]["ID"]
                elif "userId" in event_data:
                    user_id = event_data["userId"]
                    
                logger.debug(f"Successfully processed {notification_type} event for user {user_id}")
            
            processed_count += 1
    
    async def _retry_failed_events(self):
        """
        Retry processing of failed events that are ready for retry.
        """
        current_time = time.time()
        ready_for_retry = []
        still_waiting = []
        
        # Separate events that are ready for retry
        for buffer_item in self.failed_events:
            if buffer_item["next_retry"] <= current_time:
                ready_for_retry.append(buffer_item)
            else:
                still_waiting.append(buffer_item)
        
        # Update the failed events list
        self.failed_events = still_waiting
        
        # Add events that are ready for retry back to the queue
        for buffer_item in ready_for_retry:
            event_data = buffer_item["event_data"]
            event_type = event_data.get("Type", event_data.get("type", "unknown"))
            user_id = "unknown"
            if "UserContext" in event_data and "ID" in event_data["UserContext"]:
                user_id = event_data["UserContext"]["ID"]
            elif "userId" in event_data:
                user_id = event_data["userId"]
                
            logger.info(f"Retrying {event_type} event for user {user_id} (attempt {buffer_item['retry_count']}/{self.max_retries})")
            self.queue.append(buffer_item)
    
    def stop(self):
        """
        Stop the event processor.
        """
        self.processing = False
        logger.info("Stopping event buffer processor")
    
    @property
    def queue_size(self) -> int:
        """
        Get the current size of the queue.
        
        Returns:
            The number of events in the queue
        """
        return len(self.queue)
    
    @property
    def failed_size(self) -> int:
        """
        Get the current size of the failed events list.
        
        Returns:
            The number of failed events waiting for retry
        """
        return len(self.failed_events)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the event buffer.
        
        Returns:
            A dictionary with buffer statistics
        """
        return {
            "queue_size": self.queue_size,
            "failed_size": self.failed_size,
            "max_size": self.queue.maxlen,
            "max_retries": self.max_retries,
            "processing": self.processing
        }
