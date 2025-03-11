#!/usr/bin/env python
"""
Stress Test Script for Optimizely Agent Listener
------------------------------------------------
This script sends requests to the Optimizely Agent's decide endpoint
at varying paces to simulate production traffic.
"""

import asyncio
import aiohttp
import os
import random
import time
from typing import List, Dict, Any

# Configuration
BASE_URL = "http://localhost:8080"
SDK_KEY = os.getenv("OPTIMIZELY_SDK_KEY")
DECIDE_ENDPOINT = f"{BASE_URL}/v1/decide"
USER_ID_START = 400
USER_ID_END = 500
FLAG_KEYS = ["show_star_rating", "enable_recommendations", "checkout_flow"]
REQUEST_COUNT = 100
MIN_DELAY = 0.05  # minimum delay between requests in seconds
MAX_DELAY = 0.5   # maximum delay between requests in seconds


async def send_decide_request(session: aiohttp.ClientSession, user_id: int, keys: List[str]) -> Dict[str, Any]:
    """
    Send a decide request to the Optimizely Agent.
    
    Args:
        session: The aiohttp client session
        user_id: The user ID to use in the request
        keys: List of flag keys to evaluate
        
    Returns:
        The response data as a dictionary
    """
    headers = {
        "X-Optimizely-SDK-Key": SDK_KEY,
        "Content-Type": "application/json"
    }
    
    payload = {
        "userId": str(user_id),
        "keys": keys
    }
    
    try:
        async with session.post(DECIDE_ENDPOINT, json=payload, headers=headers) as response:
            if response.status == 200:
                result = await response.json()
                print(f"Request for user {user_id} successful")
                return result
            else:
                print(f"Error: {response.status} - {await response.text()}")
                return {}
    except Exception as e:
        print(f"Request error for user {user_id}: {str(e)}")
        return {}


async def simulate_production_traffic() -> None:
    """
    Simulate production traffic by sending requests at varying paces.
    """
    print(f"Starting production traffic simulation with {REQUEST_COUNT} total requests")
    print(f"User ID range: {USER_ID_START}-{USER_ID_END}")
    print(f"Flag keys: {FLAG_KEYS}")
    print(f"Delay between requests: {MIN_DELAY}-{MAX_DELAY} seconds")
    
    start_time = time.time()
    
    async with aiohttp.ClientSession() as session:
        current_user_id = USER_ID_START
        
        for i in range(REQUEST_COUNT):
            # Select random subset of flag keys
            keys = random.sample(FLAG_KEYS, random.randint(1, len(FLAG_KEYS)))
            
            # Send request
            await send_decide_request(session, current_user_id, keys)
            
            # Increment user ID
            current_user_id += 1
            if current_user_id > USER_ID_END:
                current_user_id = USER_ID_START
            
            # Random delay to simulate varying traffic patterns
            if i < REQUEST_COUNT - 1:  # No need to delay after the last request
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                print(f"Waiting {delay:.2f} seconds before next request...")
                await asyncio.sleep(delay)
    
    end_time = time.time()
    duration = end_time - start_time
    
    print("\nTraffic simulation completed!")
    print(f"Total time: {duration:.2f} seconds")
    print(f"Average requests per second: {REQUEST_COUNT / duration:.2f}")


if __name__ == "__main__":
    asyncio.run(simulate_production_traffic())
