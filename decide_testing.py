#!/usr/bin/env python
"""
Stress Test Script for Optimizely Agent Listener
------------------------------------------------
This script sends requests to the Optimizely Agent's decide endpoint
at varying paces to simulate production traffic.
It mixes sequential and parallel requests to create a more realistic pattern.
Each user ID is used exactly once to ensure complete coverage of the user ID range.
"""

import asyncio
import aiohttp
import os
import random
import time
from typing import List, Dict, Any, Set

# Configuration
BASE_URL = "http://localhost:8080"
SDK_KEY = os.getenv("OPTIMIZELY_SDK_KEY")
DECIDE_ENDPOINT = f"{BASE_URL}/v1/decide"
USER_ID_START = 400
USER_ID_END = 500
FLAG_KEYS = ["show_star_rating"]
# Calculate total available user IDs
TOTAL_USER_IDS = USER_ID_END - USER_ID_START
# Determine how to split user IDs between sequential and parallel
SEQUENTIAL_PERCENT = 0.3  # 30% of user IDs for sequential requests
SEQUENTIAL_REQUESTS = max(5, int(TOTAL_USER_IDS * SEQUENTIAL_PERCENT))
PARALLEL_BATCH_SIZE = 5  # Users per batch
MIN_DELAY = 0.05  # minimum delay between requests in seconds
MAX_DELAY = 1   # maximum delay between requests in seconds


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
    
    # Validate keys to prevent serialization errors
    if keys is None:
        keys = []
    # Filter out None values from keys list
    valid_keys = [key for key in keys if key is not None]
    
    payload = {
        "userId": str(user_id),
        "keys": valid_keys
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


async def send_parallel_requests(session: aiohttp.ClientSession, user_ids: List[int]) -> None:
    """
    Send multiple requests in parallel.
    
    Args:
        session: The aiohttp client session
        user_ids: List of user IDs to use for the requests
    """
    tasks = []
    for user_id in user_ids:
        tasks.append(send_decide_request(session, user_id, FLAG_KEYS))
    
    print(f"Sending {len(user_ids)} parallel requests for users {min(user_ids)}-{max(user_ids)}")
    await asyncio.gather(*tasks)
    print(f"Completed batch of {len(user_ids)} parallel requests")


async def simulate_production_traffic() -> None:
    """
    Simulate production traffic by sending a mix of sequential and parallel requests.
    Each user ID is used exactly once.
    """
    # Create a list of all available user IDs
    all_user_ids = list(range(USER_ID_START, USER_ID_END + 1))
    random.shuffle(all_user_ids)  # Shuffle to ensure random distribution
    
    # Allocate user IDs for sequential and parallel phases
    sequential_user_ids = all_user_ids[:SEQUENTIAL_REQUESTS]
    parallel_user_ids = all_user_ids[SEQUENTIAL_REQUESTS:]
    
    # Create batches for parallel requests
    parallel_batches = []
    for i in range(0, len(parallel_user_ids), PARALLEL_BATCH_SIZE):
        batch = parallel_user_ids[i:i + PARALLEL_BATCH_SIZE]
        if batch:  # Only add non-empty batches
            parallel_batches.append(batch)
    
    total_requests = SEQUENTIAL_REQUESTS + len(parallel_user_ids)
    
    print(f"Starting production traffic simulation with {total_requests} total requests")
    print(f"User ID range: {USER_ID_START}-{USER_ID_END}")
    print(f"Total available user IDs: {TOTAL_USER_IDS}")
    print(f"Flag keys: {FLAG_KEYS}")
    print(f"Sequential requests: {SEQUENTIAL_REQUESTS}")
    print(f"Parallel batches: {len(parallel_batches)} (with up to {PARALLEL_BATCH_SIZE} users per batch)")
    print(f"Delay between sequential requests: {MIN_DELAY}-{MAX_DELAY} seconds")
    
    # Track used user IDs to ensure no duplicates
    used_user_ids = set()
    
    # Delete the user_ids.txt file if it exists
    if os.path.exists("user_ids.txt"):
        os.remove("user_ids.txt")
    
    start_time = time.time()
    
    async with aiohttp.ClientSession() as session:
        # Phase 1: Sequential requests
        print("\n--- Starting sequential requests phase ---")
        
        for i, user_id in enumerate(sequential_user_ids):
            # Send request
            await send_decide_request(session, user_id, FLAG_KEYS)
            used_user_ids.add(user_id)
            
            # Random delay to simulate varying traffic patterns
            if i < len(sequential_user_ids) - 1:  # No need to delay after the last request
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                print(f"Waiting {delay:.2f} seconds before next request...")
                await asyncio.sleep(delay)
        
        # Phase 2: Parallel requests
        print("\n--- Starting parallel requests phase ---")
        
        for batch_idx, batch_user_ids in enumerate(parallel_batches):
            await send_parallel_requests(session, batch_user_ids)
            used_user_ids.update(batch_user_ids)
            
            # Add delay between batches
            if batch_idx < len(parallel_batches) - 1:
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                print(f"Waiting {delay:.2f} seconds before next batch...")
                await asyncio.sleep(delay)
    
    end_time = time.time()
    duration = end_time - start_time
    
    print("\nTraffic simulation completed!")
    print(f"Total time: {duration:.2f} seconds")
    print(f"Average requests per second: {total_requests / duration:.2f}")
    print(f"Total unique user IDs used: {len(used_user_ids)}")
    
    # Verify all user IDs were used exactly once
    expected_user_ids = set(range(USER_ID_START, USER_ID_END + 1))
    if used_user_ids == expected_user_ids:
        print("SUCCESS: All user IDs were used exactly once")
    else:
        print(f"WARNING: Some user IDs were not used as expected")
        if len(used_user_ids) < len(expected_user_ids):
            missing = expected_user_ids - used_user_ids
            print(f"Missing {len(missing)} user IDs")
            if len(missing) <= 10:
                print(f"Missing user IDs: {sorted(missing)}")
        
        if len(used_user_ids) > len(expected_user_ids):
            extra = used_user_ids - expected_user_ids
            print(f"Used {len(extra)} extra user IDs")
            if len(extra) <= 10:
                print(f"Extra user IDs: {sorted(extra)}")


if __name__ == "__main__":
    asyncio.run(simulate_production_traffic())
