import requests
import time
import random


def fetch_with_retries(url, max_retries=10, initial_delay=12):
    retries = 0
    while retries < max_retries:
        response = requests.get(url)
        # exceeded api call limit; 429 Too Many Requests
        if response.status_code == 429:
            # Retry with exponential backoff
            # Add jitter for API calls to be randomly staggered
            jittered_delay = random.uniform(initial_delay-1, initial_delay)
            expo = 2 ** retries
            wait_time = int(response.headers.get("Retry-After", jittered_delay * expo))
            print(f"Rate Limited: Retrying in {wait_time:.2f} seconds... Function Retries Left: {max_retries - retries}")
            time.sleep(wait_time)
            retries += 1
        elif response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status() # Raise an exception for other error codes
    raise Exception("Exceeded maximum retries for the API request")