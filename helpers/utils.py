import random
import time
from datetime import datetime, timedelta

import requests


def fetch_with_retries(url, params=None, max_retries=10, initial_delay=12):
    retries = 0
    while retries < max_retries:
        if params is not None:
            response = requests.get(url=url, params=params)
        else:
            response = requests.get(url)
        # exceeded api call limit; 429 Too Many Requests
        if response.status_code == 429:
            # Retry with exponential backoff
            # Add jitter for API calls to be randomly staggered
            jittered_delay = random.uniform(initial_delay - 1, initial_delay)
            expo = float(2**retries)
            wait_time = float(
                response.headers.get("Retry-After", jittered_delay * expo)
            )
            print(
                f"Rate Limited: Retrying in {wait_time:.2f}" + " "
                f"seconds... Function Retries Left: {max_retries - retries}"
            )
            time.sleep(wait_time)
            retries += 1
        elif response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()  # Raise an exception for other error codes
    raise Exception("Exceeded maximum retries for the API request")


def get_closest_past_monday(date):
    date = datetime.fromisoformat(date)
    # Calculate the number of days to subtract to reach the closest past Monday
    days_to_subtract = (date.weekday() - 0) % 7  # 0 represents Monday
    # Subtract that number of days to get the previous Monday
    closest_monday = date - timedelta(days=days_to_subtract)
    return closest_monday.date()
