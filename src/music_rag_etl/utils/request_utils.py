import time
from typing import Any, Dict

import requests


def make_request_with_retries(
    url: str,
    params: Dict[str, Any],
    headers: Dict[str, str],
    max_retries: int = 8,
    initial_backoff: int = 2,
    timeout: int = 60,
) -> requests.Response:
    """
    Make a request with exponential backoff for retries.

    Args:
        url: Target URL.
        params: Request params.
        headers: Request headers.
        max_retries: Max retry attempts.
        initial_backoff: Initial backoff in seconds.
        timeout: Timeout per request in seconds.

    Returns:
        requests.Response on success.

    Raises:
        requests.exceptions.RequestException if retries are exhausted.
    """
    for attempt in range(max_retries):
        try:
            response = requests.post(
                url, data=params, headers=headers, timeout=timeout
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as error:
            wait_time = initial_backoff * (2**attempt)
            print(
                f"Attempt {attempt + 1}/{max_retries} failed "
                f"({type(error).__name__}). Retrying in {wait_time}s..."
            )
            time.sleep(wait_time)

    raise requests.exceptions.RequestException(
        f"Failed to fetch data after {max_retries} retries."
    )
