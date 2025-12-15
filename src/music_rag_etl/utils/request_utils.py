import time
from typing import Any, Dict, Optional
import requests

from dagster import AssetExecutionContext


def make_request_with_retries(
    context: AssetExecutionContext,
    url: str,
    method: str = "POST",
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    max_retries: int = 10,
    initial_backoff: int = 2,
    timeout: int = 60,
) -> requests.Response:
    """
    Make a request with exponential backoff for retries.

    Args:
        context: Dagster asset execution context.
        url: Target URL.
        method: HTTP method ('GET' or 'POST').
        params: Request params (for GET) or data (for POST).
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
            request_args = {"headers": headers, "timeout": timeout}
            if method.upper() == "GET":
                request_args["params"] = params
            elif method.upper() == "POST":
                request_args["data"] = params
            else:
                raise ValueError("Method must be 'GET' or 'POST'")

            response = requests.request(method, url, **request_args)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as error:
            wait_time = initial_backoff * (2**attempt)
            context.log.warning(
                f"Attempt {attempt + 1}/{max_retries} for {method} {url} failed ({type(error).__name__}). "
                f"Retrying in {wait_time}s."
            )
            time.sleep(wait_time)

    raise requests.exceptions.RequestException(
        f"Failed to fetch data using {method} for {url} after {max_retries} retries."
    )
