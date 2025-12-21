import time
import asyncio
import ssl
import certifi
from typing import Any, Dict, Optional
import requests
import aiohttp

from dagster import AssetExecutionContext


def create_aiohttp_session() -> aiohttp.ClientSession:
    """
    Creates an aiohttp ClientSession with SSL verification configured using certifi.
    This resolves ClientConnectorCertificateError on some systems (like macOS).
    """
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    return aiohttp.ClientSession(connector=connector)


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


async def async_make_request_with_retries(
    context: AssetExecutionContext,
    url: str,
    method: str = "POST",
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    max_retries: int = 10,
    initial_backoff: int = 2,
    timeout: int = 60,
    session: Optional[aiohttp.ClientSession] = None,
) -> Any:
    """
    Make an async request with exponential backoff for retries using aiohttp.

    Args:
        context: Dagster asset execution context.
        url: Target URL.
        method: HTTP method ('GET' or 'POST').
        params: Request params (for GET) or data (for POST).
        headers: Request headers.
        max_retries: Max retry attempts.
        initial_backoff: Initial backoff in seconds.
        timeout: Timeout per request in seconds.
        session: Optional aiohttp ClientSession. If None, one will be created per request.

    Returns:
        The response object (or its content depending on usage, currently returns the client response context).
        Note: The caller is responsible for reading the response body (e.g. await resp.text())
        if the session is passed in. If session is managed internally, we might need to read it here.
        **For this implementation, we return the text content to be safe as the session might close.**

    Raises:
        aiohttp.ClientError if retries are exhausted.
    """
    should_close_session = False
    if session is None:
        session = create_aiohttp_session()
        should_close_session = True

    try:
        for attempt in range(max_retries):
            try:
                request_args = {
                    "headers": headers,
                    "timeout": aiohttp.ClientTimeout(total=timeout),
                }
                if method.upper() == "GET":
                    request_args["params"] = params
                elif method.upper() == "POST":
                    request_args["data"] = params
                else:
                    raise ValueError("Method must be 'GET' or 'POST'")

                async with session.request(method, url, **request_args) as response:
                    response.raise_for_status()
                    # We read the content here because if we created the session locally,
                    # it will be closed when we return. Even if passed in, it's safer
                    # to return data than a response object tied to a connection.
                    # Depending on content type, we might want json or text.
                    # For generic usage, we'll return text and let caller parse,
                    # or better: return the response data structure if it was a JSON API.
                    # Given the usage in this project, it's mostly JSON.
                    # Let's try to parse JSON, fall back to text.
                    try:
                        return await response.json()
                    except Exception:
                        return await response.text()

            except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                wait_time = initial_backoff * (2**attempt)
                context.log.warning(
                    f"Attempt {attempt + 1}/{max_retries} for {method} {url} failed ({type(error).__name__}). "
                    f"Retrying in {wait_time}s."
                )
                await asyncio.sleep(wait_time)
    finally:
        if should_close_session:
            await session.close()

    raise aiohttp.ClientError(
        f"Failed to fetch data using {method} for {url} after {max_retries} retries."
    )
