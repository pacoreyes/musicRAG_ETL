import logging
import sys
import threading
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Iterable, Optional, Awaitable


class AsyncRateLimiter:
    """
    A simple rate limiter to enforce a maximum number of requests per second.
    """
    def __init__(self, max_rps: float):
        self.max_rps = max_rps
        self.min_interval = 1.0 / max_rps
        self.last_request_time = 0.0
        self.lock = asyncio.Lock()

    async def wait(self):
        """
        Wait until it's safe to make the next request.
        """
        async with self.lock:
            current_time = asyncio.get_event_loop().time()
            elapsed = current_time - self.last_request_time
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self.last_request_time = asyncio.get_event_loop().time()


def process_items_concurrently_with_lock(
    items: Iterable[Any],
    process_func: Callable[[Any, threading.Lock], None],
    max_workers: int = 5,
    logger: Optional[logging.Logger] = None,
):
    """
    Processes items concurrently, passing a shared lock to each worker.
    Ideal for concurrent operations with side effects (e.g., file writing).

    Args:
        items: An iterable of items to process.
        process_func: A function that takes an item and a lock, and performs an action.
        max_workers: The maximum number of threads to use.
        logger: A logger instance for structured logging.
    """
    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {
            executor.submit(process_func, item, lock): item for item in items
        }
        for future in as_completed(future_to_item):
            try:
                # result() is called to raise any exceptions that occurred in the thread
                future.result()
            except Exception as e:
                error_message = f"Error processing item {future_to_item[future]}: {e}"
                if logger:
                    logger.error(error_message)
                else:
                    print(error_message, file=sys.stderr)


def process_items_concurrently(
    items: Iterable[Any],
    process_func: Callable[[Any], Any],
    max_workers: int = 5,
    logger: Optional[logging.Logger] = None,
) -> list[Any]:
    """
    Processes a list of items concurrently using a thread pool.

    Args:
        items: An iterable of items to process.
        process_func: A function that takes one item and returns a result.
        max_workers: The maximum number of threads to use.
        logger: A logger instance for structured logging.

    Returns:
        A list of results from processing the items. It filters out None results.
    """
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {executor.submit(process_func, item): item for item in items}
        for future in as_completed(future_to_item):
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                error_message = f"Error processing item: {e}"
                if logger:
                    logger.error(error_message)
                else:
                    print(error_message, file=sys.stderr)
    return results


async def process_items_concurrently_async(
    items: Iterable[Any],
    process_func: Callable[[Any], Awaitable[Any]],
    max_concurrent_tasks: int = 5,
    logger: Optional[logging.Logger] = None,
) -> list[Any]:
    """
    Processes a list of items concurrently using asyncio with a semaphore.

    Args:
        items: An iterable of items to process.
        process_func: An async function that takes one item and returns a result.
        max_concurrent_tasks: The maximum number of concurrent tasks.
                              Set to 1 for serial execution (e.g., for Wikidata).
        logger: A logger instance for structured logging.

    Returns:
        A list of results from processing the items. It filters out None results.
    """
    semaphore = asyncio.Semaphore(max_concurrent_tasks)
    results = []

    async def sem_task(item: Any):
        async with semaphore:
            try:
                result = await process_func(item)
                return result
            except Exception as e:
                error_message = f"Error processing item: {e}"
                if logger:
                    logger.error(error_message)
                else:
                    print(error_message, file=sys.stderr)
                return None

    tasks = [sem_task(item) for item in items]
    # We use return_exceptions=False (default behavior of gather with simple await)
    # but we are catching exceptions inside sem_task, so gather will return None for failures.
    all_results = await asyncio.gather(*tasks)

    results = [res for res in all_results if res is not None]
    return results


async def process_items_incrementally_async(
    items: Iterable[Any],
    process_func: Callable[[Any], Awaitable[Any]],
    max_concurrent_tasks: int = 5,
    logger: Optional[logging.Logger] = None,
) -> Iterable[Any]:
    """
    Processes items concurrently and yields results as they complete.

    Args:
        items: An iterable of items to process.
        process_func: An async function that takes one item and returns a result.
        max_concurrent_tasks: The maximum number of concurrent tasks.
        logger: A logger instance for structured logging.

    Yields:
        Results from processing items as they become available.
    """
    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def sem_task(item: Any):
        async with semaphore:
            try:
                result = await process_func(item)
                return result
            except Exception as e:
                error_message = f"Error processing item: {e}"
                if logger:
                    logger.error(error_message)
                else:
                    print(error_message, file=sys.stderr)
                return None

    tasks = [sem_task(item) for item in items]
    
    for future in asyncio.as_completed(tasks):
        result = await future
        if result is not None:
            yield result
