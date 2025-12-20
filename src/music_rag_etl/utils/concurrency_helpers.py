import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Iterable, Optional


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
