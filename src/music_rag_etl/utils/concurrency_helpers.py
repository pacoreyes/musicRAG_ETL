import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Iterable


def process_items_concurrently_with_lock(
    items: Iterable[Any],
    process_func: Callable[[Any, threading.Lock], None],
    max_workers: int = 5,
):
    """
    Processes items concurrently, passing a shared lock to each worker.
    Ideal for concurrent operations with side effects (e.g., file writing).

    Args:
        items: An iterable of items to process.
        process_func: A function that takes an item and a lock, and performs an action.
        max_workers: The maximum number of threads to use.
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
                print(
                    f"Error processing item {future_to_item[future]}: {e}",
                    file=sys.stderr,
                )


def process_items_concurrently(
    items: Iterable[Any],
    process_func: Callable[[Any], Any],
    max_workers: int = 5,
) -> list[Any]:
    """
    Processes a list of items concurrently using a thread pool.

    Args:
        items: An iterable of items to process.
        process_func: A function that takes one item and returns a result.
        max_workers: The maximum number of threads to use.

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
                # This should be logged properly in a real application
                # For now, we print to stderr or can use a passed-in logger.
                print(f"Error processing item: {e}", file=sys.stderr)
    return results
