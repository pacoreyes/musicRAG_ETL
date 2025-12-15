"""
Utility functions for interacting with Wikipedia.
"""

from typing import Optional
import requests
from bs4 import BeautifulSoup
import logging

# Configure logging for this module
logger = logging.getLogger(__name__)


def get_references(wikipedia_url: str) -> int:
    """
    Fetches the number of external links (references) from a Wikipedia page.

    Args:
        wikipedia_url: The full URL of the Wikipedia page.

    Returns:
        The number of external links found on the page, or 0 if an error occurs.
    """
    if not wikipedia_url or not wikipedia_url.startswith("http"):
        logger.warning(f"Invalid Wikipedia URL provided: {wikipedia_url}")
        return 0

    try:
        response = requests.get(wikipedia_url, timeout=10)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the "External links" section and count the links within it
        # This is a common pattern, but might need adjustment for specific Wikipedia templates
        external_links_heading = soup.find("span", {"id": "External_links"})
        if external_links_heading:
            # Navigate up to the section's parent (e.g., <h2>) and then down to list items
            section = external_links_heading.find_parent("h2").find_next_sibling()
            if section:
                # Count <li> elements within the section, which typically contain the links
                return len(section.find_all("li"))
        
        # Fallback: count all external links in the page if specific section not found
        # This might be less precise but captures general external references
        return len(soup.find_all("a", class_="external text"))

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Wikipedia page {wikipedia_url}: {e}")
        return 0
    except Exception as e:
        logger.error(f"An unexpected error occurred while parsing Wikipedia page {wikipedia_url}: {e}")
        return 0
