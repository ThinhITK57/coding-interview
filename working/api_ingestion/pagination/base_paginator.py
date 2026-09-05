from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BasePaginator(ABC):
    """
    Abstract base for pagination strategies.

    Deep module design:
        Interface (small): get_next_params(), extract_records(), has_more(), update_state()
        Implementation (deep): Each concrete paginator handles its own state tracking,
        parameter injection (query vs body vs nested body), and termination detection.
    """

    def __init__(self, page_size, max_pages=None, max_records=None):
        """Initialize paginator.

        Args:
            page_size: Number of records per page.
            max_pages: Maximum pages to fetch. None = unlimited.
            max_records: Maximum total records to fetch. None = unlimited.
        """
        self._page_size = page_size
        self._max_pages = max_pages
        self._max_records = max_records
        self._current_page = 0
        self._total_records = 0

    @abstractmethod
    def get_next_params(self, base_params=None, base_body=None):
        """Return updated (params, body) for the next page request.

        Args:
            base_params: Original query parameters dict.
            base_body: Original request body dict.

        Returns:
            tuple: (updated_params, updated_body) ready for the HTTP client.
        """
        pass

    @abstractmethod
    def extract_records(self, response_body):
        """Extract data records from the API response.

        Args:
            response_body: Parsed JSON response body.

        Returns:
            list: Extracted records from this page.
        """
        pass

    @abstractmethod
    def update_state(self, response_body, records_count):
        """Update paginator state after receiving a page.

        Args:
            response_body: Parsed JSON response body.
            records_count: Number of records extracted from this page.
        """
        pass

    def has_more(self):
        """Check if more pages should be fetched.

        Returns:
            bool: True if pagination should continue.
        """
        if self._max_pages is not None and self._current_page >= self._max_pages:
            return False
        if self._max_records is not None and self._total_records >= self._max_records:
            return False
        return True

    @property
    def current_page(self):
        return self._current_page

    @property
    def total_records(self):
        return self._total_records

    def get_state(self):
        """Return serializable paginator state for checkpointing."""
        return {
            "current_page": self._current_page,
            "total_records": self._total_records,
        }

    def restore_state(self, state):
        """Restore paginator state from checkpoint."""
        self._current_page = state.get("current_page", 0)
        self._total_records = state.get("total_records", 0)
