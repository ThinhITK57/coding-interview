from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BasePaginator(ABC):
    def __init__(self, page_size, max_pages=None, max_records=None):
        self._page_size = page_size
        self._max_pages = max_pages
        self._max_records = max_records
        self._current_page = 0
        self._total_records = 0

    @abstractmethod
    def get_next_params(self, base_params=None, base_body=None):
        """
        Return tuple includes: updated_params, updated_body ; ready for the http client
        """
        pass

    @abstractmethod
    def extract_records(self, response_body):
        """Return list Extracted records from this page"""
        pass

    @abstractmethod
    def update_state(self, response_body, records_count):
        """Update paginator state after receiving a page"""
        pass


    def has_more(self):
        """Return true if paginations should continue"""
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
        return {
            "current_page": self._current_page,
            "total_records": self._total_records
        }

    def restore_state(self, state):
        self._current_page = state.get("current_page", 0)
        self._total_records = state.get("total_records", 0)        