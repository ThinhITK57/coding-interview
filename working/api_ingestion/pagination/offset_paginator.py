import copy
import logging
import json

from pagination.base_paginator import BasePaginator

logger = logging.getLogger(__name__)


class OffsetPaginator(BasePaginator):
    """
    Offset/limit pagination strategy.

    Handles both query-parameter and nested-body-parameter offset injection.
    For Clarizen API, offset params live inside body["paging"]["from"] and
    body["paging"]["limit"], not at the top level.

    Deep module design:
        Interface: Inherited from BasePaginator (get_next_params, extract_records, etc.)
        Implementation: Detects nested body paths, deep-copies body to avoid mutation,
        injects offset/limit at correct nesting level.
    """

    def __init__(
        self,
        page_size,
        offset_param="offset",
        limit_param="limit",
        location="query",
        max_pages=None,
        max_records=None,
        records_key="entities",
    ):
        """Initialize offset paginator.

        Args:
            page_size: Records per page.
            offset_param: Name of the offset parameter (e.g. "from", "offset").
            limit_param: Name of the limit parameter (e.g. "limit", "count").
            location: Where pagination params live: "query" or "body".
            max_pages: Maximum pages to fetch.
            max_records: Maximum total records.
            records_key: Key in response body containing the records list.
                Defaults to "entities" (Clarizen convention).
        """
        super().__init__(page_size, max_pages, max_records)
        self._offset_param = offset_param
        self._limit_param = limit_param
        self._location = location
        self._records_key = records_key
        self._current_offset = 0
        self._has_more = True

    def get_next_params(self, base_params=None, base_body=None):
        """Inject offset/limit into query params or body (including nested).

        Returns:
            tuple: (params_dict, body_dict) with pagination values injected.
        """
        params = dict(base_params) if base_params else {}
        body = copy.deepcopy(base_body) if base_body else {}

        if self._location == "query":
            params[self._offset_param] = self._current_offset
            params[self._limit_param] = self._page_size
        elif self._location == "body":
            # Detect if params are nested (e.g. body["paging"]["from"])
            self._inject_into_body(body)

        return params, body

    def _inject_into_body(self, body):
        """Inject offset/limit into body, handling nested structures.

        Searches for existing keys matching offset_param and limit_param
        at any nesting level. If found nested (e.g. body.paging.from),
        updates in-place. Otherwise sets at top level.
        """
        # First try: find nested location by scanning body structure
        for key, value in body.items():
            if isinstance(value, dict):
                if self._offset_param in value and self._limit_param in value:
                    # Found nested container (e.g. body["paging"])
                    value[self._offset_param] = self._current_offset
                    value[self._limit_param] = self._page_size
                    return

        # Fallback: set at top level
        body[self._offset_param] = self._current_offset
        body[self._limit_param] = self._page_size

    def extract_records(self, response_body):
        """Extract records list from response body.

        Tries response_body[records_key] first, then falls back to
        treating the entire response as a list.

        Args:
            response_body: Parsed JSON response.

        Returns:
            list: Extracted records.
        """
        if isinstance(response_body, dict):
            records = response_body.get(self._records_key)
            if records is not None:
                return records if isinstance(records, list) else []
            # Fallback: try common keys
            for key in ["data", "results", "items", "records"]:
                records = response_body.get(key)
                if isinstance(records, list):
                    return records
        elif isinstance(response_body, list):
            return response_body
        return []

    def update_state(self, response_body, records_count):
        """Update offset and page counters after receiving a page."""
        self._current_page += 1
        self._current_offset += records_count
        self._total_records += records_count

        # Termination: fewer records than page_size means last page
        if records_count < self._page_size:
            self._has_more = False

        logger.info(json.dumps({
            "event": "page_extracted",
            "page": self._current_page,
            "records_this_page": records_count,
            "total_records": self._total_records,
            "current_offset": self._current_offset,
            "has_more": self._has_more,
        }))

    def has_more(self):
        """Check if more pages should be fetched."""
        if not self._has_more:
            return False
        return super().has_more()

    def get_state(self):
        """Return serializable state for checkpointing."""
        state = super().get_state()
        state["current_offset"] = self._current_offset
        state["has_more"] = self._has_more
        return state

    def restore_state(self, state):
        """Restore state from checkpoint."""
        super().restore_state(state)
        self._current_offset = state.get("current_offset", 0)
        self._has_more = state.get("has_more", True)
