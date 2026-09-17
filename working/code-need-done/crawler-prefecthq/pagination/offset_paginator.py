import copy
import logging
import json

from pagination.base_paginator import BasePaginator

logger = logging.getLogger(__name__)


class OffsetPaginator(BasePaginator):
    def __init__(self, page_size, offset_param="offset", limit_param="limit", location="query", max_pages=None, max_records=None,
                 records_key="entities"):
        super().__init__(page_size, max_pages, max_records)
        self._offset_param = offset_param
        self._limit_param = limit_param
        self._location = location
        self._records_key = records_key
        self._current_offset = 0
        self._has_more = True

    def get_next_params(self, base_params=None, base_body=None):
        params = dict(base_params) if base_params else {}
        body = copy.deepcopy(base_body) if base_body else {}

        if self._location == "query":
            params[self._offset_param] = self._current_offset
            params[self._limit_param] = self._page_size

        elif self._location == "body":
            self._inject_into_body(body)

        return params, body

    def _inject_into_body(self, body):
        for key, value in body.items():
            if isinstance(value, dict):
                if self._offset_param in value and self._limit_param in value:
                    value[self._offset_param] = self._current_offset
                    value[self._limit_param] = self._page_size
                    return

        body[self._offset_param] = self._current_offset
        body[self._limit_param] = self._page_size


    def extract_records(self, response_body):
        if isinstance(response_body,dict):
            records = response_body.get(self._records_key)
            if records is not None:
                return records if isinstance(records, list) else []

            for key in ["data", "results", "items", "records"]:
                records = response_body.get(key)
                if isinstance(records, list):
                    return records

        elif isinstance(response_body, list):
            return response_body

        return []

    def update_state(self, response_body, records_count):
        self._current_page += 1
        self._current_offset += records_count
        self._total_records += records_count

        if records_count < self._page_size:
            self._has_more = False

        logger.info(json.dumps(
            {
                "event": "page_extracted",
                "page": self._current_page,
                "records_this_page": records_count,
                "total_records": self._total_records,
                "current_offset": self._current_offset,
                "has_more": self._has_more
            }
        ))

    def has_more(self):
        if not self._has_more:
            return False
        return super().has_more()

    def get_state(self):
        state = super().get_state()
        state["current_offset"] = self._current_offset
        state["has_more"] = self._has_more
        return state

    def restore_state(self, state):
        super().restore_state(state)
        self._current_offset = state.get("current_offset", 0)
        self._has_more = state.get("has_more", True)
