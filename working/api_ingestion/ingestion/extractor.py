import os
import ssl
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional

from config import IngestionConfig
from config.api_config import PaginationType, EndpointConfig
from config.job_config import ExtractionMode
from auth.token_auth import TokenAuth
from client.resilient_client import ResilientHTTPClient
from reliability.rate_limiter import RateLimiter
from reliability.retry import RetryExecutor
from reliability.circuit_breaker import CircuitBreaker
from pagination.offset_paginator import OffsetPaginator
from pagination.base_paginator import BasePaginator
from checkpoint.checkpoint_store import CheckpointStore

logger = logging.getLogger(__name__)


class Batch:
    """Container for a batch of extracted records."""

    def __init__(
        self,
        records,
        batch_id,
        page_number,
        record_count,
        endpoint_name,
        window_start=None,
        window_end=None,
    ):
        self.records = records
        self.batch_id = batch_id
        self.page_number = page_number
        self.record_count = record_count
        self.endpoint_name = endpoint_name
        self.window_start = window_start
        self.window_end = window_end

    def to_dict(self):
        return {
            "batch_id": self.batch_id,
            "page_number": self.page_number,
            "record_count": self.record_count,
            "endpoint_name": self.endpoint_name,
            "window_start": self.window_start,
            "window_end": self.window_end,
        }


class Extractor:
    """
    Multi-endpoint paginated data extractor with checkpoint resume.

    Deep module design:
        Interface (small):  iterate_batches(endpoint_config) -> Iterator[Batch]
        Implementation (deep): Auth header injection, paginator strategy selection,
        resilient HTTP calls (rate limit + retry + circuit breaker), atomic
        checkpoint commit after each batch, resume-from-crash, runtime guard,
        structured JSON logging with page-level progress tracking.

    Usage:
        extractor = Extractor(config)
        for batch in extractor.iterate_batches(endpoint_config):
            process(batch.records)
    """

    def __init__(self, config, client=None):
        """Initialize extractor.

        Args:
            config: IngestionConfig with api and job settings.
            client: Optional ResilientHTTPClient. If None, creates one from config.
        """
        self._config = config
        self._api_config = config.api
        self._job_config = config.job

        # Build client from config if not injected
        if client is not None:
            self._client = client
        else:
            self._client = self._build_client()

    def _build_client(self):
        """Build ResilientHTTPClient from the first endpoint's config."""
        # Find the target endpoint for rate limit / retry config
        endpoint = self._find_endpoint(self._job_config.endpoint)

        rate_limiter = RateLimiter(
            requests_per_minute=(
                endpoint.rate_limit.requests_per_minute
                if endpoint.rate_limit else None
            ),
            requests_per_day=(
                endpoint.rate_limit.requests_per_day
                if endpoint.rate_limit else None
            ),
        )

        retry_executor = RetryExecutor(
            max_attempts=endpoint.retry.max_attempts,
            backoff_factor=endpoint.retry.backoff_factor,
            max_backoff_seconds=endpoint.retry.max_backoff_seconds,
            retry_status_codes=endpoint.retry.retry_status_codes,
        )

        circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60,
        )

        return ResilientHTTPClient(
            rate_limiter=rate_limiter,
            retry_executor=retry_executor,
            circuit_breaker=circuit_breaker,
        )

    def _find_endpoint(self, endpoint_name):
        """Find EndpointConfig by name or alias."""
        alias_map = {
            "muc_1": "tasks", "task": "tasks", "tasks": "tasks",
            "muc_2": "projects", "project": "projects", "projects": "projects",
            "muc_3": "bsc", "objective": "bsc", "bsc": "bsc",
            "muc_4": "assignments", "assignment": "assignments", "assignments": "assignments",
            "muc_5": "targets", "target": "targets", "targets": "targets",
        }
        target_name = alias_map.get(endpoint_name.lower() if endpoint_name else "", endpoint_name)
        for ep in self._api_config.endpoints:
            if ep.name == endpoint_name or ep.name == target_name:
                return ep
        raise ValueError(f"Endpoint '{endpoint_name}' not found in config")

    def _calculate_window(
        self,
        endpoint_config,
        checkpoint,
        override_start=None,
        override_end=None,
    ):
        """Calculate the incremental window [window_start, window_end).

        Priority:
            1. Explicit override (CLI/caller).
            2. Configured window in ExtractionConfig.
            3. Checkpoint last watermark minus lookback buffer.
            4. Initial start time in IncrementalConfig.
            5. Default 24-hour lookback from window_end.
        """
        extraction = self._job_config.extraction
        if extraction.mode != ExtractionMode.INCREMENTAL:
            return None, None

        incr = extraction.incremental
        fmt = incr.datetime_format or "%Y-%m-%dT%H:%M:%SZ"
        now = datetime.utcnow()

        # 1. Determine window_end
        if override_end:
            win_end = override_end
        elif extraction.window_end:
            win_end = extraction.window_end
        else:
            win_end = now.strftime(fmt)

        # 2. Determine window_start
        if override_start:
            win_start = override_start
        elif extraction.window_start:
            win_start = extraction.window_start
        elif checkpoint is not None and checkpoint.get_last_watermark():
            last_wm_str = checkpoint.get_last_watermark()
            try:
                last_wm_dt = datetime.strptime(last_wm_str, fmt)
                buf_dt = last_wm_dt - timedelta(minutes=incr.lookback_minutes)
                win_start = buf_dt.strftime(fmt)
            except ValueError:
                win_start = last_wm_str
        elif incr.initial_start_time:
            win_start = incr.initial_start_time
        else:
            # Default to 24-hour lookback
            try:
                end_dt = datetime.strptime(win_end, fmt)
            except ValueError:
                end_dt = now
            win_start = (end_dt - timedelta(days=1)).strftime(fmt)

        return win_start, win_end

    def _interpolate_params(self, data, window_start, window_end):
        """Recursively replace ${WINDOW_START} and ${WINDOW_END} placeholders."""
        if data is None:
            return None
        if isinstance(data, dict):
            return {
                k: self._interpolate_params(v, window_start, window_end)
                for k, v in data.items()
            }
        if isinstance(data, list):
            return [
                self._interpolate_params(item, window_start, window_end)
                for item in data
            ]
        if isinstance(data, str):
            res = data
            if window_start:
                res = res.replace("${WINDOW_START}", window_start)
            if window_end:
                res = res.replace("${WINDOW_END}", window_end)
            return res
        return data

    def iterate_batches(
        self,
        endpoint_config,
        window_start=None,
        window_end=None,
    ):
        """Extract all pages from an endpoint, yielding batches.

        Resumes from checkpoint if one exists. Commits checkpoint
        after each successful page. Stops when pagination exhausted
        or max_records/max_pages/max_runtime_seconds reached.

        Args:
            endpoint_config: EndpointConfig to extract from.
            window_start: Optional ISO timestamp override for incremental start.
            window_end: Optional ISO timestamp override for incremental end.

        Yields:
            Batch: Container with records, batch_id, page_number, record_count.
        """
        start_time = time.monotonic()
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S") + f"_{endpoint_config.name}"
        extraction = self._job_config.extraction

        # Setup checkpoint
        checkpoint = None
        if self._job_config.checkpoint.enabled:
            checkpoint = CheckpointStore(
                checkpoint_path=self._job_config.checkpoint.path or "./checkpoints",
                endpoint_name=endpoint_config.name,
            )

        # Calculate incremental window [window_start, window_end)
        win_start, win_end = self._calculate_window(
            endpoint_config, checkpoint, window_start, window_end
        )

        # Setup paginator
        paginator = self._build_paginator(endpoint_config)

        # Resume from checkpoint if available (interrupted run)
        if checkpoint is not None:
            saved_state = checkpoint.load(skip_completed=True)
            if saved_state is not None:
                paginator.restore_state(saved_state.get("paginator_state", {}))
                batch_id = saved_state.get("batch_id", batch_id)
                logger.info(json.dumps({
                    "event": "extraction_resuming",
                    "endpoint": endpoint_config.name,
                    "from_page": paginator.current_page,
                    "from_offset": saved_state.get("last_offset", 0),
                    "total_records_so_far": paginator.total_records,
                }))

        # Setup auth headers
        headers = dict(endpoint_config.headers)
        if endpoint_config.auth:
            auth = TokenAuth(endpoint_config.auth)
            headers.update(auth.get_headers())

        # Build full URL
        base_url = self._api_config.base_url.rstrip("/")
        url = base_url + endpoint_config.path

        # Interpolate window parameters into base params and body
        base_params = self._interpolate_params(
            dict(endpoint_config.params), win_start, win_end
        )
        base_body = self._interpolate_params(
            endpoint_config.body, win_start, win_end
        )

        logger.info(json.dumps({
            "event": "extraction_started",
            "endpoint": endpoint_config.name,
            "url": url,
            "batch_id": batch_id,
            "mode": extraction.mode.value,
            "window_start": win_start,
            "window_end": win_end,
            "max_records": extraction.max_records,
            "max_pages": extraction.max_pages,
            "max_runtime_seconds": extraction.max_runtime_seconds,
        }))

        max_watermark_seen = None
        watermark_field = (
            extraction.incremental.watermark_field
            if extraction.mode == ExtractionMode.INCREMENTAL else None
        )

        # Main extraction loop
        while paginator.has_more():
            # Runtime guard
            if extraction.max_runtime_seconds is not None:
                elapsed = time.monotonic() - start_time
                if elapsed >= extraction.max_runtime_seconds:
                    logger.warning(json.dumps({
                        "event": "extraction_runtime_limit",
                        "elapsed_seconds": round(elapsed, 1),
                        "max_runtime_seconds": extraction.max_runtime_seconds,
                        "total_records": paginator.total_records,
                    }))
                    break

            # Get pagination params with window bounds injected
            params, body = paginator.get_next_params(
                base_params=base_params,
                base_body=base_body,
            )

            # Execute request
            response = self._client.request(
                method=endpoint_config.method.value,
                url=url,
                headers=headers,
                params=params if params else None,
                json_body=body if body else None,
                timeout=endpoint_config.timeout_seconds,
            )

            # Extract records
            records = paginator.extract_records(response.get("body", {}))
            records_count = len(records)

            # Update paginator state
            paginator.update_state(response.get("body", {}), records_count)

            # Scan watermark in records
            if watermark_field and records:
                for rec in records:
                    if isinstance(rec, dict):
                        wm_val = rec.get(watermark_field)
                        if wm_val is not None:
                            wm_str = str(wm_val)
                            if max_watermark_seen is None or wm_str > max_watermark_seen:
                                max_watermark_seen = wm_str

            # Empty page = end of data
            if records_count == 0:
                logger.info(json.dumps({
                    "event": "extraction_empty_page",
                    "page": paginator.current_page,
                    "total_records": paginator.total_records,
                }))
                break

            # Create batch
            batch = Batch(
                records=records,
                batch_id=batch_id,
                page_number=paginator.current_page,
                record_count=records_count,
                endpoint_name=endpoint_config.name,
                window_start=win_start,
                window_end=win_end,
            )

            # Commit checkpoint
            if checkpoint is not None:
                checkpoint.commit({
                    "batch_id": batch_id,
                    "last_offset": paginator.get_state().get("current_offset", paginator.total_records),
                    "total_records": paginator.total_records,
                    "current_page": paginator.current_page,
                    "paginator_state": paginator.get_state(),
                    "watermark": max_watermark_seen,
                    "window_start": win_start,
                    "window_end": win_end,
                    "completed": False,
                })

            yield batch

        # Mark checkpoint as completed with watermark
        if checkpoint is not None:
            final_wm = max_watermark_seen or win_end
            checkpoint.mark_completed(
                final_watermark=final_wm,
                window_start=win_start,
                window_end=win_end,
            )

        elapsed = time.monotonic() - start_time
        logger.info(json.dumps({
            "event": "extraction_completed",
            "endpoint": endpoint_config.name,
            "total_pages": paginator.current_page,
            "total_records": paginator.total_records,
            "elapsed_seconds": round(elapsed, 1),
            "batch_id": batch_id,
            "watermark": max_watermark_seen,
        }))

    def _build_paginator(self, endpoint_config):
        """Build the appropriate paginator from endpoint config."""
        pagination = endpoint_config.pagination
        extraction = self._job_config.extraction

        if pagination is None or pagination.type == PaginationType.NONE:
            # No pagination: use offset with page_size=max_records to get single page
            return OffsetPaginator(
                page_size=extraction.batch_size,
                max_pages=1,
            )

        if pagination.type == PaginationType.OFFSET:
            return OffsetPaginator(
                page_size=pagination.page_size,
                offset_param=pagination.offset_param,
                limit_param=pagination.limit_param,
                location=pagination.location,
                max_pages=extraction.max_pages or pagination.max_pages,
                max_records=extraction.max_records or pagination.max_records,
            )

        # Default fallback
        return OffsetPaginator(
            page_size=pagination.page_size,
            max_pages=extraction.max_pages or pagination.max_pages,
            max_records=extraction.max_records,
        )

    def extract_all_endpoints(
        self,
        endpoint_names=None,
        window_start=None,
        window_end=None,
    ):
        """Extract from multiple endpoints sequentially.

        Args:
            endpoint_names: List of endpoint names to extract.
                If None, extracts from all configured endpoints.
            window_start: Optional ISO timestamp override.
            window_end: Optional ISO timestamp override.

        Yields:
            Batch: Batches from each endpoint in sequence.
        """
        if endpoint_names is None:
            endpoints = self._api_config.endpoints
        else:
            endpoints = [
                self._find_endpoint(name)
                for name in endpoint_names
            ]

        for endpoint_config in endpoints:
            logger.info(json.dumps({
                "event": "multi_endpoint_starting",
                "endpoint": endpoint_config.name,
                "total_endpoints": len(endpoints),
            }))

            for batch in self.iterate_batches(
                endpoint_config,
                window_start=window_start,
                window_end=window_end,
            ):
                yield batch
