import os
import ssl
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional

from config import IngestionConfig
from config.api_config import EndpointConfig, PaginationType
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
    def __init__(
            self,
            records,
            batch_id,
            page_number,
            record_count,
            endpoint_name,
            window_start=None,
            window_end=None
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
            "window_end": self.window_end
        }

class Extractor:
    def __init__(self, config, client=None):
        self._config = config
        self._api_config = config.api
        self._job_config = config.job

        if client is not None:
            self._client = client
        else:
            self._client = self._build_client()

    def _build_client(self):
        # Find target endpoint for rate limit
        endpoint = self._find_endpoint(self._job_config.endpoint)
        logger.info(
            "ENDPOINT BODY FIELDS: %s",
            endpoint.body.get("fields", [])
        )


        rate_limiter = RateLimiter(
            requests_per_minute=(endpoint.rate_limit.requests_per_minute if endpoint.rate_limit else None),
            requests_per_day=(endpoint.rate_limit.requests_per_day if endpoint.rate_limit else None)
        )

        retry_executor = RetryExecutor(
            max_attempts=endpoint.retry.max_attempt,
            backoff_factor=endpoint.retry.backoff_factor,
            max_backoff_seconds=endpoint.retry.max_backoff_seconds,
            retry_status_codes=endpoint.retry.retry_status_codes
        )

        circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60
        )

        return ResilientHTTPClient(
            rate_limiter=rate_limiter,
            retry_executor=retry_executor,
            circuit_breaker=circuit_breaker
        )

    def _find_endpoint(self, endpoint_name):
        from config.loader import resolve_endpoint_name
        target_name = resolve_endpoint_name(endpoint_name, self._api_config.endpoints)
        for ep in self._api_config.endpoints:
            if ep.name == endpoint_name or ep.name == target_name:
                return ep
        raise ValueError(f"Endpoint : {endpoint_name} not found in config")

    def _caculate_window(self, endpoint_config, checkpoint, override_start=None, override_end=None):
        """Caculate the incremental window [window_start , window_end]"""
        extraction = self._job_config.extraction
        if extraction.mode in (ExtractionMode.INITIAL, ExtractionMode.FULL):
            return None, None

        if extraction.mode == ExtractionMode.BACKFILL:
            if not override_start or not override_end:
                raise ValueError(
                    "Backfill mode requires window_start and window_end"
                )
            return override_start, override_end
        
        # Mode incremental
        incr = extraction.incremental
        fmt = incr.datetime_format or "%Y-%m-%dT%H:%M:%SZ"
        now = datetime.utcnow()

        if override_end:
            win_end = override_end
        elif extraction.window_end:
            win_end = extraction.window_end
        else:
            win_end = now.strftime(fmt)

        if override_start:
            win_start = override_start
        elif extraction.window_start:
            win_start = extraction.window_start
        elif checkpoint is not None and checkpoint.get_last_watermark():
            last_wm_value  = checkpoint.get_last_watermark()
            try:
                last_wm_str = datetime.strptime(last_wm_value, fmt)
                buf_dt = last_wm_str - timedelta(minutes=incr.lookback_minutes)
                win_start = buf_dt.strftime(fmt)
            except ValueError:
                win_start = last_wm_value
        elif incr.initial_start_time:
            win_start = incr.initial_start_time
        else:
            try:
                end_dt = datetime.strptime(win_end, fmt)
            except ValueError:
                end_dt = now

            win_start = (end_dt - timedelta(days=1)).strftime(fmt)

        return win_start, win_end

    def _interpolate_params(self, data, window_start, window_end):
        if data is None:
            return None
        if isinstance(data, dict):
            return {
                k: self._interpolate_params(v, window_start, window_end) for k, v in data.items()
            }
        if isinstance(data, list):
            return [
                self._interpolate_params(item, window_start, window_end) for item in data
            ]
        if isinstance(data, str):
            res = data
            if window_start is not None:
                res = res.replace("__WINDOW_START__", window_start)
            if window_end is not None:
                res = res.replace("__WINDOW_END__", window_end)

            return res
        return data

    def iterate_batches(self, endpoint_config, window_start=None, window_end=None):
        start_time = time.monotonic()
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S") + f"_{endpoint_config.name}"
        extraction = self._job_config.extraction

        checkpoint = None
        if self._job_config.checkpoint.enabled:
            checkpoint = CheckpointStore(
                checkpoint_path=self._job_config.checkpoint.path or "./checkpoints",
                endpoint_name=endpoint_config.name
            )

        win_start, win_end = self._caculate_window(
            endpoint_config, checkpoint, window_start, window_end
        )

        paginator = self._build_paginator(endpoint_config)

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
                    "total_records_so_far": paginator.total_records
                }))

        # Set auth heaeders
        headers = dict(endpoint_config.headers)
        if endpoint_config.auth:
            auth = TokenAuth(endpoint_config.auth)
            headers.update(auth.get_headers())

        base_url = self._api_config.base_url.rstrip("/")
        url = base_url + endpoint_config.path

        base_params = self._interpolate_params(
            dict(endpoint_config.params), win_start, win_end
        )

        # base_body = self._interpolate_params(
        #     endpoint_config.body, win_start, win_end
        # )
        import copy
        base_body = copy.deepcopy(endpoint_config.body)
        if extraction.mode in (
            ExtractionMode.INITIAL, ExtractionMode.FULL
        ):
            logger.info(
                "COLD START: watermark_field=%s",
                extraction.incremental.watermark_field
            )
            watermark_field = extraction.incremental.watermark_field
            base_body = self._remove_watermark_filter(
                base_body, watermark_field
            )

        else:
            base_body = self._interpolate_params(
                base_body, win_start, win_end
            )

        logger.info(
            "EXTRACTION MODE: %s",
            extraction.mode.value
        )

        logger.info(
            "WINDOW: start=%s, end=%s",
            win_start,
            win_end
        )

        logger.info(
            "BASE BODY AFTER MODE PROCESSING: %s",
            json.dumps(
                base_body,
                ensure_ascii=False,
                indent=2
            )
        )

        logger.info(
            "BASE BODY FIELDS: %s",
            base_body.get("fields", []) if base_body else None
        )

        logger.info(
            "FINAL REQUEST BODY: %s",
            json.dumps(base_body, ensure_ascii=False)
        )


        logger.info(json.dumps({
            "event": "extracted_started",
            "endpoint": endpoint_config.name,
            "url": url,
            "batch_id": batch_id,
            "mode": extraction.mode.value,
            "window_start": win_start,
            "window_end": win_end,
            "max_records": extraction.max_records,
            "max_pages": extraction.max_pages,
            "max_runtime_seconds": extraction.max_runtime_seconds
        }))

        max_watermark_seen = None
        watermark_field = (
            extraction.incremental.watermark_field if extraction.mode == ExtractionMode.INCREMENTAL else None
        )

        while paginator.has_more():
            if extraction.max_runtime_seconds is not None:
                elapsed = time.monotonic() - start_time
                if elapsed >= extraction.max_runtime_seconds:
                    logger.warning(json.dumps({
                        "event": "extraction_runtime_limit",
                        "elapsed_seconds": round(elapsed, 1),
                        "max_runtime_seconds": extraction.max_runtime_seconds,
                        "total_records": paginator.total_records
                    }))
                    break

            params, body = paginator.get_next_params(
                base_params=base_params,
                base_body=base_body
            )
            logger.info(
                "PAGINATED BODY FIELDS: %s",
                body.get("fields", []) if body else None
            )

            logger.info(
                "FINAL REQUEST BODY: %s",
                json.dumps(body, ensure_ascii=False, indent=2)
            )

            response = self._client.request(
                method=endpoint_config.method.value,
                url=url,
                headers=headers,
                params=params if params else None,
                json_body=body if body else None,
                timeout=endpoint_config.timeout_seconds
            )

            records = paginator.extract_records(response.get("body", {}))
            records_count = len(records)

            paginator.update_state(response.get("body", {}), records_count)

            if watermark_field and records:
                for rec in records:
                    if isinstance(rec, dict):
                        wm_val = rec.get(watermark_field)
                        if wm_val is not None:
                            wm_str = str(wm_val)
                            if max_watermark_seen is None or wm_str > max_watermark_seen:
                                max_watermark_seen = wm_str

            if records_count == 0:
                logger.info(json.dumps({
                    "event": "extraction_empty_page",
                    "page": paginator.current_page,
                    "total_records": paginator.total_records
                }))
                break

            batch = Batch(
                records=records,
                batch_id=batch_id,
                page_number=paginator.current_page,
                record_count=records_count,
                endpoint_name=endpoint_config.name,
                window_start=win_start,
                window_end=win_end
            )
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
                    "completed": False
                })
            yield batch

        if checkpoint is not None:
            final_wm = max_watermark_seen or win_end
            checkpoint.mark_completed(
                final_watermark=final_wm,
                window_start=win_start,
                window_end=win_end
            )

        elapsed = time.monotonic() - start_time
        logger.info(json.dumps({
            "event": "extraction_completed",
            "endpoint": endpoint_config.name,
            "total_pages": paginator.current_page,
            "total_records": paginator.total_records,
            "elapsed_seconds": round(elapsed, 1),
            "batch_id": batch_id,
            "watermark": max_watermark_seen
        }))

    def _build_paginator(self, endpoint_config):
        pagination = endpoint_config.pagination
        extraction = self._job_config.extraction

        if pagination is None or pagination.type == PaginationType.NONE:
            return OffsetPaginator(
                page_size=extraction.batch_size,
                max_pages=1
            )

        if pagination.type == PaginationType.OFFSET:
            return OffsetPaginator(
                page_size=pagination.page_size,
                offset_param=pagination.offset_param,
                limit_param=pagination.limit_param,
                location=pagination.location,
                max_pages=extraction.max_pages or pagination.max_pages,
                max_records=extraction.max_records or pagination.max_records
            )

        return OffsetPaginator(
            page_size=pagination.page_size,
            max_pages=extraction.max_pages or pagination.max_pages,
            max_records=extraction.max_records
        )

    def extract_all_endpoints(self, endpoint_names=None, window_start=None, window_end=None):
        if endpoint_names is None:
            endpoints = self._api_config.endpoints

        else:
            endpoints = [
                self._find_endpoint(name) for name in endpoint_names
            ]

        for endpoint_config in endpoints:
            logger.info(json.dumps({
                "event": "multi_endpoint_starting",
                "endpoint": endpoint_config.name,
                "total_endpoints": len(endpoints)
            }))

            for batch in self.iterate_batches(
                endpoint_config,
                window_start=window_start,
                window_end=window_end
            ):
                yield batch

    def _remove_watermark_filter(self, data, watermark_field): 
        if not isinstance(data, dict):
            return data

        data = dict(data)
        where = data.get("where")

        if not isinstance(where, dict):
            return data

        conditions = where.get("and")

        if not isinstance(conditions, list):
            return data

        filtered_conditions = []

        for condition in conditions:
            if not isinstance(condition, dict):
                filtered_conditions.append(condition)
                continue

            left_expression = condition.get("leftExpression", {})
            field_name = left_expression.get("fieldName")

            if field_name == watermark_field:
                continue
            filtered_conditions.append(condition)

        if filtered_conditions:
            data['where'] = dict(where)
            data["where"]['and'] = filtered_conditions
        else:
            data.pop("where", None)

        return data
