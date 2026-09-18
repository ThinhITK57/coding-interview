import copy
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

from epm.monitoring.metrics import EndpointConfig, Batch, ExtractionMode
from epm.client.resilient_client import ResilientHTTPClient
from epm.storage.state_store import StateStore

logger = logging.getLogger(__name__)


class LightweightExtractor:
    """Pure-Python, Zero-Spark API Extractor for EPM Clarizen.
    Handles:
    - Offset pagination
    - Incremental window computation & template replacement
    - Stateful checkpointing across pages
    - Watermark tracking
    """

    def __init__(
        self,
        endpoint_config: EndpointConfig,
        state_store: Optional[StateStore] = None,
        client: Optional[ResilientHTTPClient] = None,
        watermark_field: str = "LastUpdatedOn",
        lookback_minutes: int = 15,
        base_url: str = "https://api2.clarizen.com",
    ):
        self.config = endpoint_config
        self.state_store = state_store or StateStore()
        self.client = client or ResilientHTTPClient(
            endpoint_config=endpoint_config,
            state_store=self.state_store,
            base_url=base_url,
        )
        self.watermark_field = watermark_field
        self.lookback_minutes = lookback_minutes

    def calculate_window(
        self,
        mode: str = ExtractionMode.INCREMENTAL.value,
        override_start: Optional[str] = None,
        override_end: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Calculates [window_start, window_end] for incremental crawling."""
        if mode == ExtractionMode.FULL.value:
            return None, None

        if mode == ExtractionMode.BACKFILL.value:
            if not override_start or not override_end:
                raise ValueError("Backfill mode requires both override_start and override_end")
            return override_start, override_end

        fmt = "%Y-%m-%dT%H:%M:%SZ"
        now = datetime.now(timezone.utc)
        win_end = override_end or now.strftime(fmt)

        if override_start:
            win_start = override_start
        else:
            cp = self.state_store.get_checkpoint(self.config.name)
            last_wm = cp.get("last_watermark") if cp else None
            if last_wm:
                try:
                    dt = datetime.strptime(last_wm, fmt)
                    win_start = (dt - timedelta(minutes=self.lookback_minutes)).strftime(fmt)
                except ValueError:
                    win_start = last_wm
            else:
                # Default to 24h ago if no checkpoint
                win_start = (now - timedelta(days=1)).strftime(fmt)

        return win_start, win_end

    def _interpolate_template(self, obj: Any, win_start: Optional[str], win_end: Optional[str]) -> Any:
        if isinstance(obj, dict):
            return {k: self._interpolate_template(v, win_start, win_end) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._interpolate_template(v, win_start, win_end) for v in obj]
        if isinstance(obj, str):
            res = obj
            if win_start:
                res = res.replace("__WINDOW_START__", win_start)
            if win_end:
                res = res.replace("__WINDOW_END__", win_end)
            return res
        return obj

    def _remove_watermark_filter(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Removes the incremental watermark filter for FULL extraction."""
        data = copy.deepcopy(body)
        where = data.get("where")
        if isinstance(where, dict):
            and_conds = where.get("and")
            if isinstance(and_conds, list):
                # Filter out conditions comparing against watermark_field
                filtered = [c for c in and_conds if isinstance(c, dict) and c.get("leftExpression", {}).get("fieldName") != self.watermark_field]
                if filtered:
                    data["where"]["and"] = filtered
                else:
                    data.pop("where", None)
        return data

    def extract_batches(
        self,
        mode: str = ExtractionMode.INCREMENTAL.value,
        window_start: Optional[str] = None,
        window_end: Optional[str] = None,
        batch_size: Optional[int] = None,
        max_pages: Optional[int] = None,
        batch_id: Optional[str] = None,
    ) -> Iterator[Batch]:
        """Generator that fetches pages and yields Batches of raw records."""
        now_utc = datetime.now(timezone.utc)
        batch_id = batch_id or f"{now_utc.strftime('%Y%m%d_%H%M%S')}_{self.config.name}"
        win_start, win_end = self.calculate_window(mode, window_start, window_end)

        page_size = batch_size or self.config.pagination.page_size
        max_p = max_pages or self.config.pagination.max_pages

        # Prepare request payload
        base_body = copy.deepcopy(self.config.body)
        if mode == ExtractionMode.FULL.value:
            base_body = self._remove_watermark_filter(base_body)
        else:
            base_body = self._interpolate_template(base_body, win_start, win_end)

        base_params = self._interpolate_template(self.config.params, win_start, win_end)

        logger.info(json.dumps({
            "event": "extraction_started",
            "endpoint": self.config.name,
            "mode": mode,
            "batch_id": batch_id,
            "window_start": win_start,
            "window_end": win_end,
            "page_size": page_size
        }))

        current_offset = 0
        current_page = 0
        total_records = 0
        max_watermark_seen = None

        has_more = True
        while has_more and current_page < max_p:
            current_page += 1
            payload = copy.deepcopy(base_body)
            payload[self.config.pagination.offset_param] = current_offset
            payload[self.config.pagination.limit_param] = page_size

            resp = self.client.request(
                method=self.config.method,
                params=base_params,
                json_body=payload,
            )

            # Extract records
            records = resp.get("entities")
            if records is None:
                for k in ["data", "results", "items", "records"]:
                    if isinstance(resp.get(k), list):
                        records = resp.get(k)
                        break
            if records is None and isinstance(resp, list):
                records = resp
            records = records or []

            records_count = len(records)
            current_offset += records_count
            total_records += records_count

            # Check watermark values
            for r in records:
                if isinstance(r, dict):
                    wm_val = r.get(self.watermark_field)
                    if wm_val:
                        wm_str = str(wm_val)
                        if max_watermark_seen is None or wm_str > max_watermark_seen:
                            max_watermark_seen = wm_str

            # Check hasMore
            paging_info = resp.get("paging", {})
            has_more = paging_info.get("hasMore", records_count >= page_size)

            logger.info(json.dumps({
                "event": "page_extracted",
                "endpoint": self.config.name,
                "page": current_page,
                "records_count": records_count,
                "total_records": total_records,
                "has_more": has_more
            }))

            # Commit partial checkpoint
            self.state_store.save_checkpoint(
                endpoint=self.config.name,
                last_watermark=max_watermark_seen or win_start,
                last_offset=current_offset,
                last_page=current_page,
                total_records=total_records,
                window_start=win_start,
                window_end=win_end,
                completed=False,
            )

            batch = Batch(
                records=records,
                batch_id=batch_id,
                page_number=current_page,
                record_count=records_count,
                endpoint_name=self.config.name,
                window_start=win_start,
                window_end=win_end,
            )
            yield batch

            if records_count == 0 or not has_more:
                break

        # Mark completed checkpoint
        final_wm = max_watermark_seen or win_end or now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        self.state_store.mark_completed(
            endpoint=self.config.name,
            final_watermark=final_wm,
            window_start=win_start,
            window_end=win_end,
        )

        logger.info(json.dumps({
            "event": "extraction_completed",
            "endpoint": self.config.name,
            "total_records": total_records,
            "total_pages": current_page,
            "final_watermark": final_wm,
            "requests_sent": self.client.requests_sent,
            "retry_count": self.client.retry_count
        }))
