import os
import logging
import json
from typing import Any, Dict, List, Optional

from epm.storage.minio_sink import MinIOSink
from epm.storage.state_store import StateStore

logger = logging.getLogger(__name__)


class BackgroundBackfillWorker:
    """Asynchronous, decoupled background backfill worker.
    Mirrors Apache Iceberg's decoupled commit pattern:
    1. Periodically / on-demand scans manifest_pending_uploads for status='PENDING' (FIFO order).
    2. Probes MinIO connectivity to ensure storage is available.
    3. Streams buffered .json.gz objects to their intended MinIO keys.
    4. Verifies object integrity on MinIO (size verification).
    5. Atomically transitions manifest status to 'COMMITTED'.
    6. Safely cleans up the local buffer file.
    Does NOT block or delay regular real-time / incremental crawls.
    """

    def __init__(
        self,
        sink: Optional[MinIOSink] = None,
        state_store: Optional[StateStore] = None,
        batch_limit: int = 50,
    ):
        self.state_store = state_store or StateStore()
        self.sink = sink or MinIOSink(state_store=self.state_store)
        self.batch_limit = batch_limit

    def run_backfill(self) -> Dict[str, Any]:
        """Executes a backfill pass over pending manifest items.
        Returns summary metrics.
        """
        logger.info("Starting background backfill check...")

        # 1. Probe MinIO connectivity
        if not self.sink.is_connected():
            logger.warning("MinIO is currently unreachable. Backfill pass skipped.")
            return {
                "status": "SKIPPED",
                "reason": "MinIO unreachable",
                "processed_count": 0,
                "committed_count": 0,
                "failed_count": 0,
            }

        # 2. Fetch pending items FIFO
        pending_items = self.state_store.list_pending_buffers(limit=self.batch_limit)
        if not pending_items:
            logger.info("No pending fallback buffers found in manifest.")
            return {
                "status": "NOOP",
                "processed_count": 0,
                "committed_count": 0,
                "failed_count": 0,
            }

        logger.info(f"Found {len(pending_items)} pending buffer files to backfill.")

        committed_count = 0
        failed_count = 0
        total_bytes = 0
        errors = []

        for item in pending_items:
            batch_id = item["batch_id"]
            endpoint = item["endpoint"]
            local_path = item["local_path"]
            intended_s3_key = item["intended_s3_key"]
            record_count = item["record_count"]

            if not os.path.exists(local_path):
                err_msg = f"Local buffer file missing: {local_path} for batch {batch_id}"
                logger.error(err_msg)
                errors.append({"batch_id": batch_id, "error": err_msg})
                failed_count += 1
                continue

            local_file_size = os.path.getsize(local_path)

            try:
                # 3. Upload to MinIO
                with open(local_path, "rb") as f:
                    self.sink.client.put_object(
                        bucket_name=self.sink.bucket_name,
                        object_name=intended_s3_key,
                        data=f,
                        length=local_file_size,
                        content_type="application/gzip",
                    )

                # 4. Verify object size in MinIO
                stat = self.sink.client.stat_object(
                    bucket_name=self.sink.bucket_name,
                    object_name=intended_s3_key,
                )
                if stat.size != local_file_size:
                    raise IOError(
                        f"Size mismatch after upload for {intended_s3_key}: "
                        f"expected {local_file_size} bytes, got {stat.size} bytes"
                    )

                # 5. Atomically commit manifest
                self.state_store.commit_buffer_upload(batch_id)

                # 6. Safely remove local file
                os.remove(local_path)

                committed_count += 1
                total_bytes += local_file_size

                logger.info(json.dumps({
                    "event": "backfill_item_committed",
                    "batch_id": batch_id,
                    "endpoint": endpoint,
                    "s3_key": intended_s3_key,
                    "record_count": record_count,
                    "bytes": local_file_size,
                }))

            except Exception as e:
                logger.error(f"Failed to backfill batch {batch_id} to {intended_s3_key}: {e}")
                errors.append({"batch_id": batch_id, "error": str(e)})
                failed_count += 1

        summary = {
            "status": "COMPLETED" if failed_count == 0 else "PARTIAL",
            "processed_count": len(pending_items),
            "committed_count": committed_count,
            "failed_count": failed_count,
            "total_bytes": total_bytes,
            "errors": errors,
        }
        logger.info(json.dumps({"event": "backfill_completed", **summary}))
        return summary
