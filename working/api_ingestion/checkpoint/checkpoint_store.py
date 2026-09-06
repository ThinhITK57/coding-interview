import os
import json
import time
import logging
from datetime import datetime

from exceptions.errors import CheckpointError

logger = logging.getLogger(__name__)


class CheckpointStore:
    """
    Atomic file-based checkpoint for pipeline resume-from-crash.

    Deep module design:
        Interface (small): load() -> state dict, commit(state) -> None
        Implementation (deep): atomic write-then-rename, crash-safe,
        directory auto-creation, structured JSON logging, idempotency
        via batch_id tracking.

    Checkpoint file format (JSON):
        {
            "endpoint": "tasks",
            "batch_id": "20260905_030000_tasks",
            "last_offset": 2500,
            "total_records": 2500,
            "current_page": 50,
            "paginator_state": {...},
            "timestamp": "2026-09-05T03:14:23Z",
            "completed": false
        }
    """

    def __init__(self, checkpoint_path, endpoint_name):
        """Initialize checkpoint store.

        Args:
            checkpoint_path: Base directory for checkpoint files.
            endpoint_name: Name of the endpoint (used in filename).
        """
        self._base_path = checkpoint_path
        self._endpoint_name = endpoint_name
        self._file_path = os.path.join(
            checkpoint_path,
            f"{endpoint_name}.checkpoint.json"
        )

    def load(self, skip_completed=True):
        """Load checkpoint state from file.

        Args:
            skip_completed: If True, returns None if checkpoint is marked completed.
                If False, returns state regardless of completion status (useful for reading watermarks).

        Returns:
            dict: Checkpoint state, or None if no checkpoint exists.

        Raises:
            CheckpointError: If file exists but cannot be parsed.
        """
        if not os.path.exists(self._file_path):
            logger.info(json.dumps({
                "event": "checkpoint_not_found",
                "path": self._file_path,
                "endpoint": self._endpoint_name,
            }))
            return None

        try:
            with open(self._file_path, "r", encoding="utf-8") as f:
                state = json.load(f)

            # Skip completed checkpoints if requested (e.g. for offset resume)
            if skip_completed and state.get("completed", False):
                logger.info(json.dumps({
                    "event": "checkpoint_completed_skip",
                    "endpoint": self._endpoint_name,
                    "total_records": state.get("total_records"),
                }))
                return None

            logger.info(json.dumps({
                "event": "checkpoint_loaded",
                "endpoint": self._endpoint_name,
                "last_offset": state.get("last_offset"),
                "total_records": state.get("total_records"),
                "timestamp": state.get("timestamp"),
                "watermark": state.get("watermark"),
            }))
            return state

        except (json.JSONDecodeError, IOError) as e:
            raise CheckpointError(
                f"Failed to load checkpoint {self._file_path}: {e}"
            )

    def get_last_watermark(self):
        """Get the latest committed watermark regardless of completion status.

        Returns:
            str or None: The last committed watermark timestamp string, or None.
        """
        state = self.load(skip_completed=False)
        if state is None:
            return None
        return state.get("watermark") or state.get("max_timestamp_seen")

    def commit(self, state):
        """Atomically write checkpoint state to file.

        Uses write-to-temp-then-rename pattern for crash safety.
        If the process dies mid-write, the old checkpoint remains intact.

        Args:
            state: dict with checkpoint data to persist.

        Raises:
            CheckpointError: If write fails.
        """
        # Ensure directory exists
        os.makedirs(self._base_path, exist_ok=True)

        # Add metadata
        state["timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        state["endpoint"] = self._endpoint_name

        # Atomic write: temp file -> rename
        temp_path = self._file_path + ".tmp"
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())  # Force write to disk

            # Atomic rename (on same filesystem)
            if os.path.exists(self._file_path):
                os.replace(temp_path, self._file_path)
            else:
                os.rename(temp_path, self._file_path)

            logger.info(json.dumps({
                "event": "checkpoint_committed",
                "endpoint": self._endpoint_name,
                "total_records": state.get("total_records"),
                "last_offset": state.get("last_offset"),
            }))

        except IOError as e:
            # Clean up temp file on failure
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            raise CheckpointError(
                f"Failed to write checkpoint {self._file_path}: {e}"
            )

    def mark_completed(self, final_watermark=None, window_start=None, window_end=None):
        """Mark the current checkpoint as completed.

        Args:
            final_watermark: Optional high watermark timestamp string to persist.
            window_start: Optional window start timestamp string.
            window_end: Optional window end timestamp string.
        """
        state = self.load(skip_completed=False) or {}
        state["completed"] = True
        if final_watermark:
            state["watermark"] = final_watermark
        if window_start:
            state["window_start"] = window_start
        if window_end:
            state["window_end"] = window_end
        self.commit(state)

        logger.info(json.dumps({
            "event": "checkpoint_marked_completed",
            "endpoint": self._endpoint_name,
            "total_records": state.get("total_records"),
            "watermark": state.get("watermark"),
            "window_end": state.get("window_end"),
        }))

    def clear(self):
        """Delete checkpoint file to force fresh start."""
        if os.path.exists(self._file_path):
            os.remove(self._file_path)
            logger.info(json.dumps({
                "event": "checkpoint_cleared",
                "endpoint": self._endpoint_name,
            }))
