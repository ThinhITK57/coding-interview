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

    def load(self):
        """Load checkpoint state from file.

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

            # Skip completed checkpoints
            if state.get("completed", False):
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
            }))
            return state

        except (json.JSONDecodeError, IOError) as e:
            raise CheckpointError(
                f"Failed to load checkpoint {self._file_path}: {e}"
            )

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

    def mark_completed(self):
        """Mark the current checkpoint as completed.

        Called when extraction for this endpoint finishes successfully.
        Next run will skip this checkpoint and start fresh.
        """
        state = self.load()
        if state is None:
            state = {}
        state["completed"] = True
        self.commit(state)

        logger.info(json.dumps({
            "event": "checkpoint_marked_completed",
            "endpoint": self._endpoint_name,
            "total_records": state.get("total_records"),
        }))

    def clear(self):
        """Delete checkpoint file to force fresh start."""
        if os.path.exists(self._file_path):
            os.remove(self._file_path)
            logger.info(json.dumps({
                "event": "checkpoint_cleared",
                "endpoint": self._endpoint_name,
            }))
