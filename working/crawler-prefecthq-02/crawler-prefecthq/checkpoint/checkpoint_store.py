import os
import json
import time
import logging
from datetime import datetime

from exceptions.errors import CheckpointError

logger = logging.getLogger(__name__)


class CheckpointStore:
    def __init__(self, checkpoint_path, endpoint_name):
        self._base_path = checkpoint_path
        self._endpoint_name = endpoint_name
        self._file_path = os.path.join(checkpoint_path, f"{endpoint_name}.checkpoint.json")

    def load(self, skip_completed=True):
        if not os.path.exists(self._file_path):
            logger.info(json.dumps(
                {
                    "event": "checkpoint_not_found",
                    "path": self._file_path,
                    "endpoint": self._endpoint_name
                }
            ))
            return None
        try:
            with open(self._file_path, "r", encoding="utf-8") as f:
                state = json.load(f)

            if skip_completed and state.get("completed", False):
                logger.info(json.dumps({
                    "event": "checkpoint_completed_skip",
                    "endpoint": self._endpoint_name,
                    "total_records": state.get("total_records")
                }))
                return None

            logger.info(json.dumps(
                {
                    "event": "checkpoint_loaded",
                    "endpoint": self._endpoint_name,
                    "last_offset": state.get("last_offset"),
                    "total_records": state.get("total_records"),
                    "timestamp": state.get("timestamp"),
                    "water_mark": state.get("watermark")
                }

            ))
            return state

        except (json.JSONDecodeError, IOError) as e:
            raise CheckpointError(
                f"Failed to load checkpoint from {self._file_path}: {str(e)}"
            )

    def get_last_watermark(self):
        state = self.load(skip_completed=False)
        if state is None:
            return None
        return state.get("watermark") or state.get("max_timestamp_seen")

    def commit(self, state):
        os.makedirs(self._base_path, exist_ok=True)
        state["timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        state["endpoint"] = self._endpoint_name

        temp_path = self._file_path + ".tmp"
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())

            if os.path.exists(self._file_path):
                os.replace(temp_path, self._file_path)
            else:
                os.rename(temp_path, self._file_path)

            logger.info(json.dumps({
                "event": "checkpoint_committed",
                "endpoint": self._endpoint_name,
                "total_records": state.get("total_records"),
                "last_offset": state.get("last_offset")
            }))

        except IOError as e:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            raise CheckpointError(
                f"Failed to write checkpoint {self._file_path}: {e}"
            )

    def mark_completed(self, final_watermark=None, window_start=None, window_end=None):
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
            "window_end": state.get("window_end")
        }))

    def clear(self):
        if os.path.exists(self._file_path):
            os.remove(self._file_path)
            logger.info(json.dumps({
                "event": "checkpoint_cleared",
                "endpoint": self._endpoint_name
            }))