import os
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class StateStoreError(Exception):
    """Base exception for StateStore operations."""
    pass


class DailyQuotaExceededError(StateStoreError):
    """Raised when daily API request quota is exhausted."""
    pass


class StateStore:
    """Persistent SQLite-backed operational state store.
    Manages:
    1. Persistent daily request quota tracking across independent runs.
    2. Endpoint checkpoints (watermark & pagination state).
    3. Pending local buffer uploads (Iceberg-style manifest log).
    4. Operational audit logs for crawler runs.
    """

    def __init__(self, db_path: str = "./state/state.db"):
        self.db_path = os.path.abspath(db_path)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
            cur = conn.cursor()
            # 1. Daily Quota Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_request_quota (
                    date_str TEXT PRIMARY KEY,
                    request_count INTEGER NOT NULL DEFAULT 0,
                    max_limit INTEGER NOT NULL DEFAULT 1000,
                    updated_at TEXT NOT NULL
                )
            """)
            # 2. Checkpoints Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS checkpoints (
                    endpoint TEXT PRIMARY KEY,
                    last_watermark TEXT,
                    last_offset INTEGER DEFAULT 0,
                    last_page INTEGER DEFAULT 0,
                    total_records INTEGER DEFAULT 0,
                    window_start TEXT,
                    window_end TEXT,
                    completed INTEGER DEFAULT 1,
                    updated_at TEXT NOT NULL
                )
            """)
            # 3. Buffer Manifest Queue (Iceberg-style decoupled backfill)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS manifest_pending_uploads (
                    batch_id TEXT PRIMARY KEY,
                    endpoint TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    intended_s3_key TEXT NOT NULL,
                    record_count INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    created_at TEXT NOT NULL,
                    committed_at TEXT
                )
            """)
            # 4. Crawler Audit Run Log
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crawler_audit_runs (
                    batch_id TEXT PRIMARY KEY,
                    endpoint_name TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    duration_seconds REAL DEFAULT 0.0,
                    records_extracted INTEGER DEFAULT 0,
                    requests_sent INTEGER DEFAULT 0,
                    retry_count INTEGER DEFAULT 0,
                    watermark_start TEXT,
                    watermark_end TEXT,
                    status TEXT NOT NULL,
                    error_message TEXT
                )
            """)
            conn.commit()

    # --------------------------------------------------------------------------
    # DAILY REQUEST QUOTA MANAGEMENT
    # --------------------------------------------------------------------------
    def get_daily_requests(self, date_str: Optional[str] = None) -> Dict[str, Any]:
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM daily_request_quota WHERE date_str = ?", (date_str,))
            row = cur.fetchone()
            if row:
                return dict(row)
            return {"date_str": date_str, "request_count": 0, "max_limit": 1000, "updated_at": ""}

    def check_and_increment_daily_requests(
        self, max_limit: int = 1000, count: int = 1, date_str: Optional[str] = None
    ) -> int:
        """Atomically checks and increments the daily request quota.
        Raises DailyQuotaExceededError if adding count exceeds max_limit.
        """
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        now_ts = datetime.now(timezone.utc).isoformat()

        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            cur.execute("SELECT request_count FROM daily_request_quota WHERE date_str = ?", (date_str,))
            row = cur.fetchone()

            current = row["request_count"] if row else 0
            if current + count > max_limit:
                conn.rollback()
                raise DailyQuotaExceededError(
                    f"Daily API request quota exceeded for {date_str}: "
                    f"Current={current}, Attempted={count}, Limit={max_limit}"
                )

            new_count = current + count
            cur.execute("""
                INSERT INTO daily_request_quota (date_str, request_count, max_limit, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(date_str) DO UPDATE SET
                    request_count = ?,
                    max_limit = ?,
                    updated_at = ?
            """, (date_str, new_count, max_limit, now_ts, new_count, max_limit, now_ts))
            conn.commit()
            return new_count

    # --------------------------------------------------------------------------
    # CHECKPOINT MANAGEMENT
    # --------------------------------------------------------------------------
    def get_checkpoint(self, endpoint: str) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM checkpoints WHERE endpoint = ?", (endpoint,))
            row = cur.fetchone()
            if row:
                d = dict(row)
                d["completed"] = bool(d["completed"])
                return d
            return None

    def save_checkpoint(
        self,
        endpoint: str,
        last_watermark: Optional[str] = None,
        last_offset: int = 0,
        last_page: int = 0,
        total_records: int = 0,
        window_start: Optional[str] = None,
        window_end: Optional[str] = None,
        completed: bool = False,
    ):
        now_ts = datetime.now(timezone.utc).isoformat()
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO checkpoints (
                    endpoint, last_watermark, last_offset, last_page,
                    total_records, window_start, window_end, completed, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(endpoint) DO UPDATE SET
                    last_watermark = COALESCE(?, checkpoints.last_watermark),
                    last_offset = ?,
                    last_page = ?,
                    total_records = ?,
                    window_start = ?,
                    window_end = ?,
                    completed = ?,
                    updated_at = ?
            """, (
                endpoint, last_watermark, last_offset, last_page,
                total_records, window_start, window_end, int(completed), now_ts,
                last_watermark, last_offset, last_page,
                total_records, window_start, window_end, int(completed), now_ts
            ))
            conn.commit()

    def mark_completed(
        self,
        endpoint: str,
        final_watermark: Optional[str],
        window_start: Optional[str] = None,
        window_end: Optional[str] = None,
    ):
        self.save_checkpoint(
            endpoint=endpoint,
            last_watermark=final_watermark,
            last_offset=0,
            last_page=0,
            window_start=window_start,
            window_end=window_end,
            completed=True,
        )

    # --------------------------------------------------------------------------
    # BUFFER MANIFEST QUEUE (Iceberg-style decoupling)
    # --------------------------------------------------------------------------
    def record_pending_buffer(
        self,
        batch_id: str,
        endpoint: str,
        local_path: str,
        intended_s3_key: str,
        record_count: int,
    ):
        now_ts = datetime.now(timezone.utc).isoformat()
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO manifest_pending_uploads (
                    batch_id, endpoint, local_path, intended_s3_key,
                    record_count, status, created_at
                ) VALUES (?, ?, ?, ?, ?, 'PENDING', ?)
                ON CONFLICT(batch_id) DO UPDATE SET
                    status = 'PENDING',
                    created_at = ?
            """, (batch_id, endpoint, local_path, intended_s3_key, record_count, now_ts, now_ts))
            conn.commit()

    def list_pending_buffers(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM manifest_pending_uploads
                WHERE status = 'PENDING'
                ORDER BY created_at ASC
                LIMIT ?
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]

    def commit_buffer_upload(self, batch_id: str):
        now_ts = datetime.now(timezone.utc).isoformat()
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE manifest_pending_uploads
                SET status = 'COMMITTED', committed_at = ?
                WHERE batch_id = ?
            """, (now_ts, batch_id))
            conn.commit()

    # --------------------------------------------------------------------------
    # AUDIT TRACKING
    # --------------------------------------------------------------------------
    def record_audit_run(self, metrics: Dict[str, Any]):
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO crawler_audit_runs (
                    batch_id, endpoint_name, mode, start_time, end_time,
                    duration_seconds, records_extracted, requests_sent,
                    retry_count, watermark_start, watermark_end, status, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(batch_id) DO UPDATE SET
                    end_time = ?,
                    duration_seconds = ?,
                    records_extracted = ?,
                    requests_sent = ?,
                    retry_count = ?,
                    status = ?,
                    error_message = ?
            """, (
                metrics["batch_id"], metrics["endpoint_name"], metrics["mode"],
                metrics["start_time"], metrics.get("end_time"), metrics.get("duration_seconds", 0.0),
                metrics.get("records_extracted", 0), metrics.get("requests_sent", 0),
                metrics.get("retry_count", 0), metrics.get("watermark_start"), metrics.get("watermark_end"),
                metrics["status"], metrics.get("error_message"),
                metrics.get("end_time"), metrics.get("duration_seconds", 0.0),
                metrics.get("records_extracted", 0), metrics.get("requests_sent", 0),
                metrics.get("retry_count", 0), metrics["status"], metrics.get("error_message")
            ))
            conn.commit()

    def list_recent_audit_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM crawler_audit_runs
                ORDER BY start_time DESC
                LIMIT ?
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]
