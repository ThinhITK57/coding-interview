import os
import gzip
import json
import tempfile
import pytest
from unittest.mock import MagicMock, patch

from epm.monitoring.metrics import Batch, CrawlRunMetrics
from epm.storage.state_store import StateStore
from epm.storage.minio_sink import MinIOSink
from epm.backfill.backfill_worker import BackgroundBackfillWorker
from epm.monitoring.audit_tracker import AuditTracker


@pytest.fixture
def temp_env():
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
        db_path = os.path.join(temp_dir, "test_state.db")
        buffer_dir = os.path.join(temp_dir, "buffer")
        audit_dir = os.path.join(temp_dir, "audit")
        store = StateStore(db_path=db_path)
        yield {
            "store": store,
            "temp_dir": temp_dir,
            "buffer_dir": buffer_dir,
            "audit_dir": audit_dir,
        }


def test_sink_compress_and_local_fallback(temp_env):
    store = temp_env["store"]
    buffer_dir = temp_env["buffer_dir"]

    # Mock MinIO client that raises an exception to trigger local fallback
    sink = MinIOSink(
        buffer_dir=buffer_dir,
        state_store=store,
    )
    mock_minio = MagicMock()
    mock_minio.bucket_exists.side_effect = Exception("MinIO Connection Refused")
    sink._client = mock_minio

    batch = Batch(
        records=[{"id": "task_1", "name": "Implement ETL"}, {"id": "task_2", "name": "Review PR"}],
        batch_id="20260918_100000_tasks",
        page_number=1,
        record_count=2,
        endpoint_name="tasks",
    )

    result = sink.write_batch(batch)

    # Verify fallback result
    assert result["status"] == "BUFFERED"
    assert result["storage"] == "LOCAL"
    assert os.path.exists(result["local_path"])

    # Verify compressed content
    with gzip.open(result["local_path"], "rt", encoding="utf-8") as gz:
        lines = [json.loads(line) for line in gz if line.strip()]
        assert len(lines) == 2
        assert lines[0]["id"] == "task_1"
        assert lines[1]["id"] == "task_2"

    # Verify SQLite manifest entry
    pending = store.list_pending_buffers()
    assert len(pending) == 1
    assert pending[0]["batch_id"] == "20260918_100000_tasks"
    assert pending[0]["status"] == "PENDING"
    assert pending[0]["record_count"] == 2


def test_sink_direct_minio_upload(temp_env):
    store = temp_env["store"]
    buffer_dir = temp_env["buffer_dir"]

    sink = MinIOSink(
        buffer_dir=buffer_dir,
        state_store=store,
    )
    mock_minio = MagicMock()
    mock_minio.bucket_exists.return_value = True
    sink._client = mock_minio

    batch = Batch(
        records=[{"id": "p1", "name": "Project Alpha"}],
        batch_id="20260918_100000_projects",
        page_number=1,
        record_count=1,
        endpoint_name="projects",
    )

    result = sink.write_batch(batch)
    assert result["status"] == "UPLOADED"
    assert result["storage"] == "MINIO"
    assert mock_minio.put_object.called

    # No pending manifest should exist
    assert len(store.list_pending_buffers()) == 0


def test_background_backfill_worker(temp_env):
    store = temp_env["store"]
    buffer_dir = temp_env["buffer_dir"]

    sink = MinIOSink(buffer_dir=buffer_dir, state_store=store)
    mock_minio = MagicMock()
    mock_minio.bucket_exists.return_value = True
    sink._client = mock_minio

    # Simulate 1 buffered file
    os.makedirs(os.path.join(buffer_dir, "tasks"), exist_ok=True)
    local_file = os.path.join(buffer_dir, "tasks", "test_batch.json.gz")
    with gzip.open(local_file, "wt", encoding="utf-8") as gz:
        gz.write(json.dumps({"id": "task_backfill"}) + "\n")

    store.record_pending_buffer(
        batch_id="test_batch",
        endpoint="tasks",
        local_path=local_file,
        intended_s3_key="bronze/clarizen/tasks/year=2026/month=09/day=18/test_batch.json.gz",
        record_count=1,
    )

    assert len(store.list_pending_buffers()) == 1

    # Configure stat_object mock to match local file size
    stat_mock = MagicMock()
    stat_mock.size = os.path.getsize(local_file)
    mock_minio.stat_object.return_value = stat_mock

    worker = BackgroundBackfillWorker(sink=sink, state_store=store)
    summary = worker.run_backfill()

    assert summary["status"] == "COMPLETED"
    assert summary["committed_count"] == 1
    assert summary["failed_count"] == 0

    # Local file should have been safely deleted
    assert not os.path.exists(local_file)

    # Manifest should be committed
    assert len(store.list_pending_buffers()) == 0


def test_audit_tracker(temp_env):
    store = temp_env["store"]
    audit_dir = temp_env["audit_dir"]

    sink = MinIOSink(state_store=store)
    mock_minio = MagicMock()
    mock_minio.bucket_exists.return_value = True
    sink._client = mock_minio

    tracker = AuditTracker(state_store=store, sink=sink, local_audit_dir=audit_dir)

    metrics = CrawlRunMetrics(
        batch_id="batch_audit_01",
        endpoint_name="tasks",
        mode="incremental",
        start_time="2026-09-18T00:00:00Z",
        end_time="2026-09-18T00:01:00Z",
        duration_seconds=60.0,
        records_extracted=500,
        requests_sent=2,
        retry_count=1,
        watermark_start="2026-09-17T00:00:00Z",
        watermark_end="2026-09-18T00:00:00Z",
        status="SUCCESS",
    )

    tracker.log_run(metrics)

    # Verify SQLite record
    recent = tracker.get_recent_runs()
    assert len(recent) == 1
    assert recent[0]["batch_id"] == "batch_audit_01"
    assert recent[0]["records_extracted"] == 500

    # Verify sync to MinIO
    sync_res = tracker.sync_daily_audit_to_minio()
    assert sync_res["status"] == "SYNCED"
    assert mock_minio.put_object.called

    # Health check
    health = tracker.get_system_health()
    assert health["minio_reachable"] is True
    assert health["pending_fallback_buffers"] == 0
    assert len(health["recent_runs"]) == 1
