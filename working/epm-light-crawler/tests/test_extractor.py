import os
import tempfile
import pytest
from unittest.mock import MagicMock

from epm.monitoring.metrics import EndpointConfig, ExtractionMode, RateLimitConfig, RetryConfig, PaginationConfig
from epm.storage.state_store import StateStore, DailyQuotaExceededError
from epm.config.loader import ConfigLoader
from epm.ingestion.extractor import LightweightExtractor
from epm.client.resilient_client import ResilientHTTPClient


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    store = StateStore(db_path=db_path)
    yield store
    import gc
    gc.collect()
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
    except OSError:
        pass


def test_state_store_daily_quota(temp_db):
    date_str = "2026-09-18"
    # Initial count should be 0
    info = temp_db.get_daily_requests(date_str)
    assert info["request_count"] == 0

    # Increment 5
    new_count = temp_db.check_and_increment_daily_requests(max_limit=10, count=5, date_str=date_str)
    assert new_count == 5

    # Increment 5 more (reaches 10)
    new_count = temp_db.check_and_increment_daily_requests(max_limit=10, count=5, date_str=date_str)
    assert new_count == 10

    # Increment 1 more -> should raise DailyQuotaExceededError
    with pytest.raises(DailyQuotaExceededError):
        temp_db.check_and_increment_daily_requests(max_limit=10, count=1, date_str=date_str)

    # Next day should be independent
    next_day = "2026-09-19"
    c2 = temp_db.check_and_increment_daily_requests(max_limit=10, count=2, date_str=next_day)
    assert c2 == 2


def test_state_store_checkpoints(temp_db):
    endpoint = "tasks"
    assert temp_db.get_checkpoint(endpoint) is None

    temp_db.save_checkpoint(
        endpoint=endpoint,
        last_watermark="2026-09-18T00:00:00Z",
        last_offset=250,
        last_page=1,
        total_records=250,
        completed=False,
    )
    cp = temp_db.get_checkpoint(endpoint)
    assert cp["endpoint"] == endpoint
    assert cp["last_offset"] == 250
    assert cp["completed"] is False

    temp_db.mark_completed(
        endpoint=endpoint,
        final_watermark="2026-09-18T02:00:00Z",
    )
    cp2 = temp_db.get_checkpoint(endpoint)
    assert cp2["completed"] is True
    assert cp2["last_watermark"] == "2026-09-18T02:00:00Z"
    assert cp2["last_offset"] == 0


def test_state_store_manifest_and_audit(temp_db):
    temp_db.record_pending_buffer(
        batch_id="batch_01",
        endpoint="tasks",
        local_path="./data/buffer/tasks/batch_01.json.gz",
        intended_s3_key="lakehouse/bronze/clarizen/tasks/2026/09/18/batch_01.json.gz",
        record_count=100,
    )
    pending = temp_db.list_pending_buffers()
    assert len(pending) == 1
    assert pending[0]["batch_id"] == "batch_01"
    assert pending[0]["status"] == "PENDING"

    temp_db.commit_buffer_upload("batch_01")
    assert len(temp_db.list_pending_buffers()) == 0

    # Audit runs
    temp_db.record_audit_run({
        "batch_id": "batch_01",
        "endpoint_name": "tasks",
        "mode": "incremental",
        "start_time": "2026-09-18T00:00:00Z",
        "end_time": "2026-09-18T00:01:00Z",
        "duration_seconds": 60.0,
        "records_extracted": 100,
        "requests_sent": 1,
        "retry_count": 0,
        "watermark_start": "2026-09-17T00:00:00Z",
        "watermark_end": "2026-09-18T00:00:00Z",
        "status": "SUCCESS",
    })
    audits = temp_db.list_recent_audit_runs()
    assert len(audits) == 1
    assert audits[0]["status"] == "SUCCESS"


def test_config_loader():
    loader = ConfigLoader("config.json")
    endpoints = loader.load()
    assert "tasks" in endpoints
    assert "projects" in endpoints
    assert "targets" in endpoints
    assert "assignments" in endpoints
    assert "objectives" in endpoints

    tasks = endpoints["tasks"]
    assert tasks.pagination.page_size == 250
    assert tasks.rate_limit.requests_per_minute > 0
    assert tasks.rate_limit.requests_per_day > 0


def test_extractor_window_and_pagination(temp_db):
    endpoint = EndpointConfig(
        name="tasks",
        path="/v2.0/services/data/entityQuery",
        method="POST",
        pagination=PaginationConfig(page_size=2, offset_param="from", limit_param="limit"),
        body={
            "typeName": "Task",
            "where": {
                "and": [
                    {
                        "leftExpression": {"fieldName": "LastUpdatedOn"},
                        "operator": "GreaterThanOrEqual",
                        "rightExpression": {"value": "__WINDOW_START__"}
                    }
                ]
            }
        }
    )

    mock_client = MagicMock(spec=ResilientHTTPClient)
    # Page 1: 2 records, hasMore = True
    page1 = {
        "entities": [
            {"id": "task_1", "LastUpdatedOn": "2026-09-18T01:00:00Z"},
            {"id": "task_2", "LastUpdatedOn": "2026-09-18T01:30:00Z"},
        ],
        "paging": {"hasMore": True}
    }
    # Page 2: 1 record, hasMore = False
    page2 = {
        "entities": [
            {"id": "task_3", "LastUpdatedOn": "2026-09-18T02:00:00Z"},
        ],
        "paging": {"hasMore": False}
    }
    mock_client.request.side_effect = [page1, page2]
    mock_client.requests_sent = 2
    mock_client.retry_count = 0

    extractor = LightweightExtractor(
        endpoint_config=endpoint,
        state_store=temp_db,
        client=mock_client,
        watermark_field="LastUpdatedOn"
    )

    batches = list(extractor.extract_batches(
        mode=ExtractionMode.INCREMENTAL.value,
        window_start="2026-09-18T00:00:00Z",
        window_end="2026-09-18T03:00:00Z"
    ))

    assert len(batches) == 2
    assert batches[0].record_count == 2
    assert batches[1].record_count == 1

    # Checkpoint should now be completed
    cp = temp_db.get_checkpoint("tasks")
    assert cp["completed"] is True
    assert cp["last_watermark"] == "2026-09-18T02:00:00Z"


def test_extractor_full_mode_removes_watermark_filter(temp_db):
    endpoint = EndpointConfig(
        name="projects",
        path="/v2.0/services/data/entityQuery",
        body={
            "typeName": "Project",
            "where": {
                "and": [
                    {
                        "leftExpression": {"fieldName": "LastUpdatedOn"},
                        "operator": "GreaterThanOrEqual",
                        "rightExpression": {"value": "__WINDOW_START__"}
                    }
                ]
            }
        }
    )
    mock_client = MagicMock(spec=ResilientHTTPClient)
    mock_client.request.return_value = {
        "entities": [{"id": "p1"}],
        "paging": {"hasMore": False}
    }
    mock_client.requests_sent = 1
    mock_client.retry_count = 0

    extractor = LightweightExtractor(endpoint_config=endpoint, state_store=temp_db, client=mock_client)
    batches = list(extractor.extract_batches(mode=ExtractionMode.FULL.value))
    assert len(batches) == 1

    # Check the call payload to mock_client: where filter should have been removed
    called_body = mock_client.request.call_args[1]["json_body"]
    assert "where" not in called_body
