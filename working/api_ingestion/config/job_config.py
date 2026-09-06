from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class ExtractionMode(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"


class StorageFormat(str, Enum):
    JSON = "json"
    JSONL = "jsonl"
    PARQUET = "parquet"


@dataclass
class IncrementalConfig:
    watermark_field: str = "LastModified"
    lookback_minutes: int = 15
    datetime_format: str = "%Y-%m-%dT%H:%M:%SZ"
    initial_start_time: Optional[str] = None


@dataclass
class ExtractionConfig:
    mode: ExtractionMode = ExtractionMode.FULL

    batch_size: int = 1000

    max_records: Optional[int] = None
    max_pages: Optional[int] = None
    max_runtime_seconds: Optional[int] = None

    window_start: Optional[str] = None
    window_end: Optional[str] = None
    incremental: IncrementalConfig = field(
        default_factory=IncrementalConfig
    )


@dataclass
class CheckpointConfig:
    enabled: bool = True

    store_type: str = "file"

    path: Optional[str] = None


@dataclass
class StorageConfig:
    format: StorageFormat = StorageFormat.PARQUET

    layer: str = "bronze"

    path: str = ""

    partition_columns: List[str] = field(
        default_factory=list
    )

    compression: str = "snappy"


@dataclass
class JobConfig:
    job_name: str
    source: str
    endpoint: str
    api_version: str

    extraction: ExtractionConfig = field(
        default_factory=ExtractionConfig
    )

    checkpoint: CheckpointConfig = field(
        default_factory=CheckpointConfig
    )

    storage: StorageConfig = field(
        default_factory=StorageConfig
    )