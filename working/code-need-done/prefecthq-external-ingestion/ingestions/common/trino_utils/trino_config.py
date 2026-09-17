import os
from dataclasses import dataclass


def get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in {
        "true",
        "1",
        "yes",
        "y",
    }


def get_int(name: str, default: int) -> int:
    value = os.getenv(name)

    if value is None:
        return default

    return int(value)


def get_list(name: str) -> list[str]:
    value = os.getenv(name, "")

    return [
        item.strip()
        for item in value.split(",")
        if item.strip()
    ]


@dataclass(frozen=False)
class TrinoConfig:
    host: str
    port: int
    user: str
    catalog: str
    http_scheme: str
    password: str
    verify_ssl: bool


@dataclass(frozen=False)
class CloneConfig:
    batch_size: int
    tmp_dir: str
    parquet_schema_dir: str
    task_max_writer_count: int
    target_max_file_size: str


def get_src_config() -> TrinoConfig:
    return TrinoConfig(
        host=os.environ["TRINO_SRC_HOST"],
        port=get_int("TRINO_SRC_PORT", 443),
        user=os.environ["TRINO_SRC_USERNAME"],
        catalog=os.getenv("TRINO_SRC_CATALOG", "hive"),
        http_scheme=os.getenv(
            "TRINO_SRC_HTTP_SCHEME",
            "https",
        ),
        password=os.environ["TRINO_SRC_PASSWORD"],
        verify_ssl=get_bool(
            "TRINO_SRC_VERIFY_SSL",
            False,
        ),
    )


def get_dst_config() -> TrinoConfig:
    return TrinoConfig(
        host=os.environ["TRINO_DST_HOST"],
        port=get_int("TRINO_DST_PORT", 8443),
        user=os.environ["TRINO_DST_USERNAME"],
        catalog=os.getenv("TRINO_DST_CATALOG", "hive"),
        http_scheme=os.getenv(
            "TRINO_DST_HTTP_SCHEME",
            "https",
        ),
        password=os.environ["TRINO_DST_PASSWORD"],
        verify_ssl=get_bool(
            "TRINO_DST_VERIFY_SSL",
            False,
        ),
    )


def get_clone_config() -> CloneConfig:
    return CloneConfig(
        batch_size=get_int(
            "CLONE_BATCH_SIZE",
            50_000,
        ),
        tmp_dir=os.getenv(
            "CLONE_TMP_DIR",
            "./tmp/trino_parquet",
        ),
        parquet_schema_dir=os.getenv(
            "CLONE_PARQUET_SCHEMA_DIR",
            "./resources/parquet_schema",
        ),
        task_max_writer_count=get_int(
            "TRINO_TASK_MAX_WRITER_COUNT",
            1,
        ),
        target_max_file_size=os.getenv(
            "TRINO_TARGET_MAX_FILE_SIZE",
            "64MB",
        ),
    )