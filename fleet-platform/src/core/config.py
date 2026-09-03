"""
Fleet Data Platform — Centralized Configuration Loader
========================================================
Mô-đun trung tâm quản trị toàn bộ tham số hệ thống.
Đọc cấu hình theo 3 tầng ưu tiên (từ thấp đến cao):
  1. configs/base.yaml          — Giá trị mặc định chung
  2. configs/{APP_ENV}.yaml     — Giá trị ghi đè theo môi trường
  3. Environment Variables (OS) — Ghi đè cuối cùng (ưu tiên cao nhất)

Usage:
  # Trong code Python:
  from src.core.config import load_config
  cfg = load_config()                      # Đọc APP_ENV từ OS (mặc định "local")
  cfg = load_config(env="prod")            # Ép buộc môi trường Production

  # Từ Terminal:
  APP_ENV=prod python -m src.jobs.scd2_customer_job
  APP_ENV=local python -m src.jobs.batch_dwh_job --granularity monthly
"""

import os
import re
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, Dict, List


# =============================================================================
# 1. DATACLASSES CHO TỪNG NHÓM CẤU HÌNH (Typed & Validated)
# =============================================================================

@dataclass
class StorageConfig:
    type: str = "local"                    # "hdfs" | "s3" | "local"
    endpoint: str = ""
    access_key: str = ""
    secret_key: str = ""
    warehouse_path: str = "./data/warehouse"


@dataclass
class DatabaseConfig:
    host: str = "localhost"
    port: int = 5432
    name: str = "fleet_oltp"
    user: str = "fleet_app"
    password: str = ""
    jdbc_url: str = ""

    def get_jdbc_url(self) -> str:
        """Tạo JDBC URL từ các trường nếu jdbc_url chưa được set."""
        if self.jdbc_url:
            return self.jdbc_url
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.name}"

    def get_connection_properties(self) -> Dict[str, str]:
        """Trả về dict connection properties chuẩn cho Spark JDBC."""
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }


@dataclass
class HiveConfig:
    enabled: bool = False
    metastore_uris: str = ""


@dataclass
class KafkaConfig:
    bootstrap_servers: str = "localhost:9092"
    consumer_group: str = "fleet-etl-consumer"
    starting_offsets: str = "latest"
    topics: Dict[str, str] = field(default_factory=lambda: {
        "telemetry": "truck-telemetry",
        "repair_request": "repair-request",
        "cdc_prefix": "fleet.public",
    })


@dataclass
class RedisConfig:
    host: str = "localhost"
    port: int = 6379
    password: str = ""


@dataclass
class SparkConfig:
    master: str = "local[2]"
    app_name: str = "FleetPlatform"
    driver_memory: str = "1g"
    executor_memory: str = "1g"
    executor_cores: int = 2
    compression_codec: str = "snappy"
    adaptive_enabled: bool = True
    adaptive_skew_join: bool = True
    serializer: str = "org.apache.spark.serializer.KryoSerializer"
    shuffle_partitions: int = 8
    salt_buckets: int = 10
    event_log_enabled: bool = False
    event_log_dir: str = ""


@dataclass
class EtlConfig:
    batch_size: int = 10000
    retry_count: int = 3
    retry_delay_seconds: int = 30
    scd2_expiration_date: str = "9999-12-31"
    partition_overwrite_mode: str = "dynamic"


@dataclass
class StreamingConfig:
    trigger_interval: str = "10 seconds"
    watermark_delay: str = "5 minutes"
    no_data_micro_batches: bool = False


@dataclass
class DatalakeConfig:
    root: str = "/fleet-datalake"
    raw: str = "/fleet-datalake/raw"
    silver: str = "/fleet-datalake/silver"
    gold: str = "/fleet-datalake/warehouse"
    checkpoints: str = "/fleet-datalake/checkpoints"


@dataclass
class MonitoringConfig:
    prometheus_pushgateway: str = ""
    grafana_url: str = ""
    alerting_enabled: bool = False
    alert_webhook: str = ""


@dataclass
class AppConfig:
    """Cấu hình tổng thể toàn bộ ứng dụng Fleet Data Platform."""
    env: str = "local"
    app_name: str = "fleet-data-platform"
    version: str = "1.0.0"
    log_level: str = "INFO"

    storage: StorageConfig = field(default_factory=StorageConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    hive: HiveConfig = field(default_factory=HiveConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    etl: EtlConfig = field(default_factory=EtlConfig)
    streaming: StreamingConfig = field(default_factory=StreamingConfig)
    datalake: DatalakeConfig = field(default_factory=DatalakeConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)


# =============================================================================
# 2. LOGIC PHÂN GIẢI BIẾN MÔI TRƯỜNG TRONG YAML
# =============================================================================

_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)(?::([^}]*))?\}")


def _resolve_env_vars(value):
    """
    Thay thế ${VAR_NAME} hoặc ${VAR_NAME:default_value} bằng giá trị
    thực từ biến môi trường OS. Nếu biến không tồn tại và không có
    default thì giữ nguyên chuỗi rỗng.

    Ví dụ:
      "${FLEET_DB_PASSWORD}"           → os.environ["FLEET_DB_PASSWORD"]
      "${REDIS_PASSWORD:}"             → "" (default rỗng)
      "${ALERT_WEBHOOK_URL:https://}"  → "https://" (nếu chưa set)
    """
    if not isinstance(value, str):
        return value

    def _replacer(match):
        var_name = match.group(1)
        default_val = match.group(2) if match.group(2) is not None else ""
        return os.environ.get(var_name, default_val)

    return _ENV_VAR_PATTERN.sub(_replacer, value)


def _resolve_dict(data: dict) -> dict:
    """Đệ quy phân giải tất cả giá trị ${...} trong dictionary."""
    resolved = {}
    for key, value in data.items():
        if isinstance(value, dict):
            resolved[key] = _resolve_dict(value)
        elif isinstance(value, list):
            resolved[key] = [_resolve_env_vars(v) if isinstance(v, str) else v for v in value]
        else:
            resolved[key] = _resolve_env_vars(value)
    return resolved


# =============================================================================
# 3. HÀM NẠP CẤU HÌNH CHÍNH (PUBLIC API)
# =============================================================================

def _find_configs_dir() -> Path:
    """Tìm thư mục configs/ bằng cách duyệt lên từ thư mục hiện tại."""
    # Ưu tiên 1: Biến môi trường chỉ định
    env_path = os.environ.get("FLEET_CONFIGS_DIR")
    if env_path:
        p = Path(env_path)
        if p.is_dir():
            return p

    # Ưu tiên 2: Duyệt lên từ thư mục chứa file config.py này
    current = Path(__file__).resolve().parent
    for _ in range(5):  # Duyệt tối đa 5 cấp
        candidate = current / "configs"
        if candidate.is_dir():
            return candidate
        current = current.parent

    # Ưu tiên 3: Duyệt lên từ thư mục làm việc hiện tại (CWD)
    current = Path.cwd()
    for _ in range(5):
        candidate = current / "configs"
        if candidate.is_dir():
            return candidate
        current = current.parent

    raise FileNotFoundError(
        "Không tìm thấy thư mục configs/. "
        "Hãy set biến môi trường FLEET_CONFIGS_DIR hoặc chạy từ thư mục gốc dự án."
    )


def _deep_merge(base: dict, override: dict) -> dict:
    """Deep merge 2 dicts — override ghi đè base ở cấp lá."""
    merged = base.copy()
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _dict_to_dataclass(data: dict, cls, prefix: str = "") -> object:
    """Chuyển đổi dict thành dataclass, bỏ qua các key không phải trường của class."""
    import dataclasses
    if not dataclasses.is_dataclass(cls):
        return data

    field_names = {f.name for f in dataclasses.fields(cls)}
    kwargs = {}
    for f in dataclasses.fields(cls):
        if f.name in data:
            val = data[f.name]
            if dataclasses.is_dataclass(f.type) and isinstance(val, dict):
                kwargs[f.name] = _dict_to_dataclass(val, f.type)
            else:
                kwargs[f.name] = val
    return cls(**kwargs)


def load_config(env: str = None) -> AppConfig:
    """
    Nạp cấu hình Fleet Data Platform theo 3 tầng ưu tiên.

    Args:
        env: Tên môi trường ("local", "dev", "staging", "prod").
             Nếu None thì đọc từ biến môi trường APP_ENV (mặc định "local").

    Returns:
        AppConfig dataclass đã được validate và phân giải biến môi trường.
    """
    env = env or os.environ.get("APP_ENV", "local")
    configs_dir = _find_configs_dir()

    # Tầng 1: Đọc base.yaml
    base_file = configs_dir / "base.yaml"
    if not base_file.exists():
        raise FileNotFoundError(f"Thiếu file cấu hình gốc: {base_file}")

    with open(base_file, "r", encoding="utf-8") as f:
        base_data = yaml.safe_load(f) or {}

    # Tầng 2: Đọc và ghi đè {env}.yaml
    env_file = configs_dir / f"{env}.yaml"
    if env_file.exists():
        with open(env_file, "r", encoding="utf-8") as f:
            env_data = yaml.safe_load(f) or {}
        merged_data = _deep_merge(base_data, env_data)
    else:
        merged_data = base_data

    # Tầng 3: Phân giải ${...} biến môi trường
    resolved_data = _resolve_dict(merged_data)

    # Inject env name
    resolved_data["env"] = env

    # Chuyển dict thành AppConfig dataclass
    cfg = _dict_to_dataclass(resolved_data, AppConfig)

    return cfg


# =============================================================================
# 4. TIỆN ÍCH TẠO SPARK SESSION TỪ CONFIG
# =============================================================================

def create_spark_session(cfg: AppConfig, job_name: str = None):
    """
    Tạo SparkSession chuẩn hóa từ AppConfig — tự động cấu hình theo môi trường.

    Args:
        cfg: AppConfig đã nạp qua load_config().
        job_name: Tên job Spark (mặc định lấy từ cfg.spark.app_name).

    Returns:
        SparkSession đã cấu hình đầy đủ.
    """
    from pyspark.sql import SparkSession

    app_name = f"{cfg.spark.app_name}-{job_name}" if job_name else cfg.spark.app_name

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(cfg.spark.master)
        .config("spark.driver.memory", cfg.spark.driver_memory)
        .config("spark.executor.memory", cfg.spark.executor_memory)
        .config("spark.sql.parquet.compression.codec", cfg.spark.compression_codec)
        .config("spark.sql.adaptive.enabled", str(cfg.spark.adaptive_enabled).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", str(cfg.spark.adaptive_skew_join).lower())
        .config("spark.serializer", cfg.spark.serializer)
        .config("spark.sql.shuffle.partitions", str(cfg.spark.shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", cfg.etl.partition_overwrite_mode)
    )

    # Kích hoạt Hive Catalog nếu môi trường hỗ trợ
    if cfg.hive.enabled and cfg.hive.metastore_uris:
        builder = (
            builder
            .config("spark.sql.catalogImplementation", "hive")
            .config("hive.metastore.uris", cfg.hive.metastore_uris)
            .enableHiveSupport()
        )

    # Kích hoạt Spark Event Log nếu production
    if cfg.spark.event_log_enabled and cfg.spark.event_log_dir:
        builder = (
            builder
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", cfg.spark.event_log_dir)
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(cfg.log_level)

    return spark


# =============================================================================
# 5. CLI ENTRYPOINT ĐỂ KIỂM TRA CẤU HÌNH NHANH TỪ TERMINAL
# =============================================================================

if __name__ == "__main__":
    """
    Chạy trực tiếp để kiểm tra cấu hình hiện tại:
      APP_ENV=local python -m src.core.config
      APP_ENV=prod  python -m src.core.config
    """
    import json
    import dataclasses

    cfg = load_config()

    print("=" * 70)
    print(f"  Fleet Data Platform — Configuration Dump")
    print(f"  Environment : {cfg.env}")
    print(f"  App Version : {cfg.version}")
    print("=" * 70)

    # Chuyển dataclass sang dict và in ra JSON đẹp (ẩn password)
    cfg_dict = dataclasses.asdict(cfg)

    # Ẩn password trong output
    for section in ["database", "redis", "storage"]:
        if section in cfg_dict and "password" in cfg_dict[section]:
            val = cfg_dict[section]["password"]
            if val:
                cfg_dict[section]["password"] = val[:2] + "***" + val[-2:] if len(val) > 4 else "***"

    print(json.dumps(cfg_dict, indent=2, ensure_ascii=False))
