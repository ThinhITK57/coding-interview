"""
Fleet Data Platform — Unit Tests for Configuration Loader
===========================================================
Kiểm tra:
  1. Đọc được base.yaml và merge local.yaml đúng thứ tự ưu tiên.
  2. Phân giải ${VAR_NAME} từ biến môi trường OS.
  3. Fallback về giá trị mặc định nếu biến môi trường không tồn tại.
  4. Deep merge không làm mất key ở tầng base.

Usage:
  cd fleet-platform
  python -m pytest tests/test_config.py -v
"""

import os
import sys
import pytest
from pathlib import Path

# Thêm thư mục gốc dự án vào PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import load_config, _resolve_env_vars, _deep_merge, AppConfig


class TestEnvVarResolution:
    """Kiểm tra phân giải biến môi trường ${...} trong giá trị YAML."""

    def test_resolve_existing_env_var(self, monkeypatch):
        monkeypatch.setenv("TEST_DB_PASS", "SuperSecret123")
        result = _resolve_env_vars("${TEST_DB_PASS}")
        assert result == "SuperSecret123"

    def test_resolve_with_default_value(self):
        # Biến không tồn tại, trả về default
        result = _resolve_env_vars("${NONEXISTENT_VAR:fallback_value}")
        assert result == "fallback_value"

    def test_resolve_missing_no_default(self):
        # Biến không tồn tại, không có default → chuỗi rỗng
        result = _resolve_env_vars("${TOTALLY_MISSING}")
        assert result == ""

    def test_non_string_passthrough(self):
        # Giá trị không phải string thì giữ nguyên
        assert _resolve_env_vars(42) == 42
        assert _resolve_env_vars(True) is True
        assert _resolve_env_vars(None) is None


class TestDeepMerge:
    """Kiểm tra deep merge 2 dictionaries."""

    def test_simple_override(self):
        base = {"a": 1, "b": 2}
        override = {"b": 99}
        result = _deep_merge(base, override)
        assert result == {"a": 1, "b": 99}

    def test_nested_override(self):
        base = {"db": {"host": "localhost", "port": 5432}}
        override = {"db": {"host": "prod-server"}}
        result = _deep_merge(base, override)
        assert result == {"db": {"host": "prod-server", "port": 5432}}

    def test_new_key_added(self):
        base = {"a": 1}
        override = {"b": 2}
        result = _deep_merge(base, override)
        assert result == {"a": 1, "b": 2}


class TestLoadConfig:
    """Kiểm tra load_config() end-to-end."""

    def test_load_local_config(self, monkeypatch):
        """Đọc cấu hình local — database.host phải là localhost."""
        monkeypatch.setenv("FLEET_CONFIGS_DIR", str(PROJECT_ROOT / "configs"))
        cfg = load_config(env="local")

        assert isinstance(cfg, AppConfig)
        assert cfg.env == "local"
        assert cfg.database.host == "localhost"
        assert cfg.spark.master == "local[2]"
        assert cfg.spark.shuffle_partitions == 4  # Override từ local.yaml

    def test_load_prod_config(self, monkeypatch):
        """Đọc cấu hình prod — spark.master phải trỏ về cụm bare-metal."""
        monkeypatch.setenv("FLEET_CONFIGS_DIR", str(PROJECT_ROOT / "configs"))
        monkeypatch.setenv("FLEET_DB_PASSWORD", "RealProdPass")
        monkeypatch.setenv("REDIS_PASSWORD", "RealRedisPass")
        cfg = load_config(env="prod")

        assert cfg.env == "prod"
        assert cfg.database.host == "master"
        assert cfg.database.password == "RealProdPass"
        assert cfg.spark.master == "spark://master:7077"
        assert cfg.spark.shuffle_partitions == 200
        assert cfg.kafka.bootstrap_servers == "master:9092,slave1:9092,slave2:9092"
        assert cfg.monitoring.alerting_enabled is True

    def test_base_values_preserved(self, monkeypatch):
        """Giá trị từ base.yaml phải được giữ lại nếu env.yaml không ghi đè."""
        monkeypatch.setenv("FLEET_CONFIGS_DIR", str(PROJECT_ROOT / "configs"))
        cfg = load_config(env="local")

        # Các giá trị base.yaml: etl.scd2_expiration_date, kafka.topics
        assert cfg.etl.scd2_expiration_date == "9999-12-31"
        assert cfg.etl.retry_count == 3

    def test_env_from_os_variable(self, monkeypatch):
        """Nếu không truyền env, đọc từ biến APP_ENV."""
        monkeypatch.setenv("FLEET_CONFIGS_DIR", str(PROJECT_ROOT / "configs"))
        monkeypatch.setenv("APP_ENV", "local")
        cfg = load_config()
        assert cfg.env == "local"

    def test_jdbc_url_helper(self, monkeypatch):
        """DatabaseConfig.get_jdbc_url() tạo URL đúng chuẩn."""
        monkeypatch.setenv("FLEET_CONFIGS_DIR", str(PROJECT_ROOT / "configs"))
        cfg = load_config(env="local")
        url = cfg.database.get_jdbc_url()
        assert "localhost" in url
        assert "5432" in url
        assert "fleet_oltp" in url

    def test_connection_properties_helper(self, monkeypatch):
        """DatabaseConfig.get_connection_properties() trả về dict chuẩn Spark."""
        monkeypatch.setenv("FLEET_CONFIGS_DIR", str(PROJECT_ROOT / "configs"))
        cfg = load_config(env="local")
        props = cfg.database.get_connection_properties()
        assert "user" in props
        assert "driver" in props
        assert props["driver"] == "org.postgresql.Driver"
