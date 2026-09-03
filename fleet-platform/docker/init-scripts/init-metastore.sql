-- =============================================================================
-- init-metastore.sql — Khởi tạo Database Hive Metastore trên cùng Postgres local
-- =============================================================================

CREATE DATABASE metastore_db;
CREATE USER hive WITH PASSWORD 'hive_local_pass';
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO hive;
