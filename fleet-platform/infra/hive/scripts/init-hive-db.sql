-- =============================================================================
-- init-hive-db.sql — Khởi tạo Database & User cho Hive Metastore trên PostgreSQL
-- Chạy trên máy: master (PostgreSQL 14+)
-- Lệnh chạy: sudo -u postgres psql -f init-hive-db.sql
-- =============================================================================

-- 1. Tạo Database riêng biệt lưu Metadata của Hive
CREATE DATABASE metastore_db;

-- 2. Tạo User riêng cho Hive tuân thủ nguyên tắc đặc quyền tối thiểu
CREATE USER hive WITH PASSWORD 'HivePass_VCS_2026';

-- 3. Cấp toàn quyền trên database metastore_db cho user hive
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO hive;

-- 4. Chuyển sang metastore_db và cấp quyền trên schema public
\c metastore_db
GRANT ALL ON SCHEMA public TO hive;
ALTER SCHEMA public OWNER TO hive;
