-- =============================================================================
-- Fleet Platform — Grant CDC Permissions
-- Chạy sau 01_schema.sql (cần bảng đã tồn tại)
-- Usage: sudo -u postgres psql -d fleet_oltp -f 03_cdc_permissions.sql
-- =============================================================================

-- Debezium user cần SELECT để đọc initial snapshot
GRANT USAGE ON SCHEMA public TO fleet_cdc;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO fleet_cdc;

-- Bảng mới tạo sau này cũng tự grant
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO fleet_cdc;

-- Publication cho logical replication (Debezium subscribe vào đây)
CREATE PUBLICATION fleet_cdc_publication FOR ALL TABLES;

-- Verify
SELECT * FROM pg_publication WHERE pubname = 'fleet_cdc_publication';
SELECT relname, relreplident FROM pg_class
WHERE relname IN ('customers', 'heads', 'parts_inventory', 'work_orders', 'invoices', 'components', 'work_order_items');
