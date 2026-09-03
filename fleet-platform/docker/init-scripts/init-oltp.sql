-- =============================================================================
-- init-oltp.sql — Khởi tạo Schema giả lập Odoo OLTP cho Local Development
-- =============================================================================
-- Tự động chạy khi PostgreSQL container khởi động lần đầu.

-- Bảng customers (Khách hàng vận tải)
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(200) NOT NULL,
    contact_name VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(100),
    status VARCHAR(20) DEFAULT 'mới',
    fleet_size INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Bảng heads (Trạm sửa chữa)
CREATE TABLE IF NOT EXISTS heads (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    address VARCHAR(300),
    lat DECIMAL(10, 7),
    lng DECIMAL(10, 7),
    capacity INT DEFAULT 10,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Bảng work_orders (Lệnh sửa chữa)
CREATE TABLE IF NOT EXISTS work_orders (
    id SERIAL PRIMARY KEY,
    head_id INT REFERENCES heads(id),
    customer_id INT REFERENCES customers(id),
    truck_plate VARCHAR(20),
    status VARCHAR(30) DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Bảng invoices (Hóa đơn)
CREATE TABLE IF NOT EXISTS invoices (
    id SERIAL PRIMARY KEY,
    work_order_id INT REFERENCES work_orders(id),
    customer_id INT REFERENCES customers(id),
    invoice_date DATE DEFAULT CURRENT_DATE,
    service_amount DECIMAL(15, 2) DEFAULT 0,
    parts_amount DECIMAL(15, 2) DEFAULT 0,
    total_amount DECIMAL(15, 2) DEFAULT 0,
    status VARCHAR(20) DEFAULT 'draft',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Bảng components (Linh kiện)
CREATE TABLE IF NOT EXISTS components (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(200),
    category VARCHAR(50),
    unit_cost DECIMAL(15, 2) DEFAULT 0
);

-- Bảng parts_inventory (Tồn kho linh kiện theo trạm)
CREATE TABLE IF NOT EXISTS parts_inventory (
    head_id INT REFERENCES heads(id),
    component_id INT REFERENCES components(id),
    quantity INT DEFAULT 0,
    unit_price DECIMAL(15, 2) DEFAULT 0,
    PRIMARY KEY (head_id, component_id)
);

-- =============================================================================
-- Nạp dữ liệu mẫu (Seed Data)
-- =============================================================================

INSERT INTO heads (name, address, lat, lng, capacity) VALUES
    ('Trạm Thủ Đức', '123 Võ Văn Ngân, TP.HCM', 10.8481, 106.7719, 15),
    ('Trạm Bình Dương', '456 Đại lộ Bình Dương', 11.0253, 106.6532, 20),
    ('Trạm Long An', '789 QL1A, Long An', 10.5359, 106.4074, 10)
ON CONFLICT DO NOTHING;

INSERT INTO customers (company_name, contact_name, phone, status, fleet_size) VALUES
    ('Công ty TNHH Vận Tải Phương Nam', 'Trần Văn Bình', '0901-234-567', 'thường niên', 35),
    ('HTX Vận Tải Đồng Nai', 'Nguyễn Thị Lan', '0912-345-678', 'cũ', 12),
    ('Công ty CP Logistics Sài Gòn', 'Lê Hoàng Minh', '0923-456-789', 'thường niên', 50),
    ('Chủ xe Nguyễn Văn Tám', 'Nguyễn Văn Tám', '0934-567-890', 'mới', 3)
ON CONFLICT DO NOTHING;

INSERT INTO components (code, name, category, unit_cost) VALUES
    ('BRK-001', 'Bố thắng đĩa trước', 'Phanh', 450000),
    ('OIL-001', 'Dầu động cơ 15W-40 5L', 'Dầu nhớt', 380000),
    ('FLT-001', 'Lọc gió động cơ', 'Lọc', 120000),
    ('TIR-001', 'Lốp xe tải 12R22.5', 'Lốp', 4500000)
ON CONFLICT DO NOTHING;

INSERT INTO work_orders (head_id, customer_id, truck_plate, status) VALUES
    (1, 1, '51C-12345', 'completed'),
    (1, 2, '60C-67890', 'in_progress'),
    (2, 3, '51D-11111', 'completed'),
    (3, 4, '62C-22222', 'pending')
ON CONFLICT DO NOTHING;

INSERT INTO invoices (work_order_id, customer_id, invoice_date, service_amount, parts_amount, total_amount, status) VALUES
    (1, 1, '2026-08-15', 500000, 1700000, 2200000, 'paid'),
    (3, 3, '2026-08-20', 800000, 7000000, 7800000, 'paid')
ON CONFLICT DO NOTHING;

-- Bật Logical Replication cho CDC (nếu cần test Debezium local)
ALTER SYSTEM SET wal_level = logical;
