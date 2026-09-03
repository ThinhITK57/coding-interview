-- =============================================================================
-- Fleet Platform — Seed Data (Dữ liệu mẫu thực tế)
-- Database: fleet_oltp
-- Chạy sau 01_schema.sql
-- Usage: psql -h master -U fleet_app -d fleet_oltp -f 02_seed_data.sql
-- =============================================================================

-- =============================================================
-- 1. Heads (10 trạm sửa chữa — khu vực TP.HCM & lân cận)
-- =============================================================

INSERT INTO public.heads (name, address, district, city, latitude, longitude, phone, capacity_slots) VALUES
('Trạm Bình Tân',        '123 Kinh Dương Vương, P. Bình Trị Đông',   'Bình Tân',    'TP.HCM',    10.7513900, 106.6063900, '028-3877-0001', 6),
('Trạm Thủ Đức',         '456 Xa lộ Hà Nội, P. Hiệp Phú',           'TP. Thủ Đức', 'TP.HCM',    10.8485600, 106.7717200, '028-3877-0002', 4),
('Trạm Quận 7',          '789 Nguyễn Thị Thập, P. Tân Phong',        'Quận 7',      'TP.HCM',    10.7375500, 106.7218400, '028-3877-0003', 5),
('Trạm Bình Dương',      '321 ĐT 743, P. An Phú',                    'Thuận An',    'Bình Dương', 10.9596100, 106.6528600, '0274-377-0004', 8),
('Trạm Long An',         '654 QL1A, TT. Bến Lức',                    'Bến Lức',     'Long An',    10.6498200, 106.4865100, '0272-377-0005', 4),
('Trạm Tân Cảng',        '99 Nguyễn Thị Định, P. An Phú',            'TP. Thủ Đức', 'TP.HCM',    10.7936800, 106.7539100, '028-3877-0006', 6),
('Trạm Củ Chi',          '888 TL8, TT. Củ Chi',                      'Củ Chi',      'TP.HCM',    11.0044400, 106.4938900, '028-3877-0007', 3),
('Trạm Cát Lái',         '150 Nguyễn Thị Định, P. Cát Lái',          'TP. Thủ Đức', 'TP.HCM',    10.7671200, 106.7887500, '028-3877-0008', 5),
('Trạm Đồng Nai',        '200 QL1A, P. Long Bình Tân',               'Biên Hòa',   'Đồng Nai',  10.9431100, 106.8382800, '0251-377-0009', 7),
('Trạm Vũng Tàu',        '50 Đường 30/4, P. Thắng Nhất',             'TP. Vũng Tàu','Bà Rịa-VT', 10.3559500, 107.0843800, '0254-377-0010', 4);

-- =============================================================
-- 2. Components (15 loại linh kiện phổ biến cho xe tải)
-- =============================================================

INSERT INTO public.components (code, name, category, unit) VALUES
('BRK-PAD-001', 'Má phanh trước',             'Phanh',          'bộ'),
('BRK-PAD-002', 'Má phanh sau',               'Phanh',          'bộ'),
('BRK-DSC-001', 'Đĩa phanh',                  'Phanh',          'cái'),
('TIR-RDL-001', 'Lốp xe tải 11R22.5',         'Lốp',            'cái'),
('TIR-RDL-002', 'Lốp xe tải 12R22.5',         'Lốp',            'cái'),
('ENG-OIL-001', 'Dầu nhớt 15W-40 (thùng 18L)','Động cơ',        'thùng'),
('ENG-FLT-001', 'Lọc dầu động cơ',            'Động cơ',        'cái'),
('ENG-FLT-002', 'Lọc nhiên liệu',             'Động cơ',        'cái'),
('ENG-BLT-001', 'Dây curoa',                   'Động cơ',        'sợi'),
('SUS-SHK-001', 'Giảm xóc trước',             'Hệ thống treo',  'cái'),
('SUS-SHK-002', 'Giảm xóc sau',               'Hệ thống treo',  'cái'),
('ELC-BAT-001', 'Ắc quy 12V 150Ah',           'Hệ thống điện',  'bình'),
('ELC-ALT-001', 'Máy phát điện',              'Hệ thống điện',  'cái'),
('CLT-PRS-001', 'Bộ ly hợp',                  'Hộp số',         'bộ'),
('CHL-RAD-001', 'Két nước làm mát',           'Hệ thống làm mát','cái');

-- =============================================================
-- 3. Customers (8 công ty vận tải / chủ xe)
-- =============================================================

INSERT INTO public.customers (company_name, contact_name, phone, email, tax_code, fleet_size, status) VALUES
('Công ty TNHH Vận Tải Phương Nam',    'Trần Văn Bình',   '0901-234-567', 'binh.tv@phuongnam.vn',   '0312345678', 35, 'thường niên'),
('HTX Vận Tải Đồng Nai',              'Nguyễn Thị Lan',  '0912-345-678', 'lan.nt@dongnaicoop.vn',  '3612345678', 22, 'cũ'),
('Công ty CP Logistics Sài Gòn',       'Lê Hoàng Minh',   '0923-456-789', 'minh.lh@sglog.vn',       '0313456789', 50, 'thường niên'),
('Chủ xe Nguyễn Văn Tám',              'Nguyễn Văn Tám',  '0934-567-890', NULL,                     NULL,          3, 'mới'),
('Công ty TNHH TM-DV Hoàng Long',     'Phạm Thị Hương',  '0945-678-901', 'huong.pt@hoanglong.vn',  '0314567890', 18, 'cũ'),
('Đội xe Anh Tuấn',                    'Trương Anh Tuấn', '0956-789-012', NULL,                     NULL,          5, 'mới'),
('Công ty CP Vận Tải Miền Đông',       'Võ Minh Trí',     '0967-890-123', 'tri.vm@miendong.vn',     '3614567890', 40, 'thường niên'),
('Công ty TNHH Vận Chuyển Nhanh',      'Đặng Thị Mai',    '0978-901-234', 'mai.dt@vanconhanh.vn',   '0315678901', 12, 'cũ');

-- =============================================================
-- 4. Parts Inventory (tồn kho linh kiện cho mỗi Head)
-- Mỗi Head có khoảng 8-12 loại linh kiện
-- =============================================================

-- Trạm Bình Tân (head_id=1) — trạm lớn, đầy đủ
INSERT INTO public.parts_inventory (head_id, component_id, quantity, unit_price, cost_price, min_stock) VALUES
(1, 1,  20, 850000,   620000,  5),   -- Má phanh trước
(1, 2,  18, 750000,   540000,  5),   -- Má phanh sau
(1, 3,  10, 1200000,  880000,  3),   -- Đĩa phanh
(1, 4,  12, 3500000,  2800000, 4),   -- Lốp 11R22.5
(1, 6,   8, 2200000,  1700000, 3),   -- Dầu nhớt
(1, 7,  30, 180000,   120000,  10),  -- Lọc dầu
(1, 8,  25, 220000,   150000,  10),  -- Lọc nhiên liệu
(1, 10,  6, 2800000,  2100000, 2),   -- Giảm xóc trước
(1, 12,  4, 3200000,  2500000, 2),   -- Ắc quy
(1, 14,  3, 5500000,  4200000, 1);   -- Bộ ly hợp

-- Trạm Thủ Đức (head_id=2)
INSERT INTO public.parts_inventory (head_id, component_id, quantity, unit_price, cost_price, min_stock) VALUES
(2, 1,  15, 870000,   630000,  5),
(2, 2,  12, 770000,   550000,  5),
(2, 4,   8, 3600000,  2850000, 3),
(2, 5,   6, 4200000,  3400000, 3),
(2, 6,   5, 2250000,  1750000, 2),
(2, 7,  20, 185000,   125000,  8),
(2, 9,   4, 450000,   320000,  2),
(2, 11,  5, 2600000,  1950000, 2);

-- Trạm Quận 7 (head_id=3)
INSERT INTO public.parts_inventory (head_id, component_id, quantity, unit_price, cost_price, min_stock) VALUES
(3, 1,  10, 860000,   625000,  4),
(3, 3,   5, 1250000,  900000,  2),
(3, 4,  10, 3550000,  2820000, 4),
(3, 6,   6, 2200000,  1700000, 3),
(3, 7,  25, 180000,   120000,  8),
(3, 8,  20, 225000,   155000,  8),
(3, 12,  3, 3250000,  2550000, 1),
(3, 15,  2, 1800000,  1350000, 1);

-- Trạm Bình Dương (head_id=4) — trạm lớn nhất
INSERT INTO public.parts_inventory (head_id, component_id, quantity, unit_price, cost_price, min_stock) VALUES
(4, 1,  25, 840000,   610000,  8),
(4, 2,  25, 740000,   530000,  8),
(4, 3,  12, 1180000,  860000,  4),
(4, 4,  20, 3450000,  2750000, 6),
(4, 5,  15, 4100000,  3300000, 5),
(4, 6,  12, 2150000,  1680000, 5),
(4, 7,  40, 175000,   115000,  15),
(4, 8,  35, 215000,   145000,  15),
(4, 10,  8, 2750000,  2050000, 3),
(4, 11,  8, 2550000,  1900000, 3),
(4, 12,  6, 3150000,  2450000, 2),
(4, 14,  4, 5400000,  4100000, 2);

-- Trạm Long An (head_id=5) — trạm nhỏ
INSERT INTO public.parts_inventory (head_id, component_id, quantity, unit_price, cost_price, min_stock) VALUES
(5, 1,   8, 880000,   640000,  3),
(5, 4,   5, 3600000,  2900000, 2),
(5, 6,   4, 2300000,  1800000, 2),
(5, 7,  15, 190000,   130000,  5),
(5, 12,  2, 3300000,  2600000, 1);

-- =============================================================
-- 5. Work Orders (15 đơn sửa chữa mẫu — các trạng thái khác nhau)
-- =============================================================

INSERT INTO public.work_orders (customer_id, head_id, truck_plate, status, issue_summary, scheduled_at, started_at, completed_at, labor_cost, total_parts_cost, total_amount) VALUES
-- Completed orders
(1, 1, '51C-123.45', 'completed', 'Thay má phanh trước + lọc dầu định kỳ',              '2024-07-01 08:00', '2024-07-01 08:30', '2024-07-01 11:00', 500000,  1700000, 2200000),
(1, 4, '51C-123.46', 'completed', 'Thay lốp trước + cân chỉnh',                          '2024-07-03 09:00', '2024-07-03 09:30', '2024-07-03 14:00', 800000,  7000000, 7800000),
(3, 2, '51D-456.78', 'completed', 'Bảo dưỡng định kỳ 50.000km: dầu nhớt + lọc + dây curoa','2024-07-05 08:00', '2024-07-05 08:30', '2024-07-05 12:00', 600000,  2850000, 3450000),
(2, 9, '60C-789.01', 'completed', 'Thay ắc quy + kiểm tra hệ thống điện',                '2024-07-08 10:00', '2024-07-08 10:30', '2024-07-08 13:00', 400000,  3200000, 3600000),
(7, 1, '51H-234.56', 'completed', 'Thay giảm xóc trước 2 bên',                            '2024-07-10 08:00', '2024-07-10 08:30', '2024-07-10 15:00', 1200000, 5600000, 6800000),
(5, 3, '51C-567.89', 'completed', 'Thay đĩa phanh + má phanh trước',                      '2024-07-12 09:00', '2024-07-12 09:30', '2024-07-12 13:30', 700000,  2110000, 2810000),
(8, 4, '51F-890.12', 'completed', 'Thay bộ ly hợp',                                        '2024-07-15 08:00', '2024-07-15 08:30', '2024-07-15 17:00', 2000000, 5400000, 7400000),
(3, 6, '51D-456.79', 'completed', 'Lốp xe nổ — thay lốp khẩn cấp + kiểm tra hệ thống treo','2024-07-18 07:00', '2024-07-18 07:30', '2024-07-18 11:00', 500000,  3500000, 4000000),

-- In-progress orders
(1, 1, '51C-123.47', 'in_progress', 'Thay két nước + kiểm tra hệ thống làm mát',          '2024-07-22 08:00', '2024-07-22 08:30', NULL,                800000,  1800000, 2600000),
(4, 2, '62C-111.22', 'in_progress', 'Lốp mòn — thay 4 lốp + cân bằng động',               '2024-07-22 09:00', '2024-07-22 09:30', NULL,                1000000, 14400000, 15400000),

-- Scheduled (chưa bắt đầu)
(6, 5, '62C-333.44', 'scheduled', 'Bảo dưỡng định kỳ 100.000km',                           '2024-07-25 08:00', NULL, NULL, 0, 0, 0),
(2, 4, '60C-789.02', 'scheduled', 'Thay dầu nhớt + lọc nhiên liệu + lọc dầu',              '2024-07-25 10:00', NULL, NULL, 0, 0, 0),
(7, 1, '51H-234.57', 'scheduled', 'Kiểm tra phanh + thay má phanh nếu cần',                 '2024-07-26 08:00', NULL, NULL, 0, 0, 0),

-- Cancelled
(5, 3, '51C-567.90', 'cancelled', 'Thay giảm xóc — khách hủy do đổi lịch',                 '2024-07-20 09:00', NULL, NULL, 0, 0, 0),
(8, 9, '51F-890.13', 'cancelled', 'Bảo dưỡng — xe đang trên tuyến, hoãn',                   '2024-07-19 14:00', NULL, NULL, 0, 0, 0);

-- =============================================================
-- 6. Invoices (chỉ cho completed work orders)
-- =============================================================

INSERT INTO public.invoices (work_order_id, customer_id, invoice_number, invoice_date, due_date, service_amount, parts_amount, tax_amount, total_amount, status, payment_date) VALUES
(1, 1, 'HD-2024-000001', '2024-07-01', '2024-07-31', 500000,  1700000, 220000,  2420000,  'paid',    '2024-07-15 10:00'),
(2, 1, 'HD-2024-000002', '2024-07-03', '2024-08-02', 800000,  7000000, 780000,  8580000,  'paid',    '2024-07-20 14:00'),
(3, 3, 'HD-2024-000003', '2024-07-05', '2024-08-04', 600000,  2850000, 345000,  3795000,  'paid',    '2024-07-10 09:00'),
(4, 2, 'HD-2024-000004', '2024-07-08', '2024-08-07', 400000,  3200000, 360000,  3960000,  'paid',    '2024-07-25 11:00'),
(5, 7, 'HD-2024-000005', '2024-07-10', '2024-08-09', 1200000, 5600000, 680000,  7480000,  'issued',  NULL),
(6, 5, 'HD-2024-000006', '2024-07-12', '2024-08-11', 700000,  2110000, 281000,  3091000,  'issued',  NULL),
(7, 8, 'HD-2024-000007', '2024-07-15', '2024-08-14', 2000000, 5400000, 740000,  8140000,  'overdue', NULL),
(8, 3, 'HD-2024-000008', '2024-07-18', '2024-08-17', 500000,  3500000, 400000,  4400000,  'paid',    '2024-07-22 16:00');

-- =============================================================
-- 7. Verify counts
-- =============================================================

SELECT 'heads' AS table_name, COUNT(*) AS row_count FROM public.heads
UNION ALL SELECT 'components', COUNT(*) FROM public.components
UNION ALL SELECT 'customers', COUNT(*) FROM public.customers
UNION ALL SELECT 'parts_inventory', COUNT(*) FROM public.parts_inventory
UNION ALL SELECT 'work_orders', COUNT(*) FROM public.work_orders
UNION ALL SELECT 'invoices', COUNT(*) FROM public.invoices
ORDER BY table_name;

-- Kết quả mong đợi:
--  table_name      | row_count
-- -----------------+-----------
--  components      |        15
--  customers       |         8
--  heads           |        10
--  invoices        |         8
--  parts_inventory |        45
--  work_orders     |        15
