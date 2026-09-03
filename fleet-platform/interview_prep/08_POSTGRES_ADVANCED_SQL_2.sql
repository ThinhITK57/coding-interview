 -- =============================================================================
-- 🐘 BÀI TẬP THỰC HÀNH POSTGRESQL & WINDOW FUNCTIONS (SET 2 - MIDDLE / SENIOR DE)
-- =============================================================================
-- File này bổ sung 4 bài tập SQL chuyên sâu có sẵn lời giải:
--   - Bài 5: Advanced Window Functions (DENSE_RANK, NTILE, FIRST_VALUE, LAST_VALUE).
--   - Bài 6: Custom Audit Log Trigger lưu lịch sử biến động dạng JSONB.
--   - Bài 7: Materialized View với Concurrent Refresh cho Báo cáo Tài chính.
--   - Bài 8: Cross-Tabulation & Pivoting Query (Biến hàng thành cột Matrix Tồn kho).
-- =============================================================================

-- =============================================================================
-- BÀI TẬP 5: ADVANCED WINDOW FUNCTIONS (DENSE_RANK, NTILE, FIRST_VALUE)
-- =============================================================================

-- Bài toán:
-- 1. Xếp hạng Top 3 trạm sửa chữa (Heads) có doanh thu cao nhất theo từng tháng (DENSE_RANK).
-- 2. Chia các hóa đơn thành 4 nhóm phân khúc giá trị (NTILE 4 - Quartiles).
-- 3. Lấy giá trị hóa đơn đầu tiên và hóa đơn gần nhất của từng khách hàng (FIRST_VALUE, LAST_VALUE).

SELECT
    h.name                              AS head_name,
    DATE_TRUNC('month', i.invoice_date) AS invoice_month,
    i.total_amount,
    
    -- 1. Xếp hạng trạm có doanh thu lớn nhất từng tháng
    DENSE_RANK() OVER (
        PARTITION BY DATE_TRUNC('month', i.invoice_date)
        ORDER BY i.total_amount DESC
    ) AS revenue_rank_in_month,
    
    -- 2. Phân khúc giá trị hóa đơn thành 4 nhóm (Quartiles: 1=Thấp, 4=Cao)
    NTILE(4) OVER (
        ORDER BY i.total_amount ASC
    ) AS price_quartile,
    
    -- 3. Giá trị hóa đơn đầu tiên của khách hàng
    FIRST_VALUE(i.total_amount) OVER (
        PARTITION BY i.customer_id
        ORDER BY i.invoice_date ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS customer_first_invoice_amount
FROM public.invoices i
JOIN public.work_orders w ON i.work_order_id = w.id
JOIN public.heads h ON w.head_id = h.id
WHERE i.status = 'paid';


-- =============================================================================
-- BÀI TẬP 6: AUDIT LOG TRIGGER LƯU THAY ĐỔI DẠNG JSONB
-- =============================================================================

-- 1. Bảng lưu Audit Log
CREATE TABLE IF NOT EXISTS public.cdc_audit_log (
    id              BIGSERIAL PRIMARY KEY,
    table_name      VARCHAR(100) NOT NULL,
    operation       VARCHAR(10)  NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE'
    old_data        JSONB,
    new_data        JSONB,
    changed_by      VARCHAR(100) NOT NULL DEFAULT CURRENT_USER,
    changed_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- 2. Hàm Trigger ghi log dạng JSONB
CREATE OR REPLACE FUNCTION public.fn_cdc_audit_logger()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO public.cdc_audit_log (table_name, operation, old_data, new_data)
        VALUES (TG_TABLE_NAME, 'DELETE', to_jsonb(OLD), NULL);
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO public.cdc_audit_log (table_name, operation, old_data, new_data)
        VALUES (TG_TABLE_NAME, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO public.cdc_audit_log (table_name, operation, old_data, new_data)
        VALUES (TG_TABLE_NAME, 'INSERT', NULL, to_jsonb(NEW));
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 3. Gắn Trigger vào bảng `customers`
DROP TRIGGER IF EXISTS trg_customers_cdc_audit ON public.customers;
CREATE TRIGGER trg_customers_cdc_audit
    AFTER INSERT OR UPDATE OR DELETE ON public.customers
    FOR EACH ROW EXECUTE FUNCTION public.fn_cdc_audit_logger();


-- =============================================================================
-- BÀI TẬP 7: MATERIALIZED VIEW VỚI CONCURRENT REFRESH
-- =============================================================================

-- Tạo Materialized View tổng hợp Doanh thu theo Trạm & Tháng
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_head_monthly_revenue AS
SELECT
    h.id                                AS head_id,
    h.name                              AS head_name,
    DATE_TRUNC('month', i.invoice_date) AS invoice_month,
    SUM(i.service_amount)               AS total_labor_revenue,
    SUM(i.parts_amount)                 AS total_parts_revenue,
    SUM(i.total_amount)                 AS total_revenue,
    COUNT(i.id)                         AS invoice_count
FROM public.heads h
JOIN public.work_orders w ON h.id = w.head_id
JOIN public.invoices i ON w.id = i.work_order_id
WHERE i.status = 'paid'
GROUP BY h.id, h.name, DATE_TRUNC('month', i.invoice_date);

-- Tạo UNIQUE Index (Bắt buộc để chạy REFRESH CONCURRENTLY không bị lock table)
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_head_monthly_revenue_pk 
ON public.mv_head_monthly_revenue (head_id, invoice_month);

-- Lệnh làm mới dữ liệu không khóa đọc
-- REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_head_monthly_revenue;


-- =============================================================================
-- BÀI TẬP 8: PIVOTING & MATRIX QUERY (FILTER WHERE SYNTAX)
-- =============================================================================

-- Bài toán: Chuyển đổi bảng tồn kho từ hàng thành các Cột Tồn Kho theo từng Loại Linh kiện
SELECT
    h.name AS head_name,
    -- Dùng FILTER (WHERE ...) để Pivot cột trên Postgres
    COALESCE(SUM(p.quantity) FILTER (WHERE c.code = 'BRK-PAD-001'), 0) AS qty_brk_pad_front,
    COALESCE(SUM(p.quantity) FILTER (WHERE c.code = 'BRK-PAD-002'), 0) AS qty_brk_pad_rear,
    COALESCE(SUM(p.quantity) FILTER (WHERE c.code = 'TIR-RDL-001'), 0) AS qty_tire_11r22,
    COALESCE(SUM(p.quantity) FILTER (WHERE c.code = 'ENG-OIL-001'), 0) AS qty_engine_oil
FROM public.heads h
LEFT JOIN public.parts_inventory p ON h.id = p.head_id
LEFT JOIN public.components c ON p.component_id = c.id
GROUP BY h.id, h.name
ORDER BY h.name;
