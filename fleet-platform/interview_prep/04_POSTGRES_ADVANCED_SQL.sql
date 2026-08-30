
-- =============================================================================
-- 🐘 BÀI TẬP THỰC HÀNH ADVANCED POSTGRESQL & PL/pgSQL (LEVEL MIDDLE / SENIOR DE)
-- =============================================================================
-- File này chứa các script SQL nâng cao có sẵn đáp án phục vụ thực hành:
--   - Bài 1: Custom Trigger & PL/pgSQL Function cho `updated_at` & Audit Log.
--   - Bài 2: Advanced Window Functions (Running Total, YoY Growth, MoM, Lead/Lag).
--   - Bài 3: PostgreSQL Declarative Range Table Partitioning theo `invoice_date`.
--   - Bài 4: Recursive CTE Query phân tích Cây cấu trúc Linh kiện (BOM Tree).
-- =============================================================================

-- =============================================================================
-- BÀI TẬP 1: TRIGGER & PL/pgSQL FUNCTION TỰ ĐỘNG CẬP NHẬT `updated_at`
-- =============================================================================

-- 1. Tạo hàm PL/pgSQL dùng chung
CREATE OR REPLACE FUNCTION public.fn_update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Gắn Trigger vào bảng `work_orders`
DROP TRIGGER IF EXISTS trg_work_orders_updated_at ON public.work_orders;
CREATE TRIGGER trg_work_orders_updated_at
    BEFORE UPDATE ON public.work_orders
    FOR EACH ROW
    EXECUTE FUNCTION public.fn_update_updated_at();

-- Verify Trigger
-- UPDATE public.work_orders SET status = 'completed' WHERE id = 1;
-- SELECT id, status, updated_at FROM public.work_orders WHERE id = 1;


-- =============================================================================
-- BÀI TẬP 2: ADVANCED WINDOW FUNCTIONS CHO BÁO CÁO TÀI CHÍNH
-- =============================================================================

-- Bài toán: Tính Doanh thu lũy kế (Running Total), Doanh thu kỳ trước (LAG),
-- và Tỉ lệ tăng trưởng doanh thu so với kỳ trước (MoM Growth Rate).

WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', invoice_date)::DATE AS revenue_month,
        SUM(total_amount)                       AS monthly_total,
        SUM(service_amount)                     AS monthly_labor,
        SUM(parts_amount)                       AS monthly_parts,
        COUNT(id)                               AS paid_invoice_count
    FROM public.invoices
    WHERE status = 'paid'
    GROUP BY DATE_TRUNC('month', invoice_date)::DATE
)
SELECT
    revenue_month,
    monthly_total,
    -- 1. Doanh thu lũy kế cộng dồn qua các tháng (Running Total)
    SUM(monthly_total) OVER (
        ORDER BY revenue_month 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_revenue,
    
    -- 2. Doanh thu tháng trước (LAG)
    LAG(monthly_total, 1, 0.0) OVER (ORDER BY revenue_month) AS prev_month_total,
    
    -- 3. Tỉ lệ tăng trưởng so với tháng trước (MoM Growth %)
    ROUND(
        (monthly_total - LAG(monthly_total, 1, 0.0) OVER (ORDER BY revenue_month)) 
        / NULLIF(LAG(monthly_total, 1, 0.0) OVER (ORDER BY revenue_month), 0) * 100.0,
        2
    ) AS mom_growth_pct
FROM monthly_revenue
ORDER BY revenue_month;


-- =============================================================================
-- BÀI TẬP 3: DECLARATIVE RANGE TABLE PARTITIONING TRÊN POSTGRESQL
-- =============================================================================

-- Tạo bảng partitioned `invoices_partitioned` phân vùng theo `invoice_date` theo năm
CREATE TABLE public.invoices_partitioned (
    id              SERIAL,
    work_order_id   INTEGER         NOT NULL,
    customer_id     INTEGER         NOT NULL,
    invoice_number  VARCHAR(50)     NOT NULL,
    invoice_date    DATE            NOT NULL,
    total_amount    DECIMAL(15, 2)  NOT NULL,
    status          VARCHAR(20)     NOT NULL,
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, invoice_date)  -- Primary key phải chứa partition key
) PARTITION BY RANGE (invoice_date);

-- Tạo các Partitions theo Năm
CREATE TABLE public.invoices_y2025 PARTITION OF public.invoices_partitioned
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE public.invoices_y2026 PARTITION OF public.invoices_partitioned
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE public.invoices_y2027 PARTITION OF public.invoices_partitioned
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

-- Verify Partition Pruning trong EXPLAIN ANALYZE
-- EXPLAIN ANALYZE SELECT * FROM public.invoices_partitioned WHERE invoice_date = '2026-07-31';


-- =============================================================================
-- BÀI TẬP 4: RECURSIVE CTE QUERY PHÂN TÍCH CÂY LINH KIỆN (BOM TREE)
-- =============================================================================

-- Giả lập bảng Cấu trúc linh kiện đa cấp (Parent-Child Component Hierarchy)
CREATE TABLE IF NOT EXISTS public.component_hierarchy (
    id          SERIAL PRIMARY KEY,
    parent_id   INTEGER REFERENCES public.component_hierarchy(id),
    name        VARCHAR(200) NOT NULL,
    unit_cost   DECIMAL(15, 2) NOT NULL DEFAULT 0
);

-- Truy vấn đệ quy (Recursive CTE) tính tổng chi phí toàn bộ cụm linh kiện phanh
WITH RECURSIVE component_tree AS (
    -- Anchor Member: Lấy linh kiện gốc (Root Level)
    SELECT 
        id, 
        parent_id, 
        name, 
        unit_cost, 
        1 AS level, 
        ARRAY[name::TEXT] AS path
    FROM public.component_hierarchy
    WHERE parent_id IS NULL

    UNION ALL

    -- Recursive Member: Join đệ quy các linh kiện con
    SELECT 
        c.id, 
        c.parent_id, 
        c.name, 
        c.unit_cost, 
        ct.level + 1, 
        ct.path || c.name::TEXT
    FROM public.component_hierarchy c
    INNER JOIN component_tree ct ON c.parent_id = ct.id
)
SELECT 
    id, 
    REPEAT('  ', level - 1) || name AS indented_name, 
    unit_cost, 
    level, 
    ARRAY_TO_STRING(path, ' -> ') AS hierarchy_path
FROM component_tree
ORDER BY path;
