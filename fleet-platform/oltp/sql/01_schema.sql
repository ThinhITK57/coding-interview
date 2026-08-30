-- =============================================================================
-- Fleet Platform — OLTP Schema (Odoo Simulation)
-- Database: fleet_oltp
-- Chạy trên: master, user: fleet_app
-- Usage: psql -h master -U fleet_app -d fleet_oltp -f 01_schema.sql
-- =============================================================================

-- =============================================================
-- 1. dim-like tables (referenced by others)
-- =============================================================

-- Trạm sửa chữa (Head / Service Station)
-- Redis GEO sẽ sync: latitude, longitude
-- Redis Hash sẽ sync: tất cả fields
CREATE TABLE public.heads (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(200)    NOT NULL,
    address         TEXT            NOT NULL,
    district        VARCHAR(100),
    city            VARCHAR(100)    NOT NULL,
    latitude        DECIMAL(10, 7)  NOT NULL,
    longitude       DECIMAL(10, 7)  NOT NULL,
    phone           VARCHAR(20),
    capacity_slots  INTEGER         NOT NULL DEFAULT 4,   -- số slot sửa chữa đồng thời
    opening_hour    TIME            NOT NULL DEFAULT '07:30',
    closing_hour    TIME            NOT NULL DEFAULT '17:30',
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- Khách hàng (chủ xe / đội xe)
-- SCD Type 2: status sẽ thay đổi mới → cũ → thường niên
CREATE TABLE public.customers (
    id              SERIAL PRIMARY KEY,
    company_name    VARCHAR(300)    NOT NULL,
    contact_name    VARCHAR(200)    NOT NULL,
    phone           VARCHAR(20)     NOT NULL,
    email           VARCHAR(200),
    tax_code        VARCHAR(20),
    fleet_size      INTEGER         NOT NULL DEFAULT 1,   -- số xe trong đội
    status          VARCHAR(20)     NOT NULL DEFAULT 'mới'
                    CHECK (status IN ('mới', 'cũ', 'thường niên')),
    loyalty_program VARCHAR(50),                          -- chương trình khách hàng thân thiết
    registered_at   TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- Linh kiện (master data)
CREATE TABLE public.components (
    id              SERIAL PRIMARY KEY,
    code            VARCHAR(50)     NOT NULL UNIQUE,       -- mã linh kiện: BRK-PAD-001
    name            VARCHAR(300)    NOT NULL,
    category        VARCHAR(100)    NOT NULL,               -- phanh, lốp, động cơ, hệ thống điện...
    unit            VARCHAR(20)     NOT NULL DEFAULT 'cái',
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- =============================================================
-- 2. Tồn kho linh kiện theo Head
-- =============================================================

-- Redis Hash sẽ sync: head_id → { component_code: {qty, price} }
CREATE TABLE public.parts_inventory (
    id              SERIAL PRIMARY KEY,
    head_id         INTEGER         NOT NULL REFERENCES public.heads(id),
    component_id    INTEGER         NOT NULL REFERENCES public.components(id),
    quantity        INTEGER         NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    unit_price      DECIMAL(15, 2)  NOT NULL,              -- giá bán cho khách
    cost_price      DECIMAL(15, 2)  NOT NULL,              -- giá vốn (tính lợi nhuận)
    min_stock       INTEGER         NOT NULL DEFAULT 5,    -- cảnh báo hết hàng
    updated_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    UNIQUE (head_id, component_id)                         -- 1 head chỉ có 1 record/linh kiện
);

-- =============================================================
-- 3. Giao dịch nghiệp vụ
-- =============================================================

-- Đơn sửa chữa (Work Order)
CREATE TABLE public.work_orders (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER         NOT NULL REFERENCES public.customers(id),
    head_id         INTEGER         NOT NULL REFERENCES public.heads(id),
    truck_plate     VARCHAR(20)     NOT NULL,               -- biển số xe
    status          VARCHAR(30)     NOT NULL DEFAULT 'scheduled'
                    CHECK (status IN ('scheduled', 'in_progress', 'completed', 'cancelled')),
    issue_summary   TEXT            NOT NULL,                -- mô tả lỗi / yêu cầu sửa
    scheduled_at    TIMESTAMP       NOT NULL,                -- ngày hẹn
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    labor_cost      DECIMAL(15, 2)  NOT NULL DEFAULT 0,     -- phí công
    total_parts_cost DECIMAL(15, 2) NOT NULL DEFAULT 0,     -- tổng giá linh kiện
    total_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0,     -- tổng cộng
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- Chi tiết linh kiện sử dụng trong work order
CREATE TABLE public.work_order_items (
    id              SERIAL PRIMARY KEY,
    work_order_id   INTEGER         NOT NULL REFERENCES public.work_orders(id),
    component_id    INTEGER         NOT NULL REFERENCES public.components(id),
    quantity        INTEGER         NOT NULL DEFAULT 1,
    unit_price      DECIMAL(15, 2)  NOT NULL,               -- giá tại thời điểm sử dụng
    cost_price      DECIMAL(15, 2)  NOT NULL,               -- giá vốn tại thời điểm
    subtotal        DECIMAL(15, 2)  NOT NULL                 -- quantity * unit_price
);

-- Hóa đơn
CREATE TABLE public.invoices (
    id              SERIAL PRIMARY KEY,
    work_order_id   INTEGER         NOT NULL REFERENCES public.work_orders(id),
    customer_id     INTEGER         NOT NULL REFERENCES public.customers(id),
    invoice_number  VARCHAR(50)     NOT NULL UNIQUE,         -- HD-2024-000001
    invoice_date    DATE            NOT NULL DEFAULT CURRENT_DATE,
    due_date        DATE,
    service_amount  DECIMAL(15, 2)  NOT NULL DEFAULT 0,     -- doanh thu dịch vụ (labor)
    parts_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0,     -- doanh thu bán linh kiện
    tax_amount      DECIMAL(15, 2)  NOT NULL DEFAULT 0,     -- VAT
    total_amount    DECIMAL(15, 2)  NOT NULL DEFAULT 0,
    status          VARCHAR(20)     NOT NULL DEFAULT 'issued'
                    CHECK (status IN ('issued', 'paid', 'overdue', 'cancelled')),
    payment_date    TIMESTAMP,
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- =============================================================
-- 4. Indexes
-- =============================================================

CREATE INDEX idx_parts_inventory_head    ON public.parts_inventory(head_id);
CREATE INDEX idx_work_orders_customer    ON public.work_orders(customer_id);
CREATE INDEX idx_work_orders_head        ON public.work_orders(head_id);
CREATE INDEX idx_work_orders_status      ON public.work_orders(status);
CREATE INDEX idx_work_orders_scheduled   ON public.work_orders(scheduled_at);
CREATE INDEX idx_invoices_customer       ON public.invoices(customer_id);
CREATE INDEX idx_invoices_date           ON public.invoices(invoice_date);
CREATE INDEX idx_invoices_status         ON public.invoices(status);

-- =============================================================
-- 5. REPLICA IDENTITY FULL cho CDC
-- =============================================================

ALTER TABLE public.heads              REPLICA IDENTITY FULL;
ALTER TABLE public.customers          REPLICA IDENTITY FULL;
ALTER TABLE public.components         REPLICA IDENTITY FULL;
ALTER TABLE public.parts_inventory    REPLICA IDENTITY FULL;
ALTER TABLE public.work_orders        REPLICA IDENTITY FULL;
ALTER TABLE public.work_order_items   REPLICA IDENTITY FULL;
ALTER TABLE public.invoices           REPLICA IDENTITY FULL;

-- =============================================================
-- 6. Updated_at trigger (tự cập nhật updated_at khi UPDATE)
-- =============================================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_heads_updated_at
    BEFORE UPDATE ON public.heads
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON public.customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_parts_inventory_updated_at
    BEFORE UPDATE ON public.parts_inventory
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_work_orders_updated_at
    BEFORE UPDATE ON public.work_orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_invoices_updated_at
    BEFORE UPDATE ON public.invoices
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
