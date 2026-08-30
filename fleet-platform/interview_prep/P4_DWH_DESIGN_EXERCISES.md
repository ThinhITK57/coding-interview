# 📐 Pillar 4 — DWH Design Exercises (Days 6–7)

> **Mục tiêu**: Thành thạo thiết kế Data Warehouse theo phương pháp Kimball — từ phân tích yêu cầu nghiệp vụ → xác định Grain → thiết kế Star Schema → viết DDL → truy vấn BI.

---

## 📚 Lý thuyết nền tảng (Compact)

### 1. Ba loại Fact Table

| Loại | Mô tả | Grain | Ví dụ |
|---|---|---|---|
| **Transaction Fact** | Mỗi dòng = 1 sự kiện/giao dịch riêng lẻ. Ghi nhận tại thời điểm xảy ra. | 1 event | `fact_invoices` — mỗi hóa đơn 1 dòng |
| **Periodic Snapshot** | Mỗi dòng = tổng hợp trạng thái cuối kỳ (ngày/tuần/tháng). Đo lường tích lũy. | 1 period per entity | `fact_monthly_inventory` — tồn kho cuối tháng |
| **Accumulating Snapshot** | 1 dòng = 1 process xuyên suốt vòng đời, cập nhật liên tục qua các milestones. | 1 lifecycle per entity | `fact_order_lifecycle` — 1 đơn hàng từ tạo → giao → thanh toán |

### 2. Additive Properties của Measures

| Loại | SUM across Time? | SUM across Non-Time Dims? | Ví dụ |
|---|---|---|---|
| **Additive** | ✅ | ✅ | `revenue`, `quantity_sold` |
| **Semi-Additive** | ❌ | ✅ | `inventory_balance`, `account_balance` |
| **Non-Additive** | ❌ | ❌ | `margin_percent`, `unit_price`, `exchange_rate` |

> **Tại sao Semi-Additive không SUM theo thời gian?** Vì balance ngày 1 + balance ngày 2 ≠ balance ngày 1→2. Ta phải dùng `AVG`, `LAST_VALUE`, hoặc `MAX` theo thời gian.

### 3. Slowly Changing Dimensions (SCD)

| Type | Cơ chế | Ưu điểm | Nhược điểm |
|---|---|---|---|
| **SCD Type 1** | Overwrite giá trị cũ | Đơn giản, tiết kiệm storage | Mất lịch sử |
| **SCD Type 2** | Thêm row mới + `effective_date` / `expiration_date` / `is_current` | Giữ toàn bộ lịch sử | Table phình to, JOIN phức tạp |
| **SCD Type 3** | Thêm cột `previous_value` | Truy vấn nhanh giá trị trước | Chỉ giữ được 1 version trước |
| **SCD Type 6** | Hybrid 1+2+3: row mới + cột `current_value` trên mọi row | Linh hoạt nhất | Phức tạp nhất |

**SCD Type 2 — Flow chi tiết:**
```
1. Record mới đến với giá trị thay đổi
2. UPDATE row hiện tại: SET is_current = FALSE, expiration_date = CURRENT_DATE - 1
3. INSERT row mới: effective_date = CURRENT_DATE, expiration_date = '9999-12-31', is_current = TRUE
4. Surrogate key mới (MD5 hoặc auto-increment)
```

### 4. Conformed Dimension

- **Định nghĩa**: Dimension được chia sẻ (shared) giữa nhiều Fact tables trong DWH.
- **Ví dụ**: `dim_date`, `dim_customer`, `dim_product` — dùng chung cho `fact_sales`, `fact_returns`, `fact_inventory`.
- **Lợi ích**: Đảm bảo consistency khi drill-across (so sánh metrics từ nhiều fact tables cùng 1 dimension).

### 5. Grain

- **Định nghĩa**: Mức chi tiết nhất (finest level of detail) mà mỗi row trong Fact table đại diện.
- **Quy tắc vàng**: Xác định Grain TRƯỚC KHI thiết kế bất kỳ thứ gì khác.
- **Ví dụ**: `fact_invoice_line` → grain = "1 dòng hóa đơn cho 1 dịch vụ/phụ tùng" vs `fact_invoice` → grain = "1 hóa đơn tổng hợp".

---

## 🚛 Đề 1 — Fleet Management Domain

### 📋 Input OLTP Schema

```sql
-- ========================================
-- SOURCE OLTP TABLES (PostgreSQL)
-- ========================================

CREATE TABLE customers (
    id              SERIAL PRIMARY KEY,
    company_name    VARCHAR(200) NOT NULL,
    status          VARCHAR(20) DEFAULT 'new',       -- new → regular → annual
    fleet_size      INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE heads (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,            -- Tên trạm sửa chữa
    lat             DECIMAL(10, 7),
    lng             DECIMAL(10, 7),
    capacity        INTEGER DEFAULT 10                -- Số bay sửa chữa
);

CREATE TABLE components (
    id              SERIAL PRIMARY KEY,
    code            VARCHAR(50) UNIQUE NOT NULL,       -- e.g. 'BRK-PAD-001'
    category        VARCHAR(50),                       -- 'brake', 'engine', 'tire'
    unit_cost       DECIMAL(12, 2)
);

CREATE TABLE work_orders (
    id              SERIAL PRIMARY KEY,
    head_id         INTEGER REFERENCES heads(id),
    customer_id     INTEGER REFERENCES customers(id),
    truck_plate     VARCHAR(20) NOT NULL,
    status          VARCHAR(20) DEFAULT 'open',       -- open → in_progress → completed → invoiced
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE invoices (
    id              SERIAL PRIMARY KEY,
    work_order_id   INTEGER REFERENCES work_orders(id),
    customer_id     INTEGER REFERENCES customers(id),
    invoice_date    DATE NOT NULL,
    service_amount  DECIMAL(12, 2) DEFAULT 0,
    parts_amount    DECIMAL(12, 2) DEFAULT 0,
    total_amount    DECIMAL(12, 2) DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'draft'       -- draft → sent → paid → overdue
);

CREATE TABLE parts_inventory (
    head_id         INTEGER REFERENCES heads(id),
    component_id    INTEGER REFERENCES components(id),
    quantity        INTEGER DEFAULT 0,
    unit_price      DECIMAL(12, 2),
    last_restock_at TIMESTAMP,
    PRIMARY KEY (head_id, component_id)
);
```

### 🎯 Tiêu chí nghiệp vụ

| Ký hiệu | Yêu cầu | Độ khó |
|---|---|---|
| **(A)** | Báo cáo doanh thu theo **Năm Tài Khóa** (Fiscal Year: 01/10 → 30/09), theo trạm, theo quý | ⭐⭐ |
| **(B)** | **SCD Type 2** tracking lịch sử trạng thái khách hàng: `new` → `regular` → `annual` | ⭐⭐⭐ |
| **(C)** | Dashboard **real-time low-latency** cho quản lý xem tổng quan doanh thu ngày | ⭐⭐ |

### ✅ Đáp án chi tiết

#### Bước 1: Xác định Grain

| Fact Table | Grain | Giải thích |
|---|---|---|
| `fact_invoices` | 1 row = 1 hóa đơn | Transaction Fact — mỗi hóa đơn phát sinh 1 dòng |
| `fact_daily_revenue` | 1 row = doanh thu 1 trạm trong 1 ngày | Periodic Snapshot — tổng hợp cuối ngày |

#### Bước 2: DDL Star Schema

```sql
-- ========================================
-- DIM_DATE — Conformed Dimension
-- Đặc biệt: Fiscal Year logic (01/10 → 30/09)
-- ========================================

CREATE TABLE dim_date (
    date_key            INTEGER PRIMARY KEY,          -- YYYYMMDD, e.g. 20250115
    full_date           DATE NOT NULL UNIQUE,
    day_of_week         SMALLINT,                     -- 1=Mon..7=Sun
    day_name            VARCHAR(10),                  -- 'Monday'
    day_of_month        SMALLINT,
    week_of_year        SMALLINT,
    month_number        SMALLINT,
    month_name          VARCHAR(10),
    calendar_quarter    SMALLINT,                     -- Q1=1..Q4=4 (Jan-based)
    calendar_year       SMALLINT,

    -- *** FISCAL YEAR LOGIC: 01/10 → 30/09 ***
    fiscal_year         SMALLINT GENERATED ALWAYS AS (
                            CASE WHEN EXTRACT(MONTH FROM full_date) >= 10
                                 THEN EXTRACT(YEAR FROM full_date) + 1
                                 ELSE EXTRACT(YEAR FROM full_date)
                            END
                        ) STORED,
    fiscal_quarter      SMALLINT GENERATED ALWAYS AS (
                            CASE
                                WHEN EXTRACT(MONTH FROM full_date) IN (10,11,12) THEN 1
                                WHEN EXTRACT(MONTH FROM full_date) IN (1,2,3)    THEN 2
                                WHEN EXTRACT(MONTH FROM full_date) IN (4,5,6)    THEN 3
                                WHEN EXTRACT(MONTH FROM full_date) IN (7,8,9)    THEN 4
                            END
                        ) STORED,
    fiscal_month        SMALLINT GENERATED ALWAYS AS (
                            CASE WHEN EXTRACT(MONTH FROM full_date) >= 10
                                 THEN EXTRACT(MONTH FROM full_date) - 9
                                 ELSE EXTRACT(MONTH FROM full_date) + 3
                            END
                        ) STORED,

    is_weekend          BOOLEAN,
    is_holiday          BOOLEAN DEFAULT FALSE
);

-- Populate dim_date (generate 10 years)
INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, day_of_month,
                      week_of_year, month_number, month_name, calendar_quarter,
                      calendar_year, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER          AS date_key,
    d                                          AS full_date,
    EXTRACT(ISODOW FROM d)                     AS day_of_week,
    TO_CHAR(d, 'Day')                          AS day_name,
    EXTRACT(DAY FROM d)                        AS day_of_month,
    EXTRACT(WEEK FROM d)                       AS week_of_year,
    EXTRACT(MONTH FROM d)                      AS month_number,
    TO_CHAR(d, 'Month')                        AS month_name,
    EXTRACT(QUARTER FROM d)                    AS calendar_quarter,
    EXTRACT(YEAR FROM d)                       AS calendar_year,
    EXTRACT(ISODOW FROM d) IN (6,7)            AS is_weekend
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day') AS d;


-- ========================================
-- DIM_CUSTOMER — SCD Type 2
-- Surrogate Key = MD5(business_key + effective_date)
-- ========================================

CREATE TABLE dim_customer (
    customer_sk         VARCHAR(32) PRIMARY KEY,      -- MD5 surrogate key
    customer_bk         INTEGER NOT NULL,              -- Business Key = customers.id
    company_name        VARCHAR(200) NOT NULL,
    status              VARCHAR(20) NOT NULL,           -- 'new', 'regular', 'annual'
    fleet_size          INTEGER,

    -- SCD2 metadata
    effective_date      DATE NOT NULL,
    expiration_date     DATE NOT NULL DEFAULT '9999-12-31',
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,

    -- Audit
    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);

-- Surrogate key generation:
-- customer_sk = MD5(customer_bk::TEXT || '|' || effective_date::TEXT)

CREATE INDEX idx_dim_customer_bk ON dim_customer(customer_bk);
CREATE INDEX idx_dim_customer_current ON dim_customer(customer_bk, is_current)
    WHERE is_current = TRUE;


-- ========================================
-- DIM_HEAD (Trạm sửa chữa) — SCD Type 1
-- ========================================

CREATE TABLE dim_head (
    head_sk             INTEGER PRIMARY KEY,           -- = heads.id (Type 1 nên dùng natural key)
    head_name           VARCHAR(100) NOT NULL,
    latitude            DECIMAL(10, 7),
    longitude           DECIMAL(10, 7),
    capacity            INTEGER,
    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);


-- ========================================
-- DIM_INVOICE_STATUS — Junk Dimension / Mini Dimension
-- ========================================

CREATE TABLE dim_invoice_status (
    status_sk           SERIAL PRIMARY KEY,
    status_code         VARCHAR(20) UNIQUE NOT NULL,   -- 'draft', 'sent', 'paid', 'overdue'
    status_label        VARCHAR(50),
    is_revenue_realized BOOLEAN                        -- TRUE chỉ khi 'paid'
);


-- ========================================
-- FACT_INVOICES — Transaction Fact Table
-- Grain: 1 row = 1 hóa đơn
-- ========================================

CREATE TABLE fact_invoices (
    invoice_id          INTEGER NOT NULL,               -- Degenerate dimension
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_key),
    customer_sk         VARCHAR(32) NOT NULL REFERENCES dim_customer(customer_sk),
    head_sk             INTEGER NOT NULL REFERENCES dim_head(head_sk),
    status_sk           INTEGER NOT NULL REFERENCES dim_invoice_status(status_sk),

    -- Measures (all Additive)
    service_amount      DECIMAL(12, 2) NOT NULL DEFAULT 0,
    parts_amount        DECIMAL(12, 2) NOT NULL DEFAULT 0,
    total_amount        DECIMAL(12, 2) NOT NULL DEFAULT 0,

    -- Audit
    etl_batch_id        BIGINT,
    etl_loaded_at       TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (invoice_id)
);

CREATE INDEX idx_fact_invoices_date ON fact_invoices(date_key);
CREATE INDEX idx_fact_invoices_customer ON fact_invoices(customer_sk);
CREATE INDEX idx_fact_invoices_head ON fact_invoices(head_sk);


-- ========================================
-- FACT_DAILY_REVENUE — Periodic Snapshot Fact Table
-- Grain: 1 row = 1 trạm × 1 ngày
-- ========================================

CREATE TABLE fact_daily_revenue (
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_key),
    head_sk             INTEGER NOT NULL REFERENCES dim_head(head_sk),

    -- Measures
    total_invoices      INTEGER DEFAULT 0,              -- Additive
    total_service_amt   DECIMAL(14, 2) DEFAULT 0,       -- Additive
    total_parts_amt     DECIMAL(14, 2) DEFAULT 0,       -- Additive
    total_revenue       DECIMAL(14, 2) DEFAULT 0,       -- Additive
    avg_invoice_value   DECIMAL(12, 2) DEFAULT 0,       -- Non-Additive (derived)

    etl_loaded_at       TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (date_key, head_sk)
);
```

#### Bước 3: Sample Data

```sql
-- ========================================
-- INSERT SAMPLE DATA
-- ========================================

-- dim_invoice_status
INSERT INTO dim_invoice_status (status_code, status_label, is_revenue_realized) VALUES
    ('draft',   'Nháp',           FALSE),
    ('sent',    'Đã gửi',        FALSE),
    ('paid',    'Đã thanh toán', TRUE),
    ('overdue', 'Quá hạn',       FALSE);

-- dim_head
INSERT INTO dim_head (head_sk, head_name, latitude, longitude, capacity) VALUES
    (1, 'Trạm Bình Dương',    11.0543172, 106.6777024, 12),
    (2, 'Trạm Đồng Nai',      10.9574128, 106.8426871, 8),
    (3, 'Trạm Long An',       10.5356006, 106.4063618, 10);

-- dim_customer (SCD2 — customer 101 đã thay đổi status 2 lần)
INSERT INTO dim_customer (customer_sk, customer_bk, company_name, status, fleet_size,
                          effective_date, expiration_date, is_current) VALUES
    -- Customer 101: new → regular → annual
    (MD5('101|2024-01-15'), 101, 'Vận tải Miền Nam',   'new',     25,
     '2024-01-15', '2024-06-30', FALSE),
    (MD5('101|2024-07-01'), 101, 'Vận tải Miền Nam',   'regular', 30,
     '2024-07-01', '2025-03-31', FALSE),
    (MD5('101|2025-04-01'), 101, 'Vận tải Miền Nam',   'annual',  35,
     '2025-04-01', '9999-12-31', TRUE),

    -- Customer 102: new → regular
    (MD5('102|2024-03-01'), 102, 'Logistics Sài Gòn',  'new',     50,
     '2024-03-01', '2025-01-14', FALSE),
    (MD5('102|2025-01-15'), 102, 'Logistics Sài Gòn',  'regular', 55,
     '2025-01-15', '9999-12-31', TRUE),

    -- Customer 103: still new
    (MD5('103|2025-02-01'), 103, 'Express Mekong',     'new',     10,
     '2025-02-01', '9999-12-31', TRUE);

-- fact_invoices
INSERT INTO fact_invoices (invoice_id, date_key, customer_sk, head_sk, status_sk,
                           service_amount, parts_amount, total_amount) VALUES
    (1001, 20241015, MD5('101|2024-07-01'), 1, 3, 5000000, 3200000, 8200000),
    (1002, 20241102, MD5('101|2024-07-01'), 1, 3, 2500000, 1800000, 4300000),
    (1003, 20250115, MD5('102|2025-01-15'), 2, 3, 7500000, 4100000, 11600000),
    (1004, 20250410, MD5('101|2025-04-01'), 1, 3, 3000000, 2500000, 5500000),
    (1005, 20250410, MD5('103|2025-02-01'), 3, 2, 1500000, 900000,  2400000),
    (1006, 20250612, MD5('102|2025-01-15'), 2, 3, 6000000, 3800000, 9800000),
    (1007, 20250815, MD5('101|2025-04-01'), 1, 4, 4200000, 2700000, 6900000);

-- fact_daily_revenue (aggregated)
INSERT INTO fact_daily_revenue (date_key, head_sk, total_invoices, total_service_amt,
                                total_parts_amt, total_revenue, avg_invoice_value) VALUES
    (20241015, 1, 1, 5000000, 3200000, 8200000, 8200000),
    (20241102, 1, 1, 2500000, 1800000, 4300000, 4300000),
    (20250115, 2, 1, 7500000, 4100000, 11600000, 11600000),
    (20250410, 1, 1, 3000000, 2500000, 5500000, 5500000),
    (20250410, 3, 1, 1500000, 900000,  2400000, 2400000),
    (20250612, 2, 1, 6000000, 3800000, 9800000, 9800000),
    (20250815, 1, 1, 4200000, 2700000, 6900000, 6900000);
```

#### Bước 4: Materialized View cho Dashboard Real-time

```sql
-- ========================================
-- MATERIALIZED VIEW — Dashboard Low-Latency
-- Yêu cầu (C): Quản lý xem tổng quan doanh thu ngày
-- ========================================

CREATE MATERIALIZED VIEW mv_dashboard_daily_revenue AS
SELECT
    d.full_date,
    d.fiscal_year,
    d.fiscal_quarter,
    h.head_name                            AS station_name,
    COUNT(f.invoice_id)                    AS invoice_count,
    SUM(f.service_amount)                  AS total_service,
    SUM(f.parts_amount)                    AS total_parts,
    SUM(f.total_amount)                    AS total_revenue,
    ROUND(AVG(f.total_amount), 0)          AS avg_invoice_value,
    SUM(f.total_amount) FILTER (
        WHERE s.is_revenue_realized = TRUE
    )                                       AS realized_revenue
FROM fact_invoices f
JOIN dim_date d             ON f.date_key   = d.date_key
JOIN dim_head h             ON f.head_sk    = h.head_sk
JOIN dim_invoice_status s   ON f.status_sk  = s.status_sk
GROUP BY d.full_date, d.fiscal_year, d.fiscal_quarter, h.head_name
ORDER BY d.full_date DESC, h.head_name;

-- Index trên Materialized View
CREATE UNIQUE INDEX idx_mv_dashboard_date_station
    ON mv_dashboard_daily_revenue(full_date, station_name);

-- Refresh strategy: mỗi 5 phút hoặc sau mỗi ETL batch
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_dashboard_daily_revenue;
-- "CONCURRENTLY" cho phép query trong khi refresh (cần UNIQUE INDEX)
```

#### Bước 5: Sample BI Queries

```sql
-- ========================================
-- QUERY (A): Doanh thu theo Năm Tài Khóa, theo trạm, theo quý
-- Fiscal Year 01/10 → 30/09
-- ========================================

SELECT
    d.fiscal_year,
    d.fiscal_quarter,
    'FY' || d.fiscal_year || '-Q' || d.fiscal_quarter   AS fiscal_period,
    h.head_name                                           AS station_name,
    COUNT(f.invoice_id)                                   AS total_invoices,
    SUM(f.service_amount)                                 AS service_revenue,
    SUM(f.parts_amount)                                   AS parts_revenue,
    SUM(f.total_amount)                                   AS total_revenue,
    ROUND(SUM(f.total_amount) * 100.0 /
          SUM(SUM(f.total_amount)) OVER (
              PARTITION BY d.fiscal_year
          ), 2)                                           AS pct_of_fiscal_year
FROM fact_invoices f
JOIN dim_date d           ON f.date_key  = d.date_key
JOIN dim_head h           ON f.head_sk   = h.head_sk
JOIN dim_invoice_status s ON f.status_sk = s.status_sk
WHERE s.is_revenue_realized = TRUE          -- Chỉ tính doanh thu đã thanh toán
GROUP BY d.fiscal_year, d.fiscal_quarter, h.head_name
ORDER BY d.fiscal_year, d.fiscal_quarter, h.head_name;

/*
Kết quả mong đợi:
fiscal_year | fiscal_quarter | fiscal_period | station_name     | total_invoices | total_revenue
2025        | 1              | FY2025-Q1     | Trạm Bình Dương  | 2              | 12,500,000
2025        | 2              | FY2025-Q2     | Trạm Đồng Nai    | 1              | 11,600,000
2025        | 3              | FY2025-Q3     | Trạm Bình Dương  | 1              | 5,500,000
2025        | 3              | FY2025-Q3     | Trạm Đồng Nai    | 1              | 9,800,000
*/


-- ========================================
-- QUERY (B): Lịch sử trạng thái khách hàng (SCD2 tracking)
-- Xem timeline thay đổi status của 1 khách hàng
-- ========================================

-- B1: Timeline đầy đủ của 1 khách hàng
SELECT
    customer_bk,
    company_name,
    status,
    fleet_size,
    effective_date,
    expiration_date,
    is_current,
    CASE
        WHEN expiration_date = '9999-12-31'
        THEN 'Hiện tại'
        ELSE (expiration_date - effective_date + 1) || ' ngày'
    END AS duration
FROM dim_customer
WHERE customer_bk = 101
ORDER BY effective_date;

/*
Kết quả:
customer_bk | company_name       | status  | fleet_size | effective_date | expiration_date | is_current | duration
101         | Vận tải Miền Nam   | new     | 25         | 2024-01-15     | 2024-06-30      | FALSE      | 168 ngày
101         | Vận tải Miền Nam   | regular | 30         | 2024-07-01     | 2025-03-31      | FALSE      | 275 ngày
101         | Vận tải Miền Nam   | annual  | 35         | 2025-04-01     | 9999-12-31      | TRUE       | Hiện tại
*/


-- B2: Doanh thu theo trạng thái khách hàng TẠI THỜI ĐIỂM phát sinh hóa đơn
-- (Point-in-time join — sức mạnh chính của SCD2)
SELECT
    c.status                    AS customer_status_at_invoice_time,
    COUNT(f.invoice_id)         AS total_invoices,
    SUM(f.total_amount)         AS total_revenue,
    ROUND(AVG(f.total_amount), 0) AS avg_invoice_value
FROM fact_invoices f
JOIN dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY c.status
ORDER BY total_revenue DESC;


-- B3: SCD2 ETL — Logic INSERT/UPDATE khi status thay đổi
-- (Đây là logic ETL, không phải BI query)
DO $$
DECLARE
    v_customer_bk   INTEGER := 101;
    v_new_status    VARCHAR := 'vip';
    v_new_fleet     INTEGER := 40;
    v_today         DATE := CURRENT_DATE;
BEGIN
    -- Step 1: Expire current record
    UPDATE dim_customer
    SET expiration_date = v_today - INTERVAL '1 day',
        is_current = FALSE
    WHERE customer_bk = v_customer_bk
      AND is_current = TRUE;

    -- Step 2: Insert new version
    INSERT INTO dim_customer (customer_sk, customer_bk, company_name, status, fleet_size,
                              effective_date, expiration_date, is_current)
    SELECT
        MD5(v_customer_bk::TEXT || '|' || v_today::TEXT),
        v_customer_bk,
        company_name,       -- giữ nguyên company_name
        v_new_status,
        v_new_fleet,
        v_today,
        '9999-12-31',
        TRUE
    FROM dim_customer
    WHERE customer_bk = v_customer_bk
      AND expiration_date = v_today - INTERVAL '1 day'
    LIMIT 1;
END $$;


-- ========================================
-- QUERY (C): Dashboard real-time từ Materialized View
-- ========================================

-- C1: Doanh thu hôm nay vs hôm qua
SELECT
    full_date,
    station_name,
    invoice_count,
    total_revenue,
    LAG(total_revenue) OVER (
        PARTITION BY station_name ORDER BY full_date
    ) AS prev_day_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (
            PARTITION BY station_name ORDER BY full_date
        )) * 100.0 / NULLIF(LAG(total_revenue) OVER (
            PARTITION BY station_name ORDER BY full_date
        ), 0), 2
    ) AS growth_pct
FROM mv_dashboard_daily_revenue
WHERE full_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY full_date DESC, station_name;

-- C2: Top trạm theo doanh thu tháng hiện tại
SELECT
    station_name,
    SUM(total_revenue)      AS mtd_revenue,
    SUM(invoice_count)      AS mtd_invoices,
    RANK() OVER (ORDER BY SUM(total_revenue) DESC) AS rank
FROM mv_dashboard_daily_revenue
WHERE full_date >= DATE_TRUNC('month', CURRENT_DATE)
GROUP BY station_name
ORDER BY mtd_revenue DESC;
```

---

## 🛒 Đề 2 — E-commerce Domain

### 📋 Input OLTP Schema

```sql
-- ========================================
-- SOURCE OLTP TABLES
-- ========================================

CREATE TABLE customers (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(200) NOT NULL,
    email           VARCHAR(200) UNIQUE,
    region          VARCHAR(50),                       -- 'North', 'South', 'Central'
    signup_date     DATE NOT NULL
);

CREATE TABLE products (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(200) NOT NULL,
    category        VARCHAR(100),                      -- 'Electronics', 'Fashion', 'Food'
    brand           VARCHAR(100)
);

CREATE TABLE orders (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES customers(id),
    order_date      TIMESTAMP NOT NULL,
    total           DECIMAL(14, 2),
    status          VARCHAR(20) DEFAULT 'pending'      -- pending → confirmed → shipped → delivered → returned
);

CREATE TABLE order_items (
    id              SERIAL PRIMARY KEY,
    order_id        INTEGER REFERENCES orders(id),
    product_id      INTEGER REFERENCES products(id),
    quantity        INTEGER NOT NULL,
    price           DECIMAL(12, 2) NOT NULL
);
```

### 🎯 Tiêu chí nghiệp vụ

| Ký hiệu | Yêu cầu | Độ khó |
|---|---|---|
| **(A)** | **Basket Analysis**: Tìm các sản phẩm thường được mua cùng nhau | ⭐⭐⭐ |
| **(B)** | **Customer Lifetime Value (CLTV)** theo cohort (tháng đăng ký) | ⭐⭐⭐ |
| **(C)** | Xử lý **Late-arriving Orders** (đơn hàng đến muộn hơn expected) | ⭐⭐ |

### ✅ Đáp án chi tiết

#### Bước 1: Xác định Grain

| Fact Table | Grain | Loại |
|---|---|---|
| `fact_order_items` | 1 row = 1 sản phẩm trong 1 đơn hàng | Transaction Fact |
| `fact_order_lifecycle` | 1 row = 1 đơn hàng xuyên suốt vòng đời | Accumulating Snapshot |

#### Bước 2: DDL Star Schema

```sql
-- ========================================
-- DIM_DATE — Conformed (dùng chung với Đề 1)
-- (Giả sử dim_date đã tồn tại từ Đề 1)
-- ========================================


-- ========================================
-- DIM_CUSTOMER_ECOM
-- ========================================

CREATE TABLE dim_customer_ecom (
    customer_sk         SERIAL PRIMARY KEY,
    customer_bk         INTEGER NOT NULL,              -- customers.id
    customer_name       VARCHAR(200) NOT NULL,
    email               VARCHAR(200),
    region              VARCHAR(50),
    signup_date         DATE NOT NULL,

    -- Derived attributes cho Cohort Analysis
    signup_year_month   VARCHAR(7) GENERATED ALWAYS AS (
                            TO_CHAR(signup_date, 'YYYY-MM')
                        ) STORED,
    signup_quarter      VARCHAR(7) GENERATED ALWAYS AS (
                            EXTRACT(YEAR FROM signup_date)::TEXT || '-Q' ||
                            EXTRACT(QUARTER FROM signup_date)::TEXT
                        ) STORED,

    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_dim_customer_ecom_cohort ON dim_customer_ecom(signup_year_month);


-- ========================================
-- DIM_PRODUCT
-- ========================================

CREATE TABLE dim_product (
    product_sk          SERIAL PRIMARY KEY,
    product_bk          INTEGER NOT NULL,               -- products.id
    product_name        VARCHAR(200) NOT NULL,
    category            VARCHAR(100),
    brand               VARCHAR(100),
    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);


-- ========================================
-- FACT_ORDER_ITEMS — Transaction Fact
-- Grain: 1 row = 1 product trong 1 order
-- ========================================

CREATE TABLE fact_order_items (
    order_item_id       INTEGER NOT NULL,               -- Degenerate dimension
    order_id            INTEGER NOT NULL,               -- Degenerate dimension (dùng cho basket analysis)
    order_date_key      INTEGER NOT NULL REFERENCES dim_date(date_key),
    customer_sk         INTEGER NOT NULL REFERENCES dim_customer_ecom(customer_sk),
    product_sk          INTEGER NOT NULL REFERENCES dim_product(product_sk),

    -- Measures
    quantity            INTEGER NOT NULL,                -- Additive
    unit_price          DECIMAL(12, 2) NOT NULL,         -- Non-Additive
    line_total          DECIMAL(14, 2) NOT NULL,          -- Additive (quantity × unit_price)

    -- Late-arriving metadata
    event_timestamp     TIMESTAMP NOT NULL,              -- Thời điểm order thực sự xảy ra
    etl_loaded_at       TIMESTAMP DEFAULT NOW(),         -- Thời điểm ETL load
    is_late_arriving    BOOLEAN DEFAULT FALSE,           -- Flag cho late data

    PRIMARY KEY (order_item_id)
);

CREATE INDEX idx_fact_oi_order ON fact_order_items(order_id);
CREATE INDEX idx_fact_oi_date ON fact_order_items(order_date_key);
CREATE INDEX idx_fact_oi_product ON fact_order_items(product_sk);


-- ========================================
-- FACT_ORDER_LIFECYCLE — Accumulating Snapshot
-- Grain: 1 row = 1 đơn hàng, cập nhật qua các milestones
-- ========================================

CREATE TABLE fact_order_lifecycle (
    order_id            INTEGER PRIMARY KEY,
    customer_sk         INTEGER NOT NULL REFERENCES dim_customer_ecom(customer_sk),

    -- Milestone date keys (NULL nếu chưa đến milestone đó)
    order_date_key      INTEGER REFERENCES dim_date(date_key),
    confirm_date_key    INTEGER REFERENCES dim_date(date_key),
    ship_date_key       INTEGER REFERENCES dim_date(date_key),
    deliver_date_key    INTEGER REFERENCES dim_date(date_key),
    return_date_key     INTEGER REFERENCES dim_date(date_key),       -- NULL nếu không trả hàng

    -- Measures
    order_total         DECIMAL(14, 2),                  -- Additive
    item_count          INTEGER,                         -- Additive
    current_status      VARCHAR(20),                     -- Degenerate dimension

    -- Derived lag measures (days)
    days_to_confirm     INTEGER GENERATED ALWAYS AS (
                            confirm_date_key - order_date_key
                        ) STORED,
    days_to_ship        INTEGER GENERATED ALWAYS AS (
                            ship_date_key - confirm_date_key
                        ) STORED,
    days_to_deliver     INTEGER GENERATED ALWAYS AS (
                            deliver_date_key - ship_date_key
                        ) STORED,

    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);
```

#### Bước 3: Sample Data

```sql
-- ========================================
-- INSERT SAMPLE DATA
-- ========================================

-- dim_customer_ecom
INSERT INTO dim_customer_ecom (customer_bk, customer_name, email, region, signup_date) VALUES
    (201, 'Nguyễn Văn Anh',   'anh@email.com',    'South',   '2024-01-15'),
    (202, 'Trần Thị Bình',    'binh@email.com',   'North',   '2024-01-20'),
    (203, 'Lê Minh Châu',     'chau@email.com',   'Central', '2024-03-05'),
    (204, 'Phạm Đức Dũng',    'dung@email.com',   'South',   '2024-03-10'),
    (205, 'Hoàng Thị Lan',    'lan@email.com',    'North',   '2024-06-01');

-- dim_product
INSERT INTO dim_product (product_bk, product_name, category, brand) VALUES
    (301, 'iPhone 15 Pro',          'Electronics', 'Apple'),
    (302, 'AirPods Pro 2',         'Electronics', 'Apple'),
    (303, 'Ốp lưng MagSafe',      'Accessories', 'Apple'),
    (304, 'Áo thun nam basic',     'Fashion',     'Uniqlo'),
    (305, 'Quần jean slim fit',    'Fashion',     'Levis'),
    (306, 'Cà phê Trung Nguyên',  'Food',        'Trung Nguyên');

-- fact_order_items (lưu ý: nhiều items cùng order_id = 1 basket)
INSERT INTO fact_order_items (order_item_id, order_id, order_date_key, customer_sk,
                              product_sk, quantity, unit_price, line_total,
                              event_timestamp, is_late_arriving) VALUES
    -- Order 5001: Anh mua iPhone + AirPods + Ốp lưng (basket 3 items)
    (1, 5001, 20240215, 1, 1, 1, 28990000, 28990000, '2024-02-15 10:30:00', FALSE),
    (2, 5001, 20240215, 1, 2, 1, 5990000,  5990000,  '2024-02-15 10:30:00', FALSE),
    (3, 5001, 20240215, 1, 3, 2, 490000,   980000,   '2024-02-15 10:30:00', FALSE),

    -- Order 5002: Bình mua iPhone + AirPods
    (4, 5002, 20240301, 2, 1, 1, 28990000, 28990000, '2024-03-01 14:00:00', FALSE),
    (5, 5002, 20240301, 2, 2, 1, 5990000,  5990000,  '2024-03-01 14:00:00', FALSE),

    -- Order 5003: Châu mua Fashion items
    (6, 5003, 20240410, 3, 4, 3, 399000,   1197000,  '2024-04-10 09:15:00', FALSE),
    (7, 5003, 20240410, 3, 5, 2, 1290000,  2580000,  '2024-04-10 09:15:00', FALSE),

    -- Order 5004: Anh mua lại AirPods + Ốp lưng (repeat purchase)
    (8, 5004, 20240601, 1, 2, 1, 5990000,  5990000,  '2024-06-01 11:00:00', FALSE),
    (9, 5004, 20240601, 1, 3, 1, 490000,   490000,   '2024-06-01 11:00:00', FALSE),

    -- Order 5005: LATE-ARRIVING — Dũng đặt hàng ngày 10/05 nhưng data đến muộn
    (10, 5005, 20240510, 4, 6, 5, 89000,   445000,   '2024-05-10 16:00:00', TRUE);


-- fact_order_lifecycle (Accumulating Snapshot)
INSERT INTO fact_order_lifecycle (order_id, customer_sk, order_date_key, confirm_date_key,
                                  ship_date_key, deliver_date_key, return_date_key,
                                  order_total, item_count, current_status) VALUES
    (5001, 1, 20240215, 20240215, 20240216, 20240220, NULL,      35960000, 3, 'delivered'),
    (5002, 2, 20240301, 20240301, 20240302, 20240306, NULL,      34980000, 2, 'delivered'),
    (5003, 3, 20240410, 20240410, 20240412, 20240418, NULL,      3777000,  2, 'delivered'),
    (5004, 1, 20240601, 20240601, 20240602, 20240605, 20240610,  6480000,  2, 'returned'),
    (5005, 4, 20240510, NULL,     NULL,     NULL,     NULL,      445000,   1, 'pending');
```

#### Bước 4: Sample BI Queries

```sql
-- ========================================
-- QUERY (A): Basket Analysis
-- Tìm cặp sản phẩm thường được mua cùng nhau
-- ========================================

-- A1: Product Pair Frequency (Self-JOIN trên cùng order_id)
WITH order_pairs AS (
    SELECT
        a.order_id,
        a.product_sk AS product_a,
        b.product_sk AS product_b
    FROM fact_order_items a
    JOIN fact_order_items b
        ON a.order_id = b.order_id
        AND a.product_sk < b.product_sk     -- Tránh duplicate pairs (A,B) = (B,A)
)
SELECT
    pa.product_name                     AS product_a_name,
    pb.product_name                     AS product_b_name,
    COUNT(*)                            AS co_occurrence_count,
    ROUND(
        COUNT(*)::DECIMAL / (
            SELECT COUNT(DISTINCT order_id) FROM fact_order_items
        ) * 100, 2
    )                                   AS support_pct    -- % đơn hàng chứa cả 2
FROM order_pairs op
JOIN dim_product pa ON op.product_a = pa.product_sk
JOIN dim_product pb ON op.product_b = pb.product_sk
GROUP BY pa.product_name, pb.product_name
HAVING COUNT(*) >= 2                     -- Lọc cặp xuất hiện ≥ 2 lần
ORDER BY co_occurrence_count DESC;

/*
Kết quả mong đợi:
product_a_name    | product_b_name   | co_occurrence_count | support_pct
AirPods Pro 2     | iPhone 15 Pro    | 2                   | 40.00
AirPods Pro 2     | Ốp lưng MagSafe | 2                   | 40.00
*/


-- A2: Basket Analysis nâng cao — Lift metric
-- Lift = P(A∩B) / (P(A) × P(B))
-- Lift > 1 → sản phẩm tương quan dương (thường mua cùng)
WITH
total_orders AS (
    SELECT COUNT(DISTINCT order_id) AS total FROM fact_order_items
),
product_freq AS (
    SELECT product_sk, COUNT(DISTINCT order_id) AS freq
    FROM fact_order_items
    GROUP BY product_sk
),
pair_freq AS (
    SELECT
        a.product_sk AS prod_a,
        b.product_sk AS prod_b,
        COUNT(DISTINCT a.order_id) AS pair_count
    FROM fact_order_items a
    JOIN fact_order_items b ON a.order_id = b.order_id AND a.product_sk < b.product_sk
    GROUP BY a.product_sk, b.product_sk
)
SELECT
    pa.product_name                                 AS product_a,
    pb.product_name                                 AS product_b,
    pf.pair_count,
    ROUND(pf.pair_count::DECIMAL / t.total, 4)     AS support,
    ROUND(pf.pair_count::DECIMAL / fa.freq, 4)     AS confidence_a_to_b,
    ROUND(
        (pf.pair_count::DECIMAL / t.total) /
        ((fa.freq::DECIMAL / t.total) * (fb.freq::DECIMAL / t.total))
    , 4)                                            AS lift
FROM pair_freq pf
JOIN dim_product pa       ON pf.prod_a = pa.product_sk
JOIN dim_product pb       ON pf.prod_b = pb.product_sk
JOIN product_freq fa      ON pf.prod_a = fa.product_sk
JOIN product_freq fb      ON pf.prod_b = fb.product_sk
CROSS JOIN total_orders t
ORDER BY lift DESC;


-- ========================================
-- QUERY (B): Customer Lifetime Value by Cohort
-- Cohort = tháng đăng ký
-- ========================================

-- B1: Cohort Revenue Matrix
WITH customer_revenue AS (
    SELECT
        c.signup_year_month                     AS cohort,
        -- Tháng thứ N kể từ signup
        (EXTRACT(YEAR FROM d.full_date) - EXTRACT(YEAR FROM c.signup_date)) * 12 +
        (EXTRACT(MONTH FROM d.full_date) - EXTRACT(MONTH FROM c.signup_date))
                                                AS months_since_signup,
        c.customer_sk,
        SUM(f.line_total)                       AS revenue
    FROM fact_order_items f
    JOIN dim_customer_ecom c    ON f.customer_sk     = c.customer_sk
    JOIN dim_date d             ON f.order_date_key  = d.date_key
    GROUP BY c.signup_year_month, months_since_signup, c.customer_sk
)
SELECT
    cohort,
    months_since_signup,
    COUNT(DISTINCT customer_sk)                 AS active_customers,
    SUM(revenue)                                AS cohort_revenue,
    ROUND(SUM(revenue) / COUNT(DISTINCT customer_sk), 0) AS avg_revenue_per_customer,
    -- Cumulative CLTV
    SUM(SUM(revenue)) OVER (
        PARTITION BY cohort ORDER BY months_since_signup
    )                                           AS cumulative_cltv
FROM customer_revenue
GROUP BY cohort, months_since_signup
ORDER BY cohort, months_since_signup;

/*
Kết quả mong đợi:
cohort   | months_since_signup | active_customers | cohort_revenue | avg_revenue_per_customer | cumulative_cltv
2024-01  | 1                   | 2                | 70,940,000     | 35,470,000               | 70,940,000
2024-01  | 5                   | 1                | 6,480,000      | 6,480,000                | 77,420,000
2024-03  | 1                   | 1                | 3,777,000      | 3,777,000                | 3,777,000
2024-03  | 2                   | 1                | 445,000        | 445,000                  | 4,222,000
*/


-- B2: Retention Rate by Cohort
WITH cohort_size AS (
    SELECT signup_year_month AS cohort, COUNT(*) AS size
    FROM dim_customer_ecom
    GROUP BY signup_year_month
),
monthly_active AS (
    SELECT
        c.signup_year_month AS cohort,
        (EXTRACT(YEAR FROM d.full_date) - EXTRACT(YEAR FROM c.signup_date)) * 12 +
        (EXTRACT(MONTH FROM d.full_date) - EXTRACT(MONTH FROM c.signup_date))
            AS month_number,
        COUNT(DISTINCT c.customer_sk) AS active
    FROM fact_order_items f
    JOIN dim_customer_ecom c ON f.customer_sk = c.customer_sk
    JOIN dim_date d ON f.order_date_key = d.date_key
    GROUP BY c.signup_year_month, month_number
)
SELECT
    ma.cohort,
    cs.size AS cohort_size,
    ma.month_number,
    ma.active,
    ROUND(ma.active::DECIMAL / cs.size * 100, 1) AS retention_pct
FROM monthly_active ma
JOIN cohort_size cs ON ma.cohort = cs.cohort
ORDER BY ma.cohort, ma.month_number;


-- ========================================
-- QUERY (C): Late-Arriving Orders — Watermark concept
-- ========================================

/*
WATERMARK CONCEPT:
- Watermark = timestamp ngưỡng: data với event_timestamp < watermark
  được coi là "đã đầy đủ" (complete).
- Data đến SAU watermark nhưng có event_timestamp TRƯỚC watermark
  → Late-arriving data.

Xử lý trong ETL:
1. Ghi nhận is_late_arriving = TRUE
2. Cập nhật Periodic Snapshot nếu cần (re-aggregate ngày đã đóng)
3. KHÔNG xóa data cũ — chỉ cập nhật aggregate
*/

-- C1: Tìm tất cả late-arriving records
SELECT
    f.order_item_id,
    f.order_id,
    f.event_timestamp,
    f.etl_loaded_at,
    f.etl_loaded_at - f.event_timestamp AS latency,
    d.full_date AS order_date,
    p.product_name,
    f.line_total
FROM fact_order_items f
JOIN dim_date d     ON f.order_date_key = d.date_key
JOIN dim_product p  ON f.product_sk     = p.product_sk
WHERE f.is_late_arriving = TRUE
ORDER BY f.event_timestamp;


-- C2: ETL logic — Detect & flag late-arriving orders
-- (Watermark = ETL batch start time - 2 hours buffer)
/*
INSERT INTO fact_order_items (..., is_late_arriving)
SELECT
    ...,
    CASE
        WHEN o.order_date < (current_batch_watermark - INTERVAL '2 hours')
        THEN TRUE
        ELSE FALSE
    END AS is_late_arriving
FROM staging_orders o;
*/


-- C3: Re-aggregate impacted snapshots
/*
-- Khi phát hiện late data cho ngày 2024-05-10:
INSERT INTO fact_daily_summary (date_key, ...)
SELECT
    20240510,
    COUNT(*),
    SUM(line_total),
    ...
FROM fact_order_items
WHERE order_date_key = 20240510
ON CONFLICT (date_key)
DO UPDATE SET
    total_orders = EXCLUDED.total_orders,
    total_revenue = EXCLUDED.total_revenue;
*/
```

---

## 🏦 Đề 3 — Banking Domain

### 📋 Input OLTP Schema

```sql
-- ========================================
-- SOURCE OLTP TABLES
-- ========================================

CREATE TABLE customers (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(200) NOT NULL,
    segment         VARCHAR(50),                       -- 'retail', 'premium', 'corporate'
    risk_rating     VARCHAR(10)                        -- 'low', 'medium', 'high'
);

CREATE TABLE branches (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    city            VARCHAR(100) NOT NULL
);

CREATE TABLE accounts (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES customers(id),
    type            VARCHAR(30),                       -- 'checking', 'savings', 'loan'
    open_date       DATE NOT NULL,
    status          VARCHAR(20) DEFAULT 'active'       -- 'active', 'dormant', 'closed'
);

CREATE TABLE transactions (
    id              SERIAL PRIMARY KEY,
    account_id      INTEGER REFERENCES accounts(id),
    txn_date        TIMESTAMP NOT NULL,
    amount          DECIMAL(15, 2) NOT NULL,           -- Dương = credit, Âm = debit
    type            VARCHAR(20),                       -- 'deposit', 'withdrawal', 'transfer', 'fee'
    counterparty    VARCHAR(200)                       -- Bên đối tác giao dịch
);
```

### 🎯 Tiêu chí nghiệp vụ

| Ký hiệu | Yêu cầu | Độ khó |
|---|---|---|
| **(A)** | Báo cáo **balance cuối ngày** — Semi-Additive measure | ⭐⭐⭐ |
| **(B)** | **Fraud Detection**: Giao dịch bất thường > 3σ (3 standard deviations) so với mean | ⭐⭐⭐ |
| **(C)** | **Regulatory Audit Trail**: Lưu trữ đầy đủ thông tin cho kiểm toán | ⭐⭐ |

### ✅ Đáp án chi tiết

#### Bước 1: Xác định Grain

| Fact Table | Grain | Loại |
|---|---|---|
| `fact_daily_balance` | 1 row = 1 tài khoản × 1 ngày | Periodic Snapshot |
| `fact_transactions` | 1 row = 1 giao dịch | Transaction Fact |
| `fact_account_lifecycle` | 1 row = 1 tài khoản xuyên suốt vòng đời | Accumulating Snapshot |

#### Bước 2: DDL Star Schema

```sql
-- ========================================
-- DIM_DATE — Conformed (reuse từ Đề 1)
-- ========================================


-- ========================================
-- DIM_CUSTOMER_BANK
-- ========================================

CREATE TABLE dim_customer_bank (
    customer_sk         SERIAL PRIMARY KEY,
    customer_bk         INTEGER NOT NULL,
    customer_name       VARCHAR(200) NOT NULL,
    segment             VARCHAR(50),                   -- 'retail', 'premium', 'corporate'
    risk_rating         VARCHAR(10),                   -- 'low', 'medium', 'high'
    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);


-- ========================================
-- DIM_ACCOUNT
-- ========================================

CREATE TABLE dim_account (
    account_sk          SERIAL PRIMARY KEY,
    account_bk          INTEGER NOT NULL,              -- accounts.id
    customer_sk         INTEGER REFERENCES dim_customer_bank(customer_sk),
    account_type        VARCHAR(30),                   -- 'checking', 'savings', 'loan'
    open_date           DATE NOT NULL,
    status              VARCHAR(20),
    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);


-- ========================================
-- DIM_BRANCH
-- ========================================

CREATE TABLE dim_branch (
    branch_sk           SERIAL PRIMARY KEY,
    branch_bk           INTEGER NOT NULL,
    branch_name         VARCHAR(100) NOT NULL,
    city                VARCHAR(100) NOT NULL,
    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);


-- ========================================
-- DIM_TXN_TYPE — Mini Dimension
-- ========================================

CREATE TABLE dim_txn_type (
    txn_type_sk         SERIAL PRIMARY KEY,
    txn_type_code       VARCHAR(20) UNIQUE NOT NULL,
    txn_type_label      VARCHAR(50),
    is_credit           BOOLEAN,                       -- TRUE = tiền vào, FALSE = tiền ra
    is_auditable        BOOLEAN DEFAULT TRUE
);


-- ========================================
-- FACT_TRANSACTIONS — Transaction Fact Table
-- Grain: 1 row = 1 giao dịch
-- ========================================

CREATE TABLE fact_transactions (
    txn_id              INTEGER NOT NULL,               -- Degenerate dimension
    txn_date_key        INTEGER NOT NULL REFERENCES dim_date(date_key),
    account_sk          INTEGER NOT NULL REFERENCES dim_account(account_sk),
    customer_sk         INTEGER NOT NULL REFERENCES dim_customer_bank(customer_sk),
    branch_sk           INTEGER REFERENCES dim_branch(branch_sk),
    txn_type_sk         INTEGER NOT NULL REFERENCES dim_txn_type(txn_type_sk),

    -- Measures
    txn_amount          DECIMAL(15, 2) NOT NULL,        -- Additive (signed: + credit, - debit)
    abs_amount          DECIMAL(15, 2) NOT NULL,        -- Additive (absolute value)

    -- Audit trail (Yêu cầu C)
    counterparty        VARCHAR(200),
    txn_timestamp       TIMESTAMP NOT NULL,             -- Thời điểm chính xác
    source_system       VARCHAR(50) DEFAULT 'core_banking',
    etl_batch_id        BIGINT,
    etl_loaded_at       TIMESTAMP DEFAULT NOW(),

    -- Fraud detection metadata
    is_flagged_fraud    BOOLEAN DEFAULT FALSE,
    fraud_score         DECIMAL(5, 4),                  -- 0.0000 → 1.0000

    PRIMARY KEY (txn_id)
);

CREATE INDEX idx_fact_txn_date ON fact_transactions(txn_date_key);
CREATE INDEX idx_fact_txn_account ON fact_transactions(account_sk);
CREATE INDEX idx_fact_txn_fraud ON fact_transactions(is_flagged_fraud) WHERE is_flagged_fraud = TRUE;


-- ========================================
-- FACT_DAILY_BALANCE — Periodic Snapshot
-- Grain: 1 row = 1 tài khoản × 1 ngày
-- *** SEMI-ADDITIVE: balance KHÔNG SUM theo thời gian ***
-- ========================================

CREATE TABLE fact_daily_balance (
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_key),
    account_sk          INTEGER NOT NULL REFERENCES dim_account(account_sk),
    customer_sk         INTEGER NOT NULL REFERENCES dim_customer_bank(customer_sk),
    branch_sk           INTEGER REFERENCES dim_branch(branch_sk),

    -- Semi-Additive Measures
    opening_balance     DECIMAL(15, 2) NOT NULL,        -- Balance đầu ngày
    closing_balance     DECIMAL(15, 2) NOT NULL,        -- Balance cuối ngày

    -- Additive Measures (TRONG NGÀY)
    total_credits       DECIMAL(15, 2) DEFAULT 0,       -- Tổng tiền vào trong ngày
    total_debits        DECIMAL(15, 2) DEFAULT 0,       -- Tổng tiền ra trong ngày
    txn_count           INTEGER DEFAULT 0,              -- Số giao dịch trong ngày

    PRIMARY KEY (date_key, account_sk)
);

CREATE INDEX idx_fact_balance_account ON fact_daily_balance(account_sk);


-- ========================================
-- FACT_ACCOUNT_LIFECYCLE — Accumulating Snapshot
-- Grain: 1 row = 1 tài khoản
-- ========================================

CREATE TABLE fact_account_lifecycle (
    account_sk          INTEGER PRIMARY KEY REFERENCES dim_account(account_sk),
    customer_sk         INTEGER NOT NULL REFERENCES dim_customer_bank(customer_sk),

    -- Milestone date keys
    open_date_key       INTEGER REFERENCES dim_date(date_key),
    first_txn_date_key  INTEGER REFERENCES dim_date(date_key),
    first_deposit_key   INTEGER REFERENCES dim_date(date_key),
    dormant_date_key    INTEGER REFERENCES dim_date(date_key),
    close_date_key      INTEGER REFERENCES dim_date(date_key),

    -- Measures
    lifetime_credits    DECIMAL(15, 2) DEFAULT 0,
    lifetime_debits     DECIMAL(15, 2) DEFAULT 0,
    lifetime_txn_count  INTEGER DEFAULT 0,
    current_balance     DECIMAL(15, 2) DEFAULT 0,       -- Semi-Additive
    current_status      VARCHAR(20),

    -- Derived
    days_to_first_txn   INTEGER,
    account_age_days    INTEGER,

    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);
```

#### Bước 3: Sample Data

```sql
-- ========================================
-- INSERT SAMPLE DATA
-- ========================================

-- dim_txn_type
INSERT INTO dim_txn_type (txn_type_code, txn_type_label, is_credit, is_auditable) VALUES
    ('deposit',    'Nạp tiền',      TRUE,  TRUE),
    ('withdrawal', 'Rút tiền',      FALSE, TRUE),
    ('transfer',   'Chuyển khoản',  NULL,  TRUE),   -- Cả credit và debit tùy chiều
    ('fee',        'Phí dịch vụ',   FALSE, TRUE),
    ('interest',   'Lãi suất',      TRUE,  TRUE);

-- dim_branch
INSERT INTO dim_branch (branch_bk, branch_name, city) VALUES
    (1, 'CN Quận 1',        'TP.HCM'),
    (2, 'CN Hoàn Kiếm',     'Hà Nội'),
    (3, 'CN Hải Châu',      'Đà Nẵng');

-- dim_customer_bank
INSERT INTO dim_customer_bank (customer_bk, customer_name, segment, risk_rating) VALUES
    (401, 'Công ty TNHH ABC',    'corporate', 'low'),
    (402, 'Nguyễn Thị Mai',      'premium',   'low'),
    (403, 'Trần Văn Hùng',       'retail',    'medium'),
    (404, 'Lê Hoàng Nam',        'retail',    'high');

-- dim_account
INSERT INTO dim_account (account_bk, customer_sk, account_type, open_date, status) VALUES
    (1001, 1, 'checking', '2023-01-15', 'active'),
    (1002, 1, 'savings',  '2023-01-15', 'active'),
    (1003, 2, 'checking', '2023-06-01', 'active'),
    (1004, 3, 'checking', '2024-01-10', 'active'),
    (1005, 4, 'savings',  '2024-03-01', 'dormant');

-- fact_transactions
INSERT INTO fact_transactions (txn_id, txn_date_key, account_sk, customer_sk, branch_sk,
                               txn_type_sk, txn_amount, abs_amount, counterparty,
                               txn_timestamp, is_flagged_fraud) VALUES
    -- Account 1001 (Checking - Corporate)
    (1, 20250101, 1, 1, 1, 1,   500000000,  500000000, 'Đối tác XYZ',       '2025-01-01 08:30:00', FALSE),
    (2, 20250101, 1, 1, 1, 2,  -50000000,   50000000,  'Thanh toán NCC',    '2025-01-01 14:00:00', FALSE),
    (3, 20250102, 1, 1, 1, 1,   200000000,  200000000, 'Khách hàng DEF',    '2025-01-02 09:15:00', FALSE),
    (4, 20250102, 1, 1, 1, 2,  -30000000,   30000000,  'Tiền thuê VP',      '2025-01-02 16:30:00', FALSE),

    -- Account 1003 (Checking - Premium)
    (5, 20250101, 3, 2, 2, 1,   100000000,  100000000, 'Lương tháng 1',     '2025-01-01 10:00:00', FALSE),
    (6, 20250101, 3, 2, 2, 2,  -15000000,   15000000,  'Thanh toán thẻ',    '2025-01-01 18:00:00', FALSE),
    (7, 20250102, 3, 2, 2, 3,  -80000000,   80000000,  'CK cho con',        '2025-01-02 11:00:00', FALSE),

    -- Account 1004 (Retail) — Giao dịch bất thường
    (8,  20250101, 4, 3, 3, 1,  10000000,   10000000,  'Lương',             '2025-01-01 09:00:00', FALSE),
    (9,  20250102, 4, 3, 3, 2,  -500000,    500000,    'Tiền ăn',           '2025-01-02 12:00:00', FALSE),
    (10, 20250102, 4, 3, 3, 2,  -300000,    300000,    'Xăng xe',           '2025-01-02 13:00:00', FALSE),
    -- *** SUSPICIOUS: Rút 95,000,000 — vượt xa pattern thông thường ***
    (11, 20250103, 4, 3, 3, 2,  -95000000,  95000000,  'ATM nước ngoài',    '2025-01-03 03:45:00', FALSE),

    -- Account 1005 (Savings - Dormant)
    (12, 20240301, 5, 4, 1, 1,  20000000,   20000000,  'Nạp tiền',          '2024-03-01 10:00:00', FALSE);


-- fact_daily_balance
INSERT INTO fact_daily_balance (date_key, account_sk, customer_sk, branch_sk,
                                opening_balance, closing_balance,
                                total_credits, total_debits, txn_count) VALUES
    -- Account 1001
    (20250101, 1, 1, 1, 1000000000, 1450000000, 500000000, 50000000,  2),
    (20250102, 1, 1, 1, 1450000000, 1620000000, 200000000, 30000000,  2),

    -- Account 1003
    (20250101, 3, 2, 2, 200000000,  285000000,  100000000, 15000000,  2),
    (20250102, 3, 2, 2, 285000000,  205000000,  0,         80000000,  1),

    -- Account 1004
    (20250101, 4, 3, 3, 5000000,    15000000,   10000000,  0,         1),
    (20250102, 4, 3, 3, 15000000,   14200000,   0,         800000,    2),
    (20250103, 4, 3, 3, 14200000,  -80800000,   0,         95000000,  1),   -- Negative balance!

    -- Account 1005
    (20240301, 5, 4, 1, 0,          20000000,   20000000,  0,         1);


-- fact_account_lifecycle
INSERT INTO fact_account_lifecycle (account_sk, customer_sk, open_date_key,
                                    first_txn_date_key, first_deposit_key,
                                    dormant_date_key, close_date_key,
                                    lifetime_credits, lifetime_debits,
                                    lifetime_txn_count, current_balance, current_status,
                                    days_to_first_txn, account_age_days) VALUES
    (1, 1, 20230115, 20230115, 20230115, NULL,     NULL, 3500000000, 1200000000, 156, 1620000000, 'active',  0,  718),
    (3, 2, 20230601, 20230601, 20230601, NULL,     NULL, 800000000,  350000000,  89,  205000000,  'active',  0,  581),
    (4, 3, 20240110, 20240115, 20240115, NULL,     NULL, 120000000,  105800000,  45, -80800000,   'active',  5,  358),
    (5, 4, 20240301, 20240301, 20240301, 20240901, NULL, 20000000,   0,          1,   20000000,   'dormant', 0,  315);
```

#### Bước 4: Sample BI Queries

```sql
-- ========================================
-- QUERY (A): Balance cuối ngày — Semi-Additive Measure
-- *** RULE: SUM across accounts OK, nhưng KHÔNG SUM across dates ***
-- ========================================

-- A1: Tổng balance tất cả tài khoản tại 1 thời điểm
-- ✅ ĐÚNG: SUM closing_balance tại 1 ngày cụ thể (cross non-time dims)
SELECT
    d.full_date,
    b.branch_name,
    cb.segment                          AS customer_segment,
    COUNT(DISTINCT fb.account_sk)       AS account_count,
    SUM(fb.closing_balance)             AS total_balance,
    AVG(fb.closing_balance)             AS avg_balance
FROM fact_daily_balance fb
JOIN dim_date d             ON fb.date_key    = d.date_key
JOIN dim_branch b           ON fb.branch_sk   = b.branch_sk
JOIN dim_customer_bank cb   ON fb.customer_sk = cb.customer_sk
WHERE d.full_date = '2025-01-02'        -- Chỉ 1 ngày cụ thể!
GROUP BY d.full_date, b.branch_name, cb.segment
ORDER BY total_balance DESC;

/*
⚠️ SAI: SUM(closing_balance) GROUP BY month → Cộng balance 30 ngày lại vô nghĩa!
✅ ĐÚNG cho time range: Lấy balance ngày cuối cùng của mỗi kỳ
*/


-- A2: Balance cuối mỗi tháng (lấy ngày cuối cùng có data)
WITH last_day_per_month AS (
    SELECT
        account_sk,
        DATE_TRUNC('month', d.full_date)   AS month_start,
        MAX(d.full_date)                    AS last_date_in_month
    FROM fact_daily_balance fb
    JOIN dim_date d ON fb.date_key = d.date_key
    GROUP BY account_sk, DATE_TRUNC('month', d.full_date)
)
SELECT
    ldm.month_start,
    a.account_type,
    COUNT(DISTINCT fb.account_sk)           AS account_count,
    SUM(fb.closing_balance)                 AS eom_total_balance,
    AVG(fb.closing_balance)                 AS eom_avg_balance,
    MIN(fb.closing_balance)                 AS eom_min_balance,
    MAX(fb.closing_balance)                 AS eom_max_balance
FROM last_day_per_month ldm
JOIN fact_daily_balance fb  ON fb.account_sk = ldm.account_sk
                            AND fb.date_key = TO_CHAR(ldm.last_date_in_month, 'YYYYMMDD')::INTEGER
JOIN dim_account a          ON fb.account_sk = a.account_sk
GROUP BY ldm.month_start, a.account_type
ORDER BY ldm.month_start, a.account_type;


-- A3: Daily balance trend cho 1 tài khoản cụ thể
SELECT
    d.full_date,
    fb.opening_balance,
    fb.total_credits,
    fb.total_debits,
    fb.closing_balance,
    fb.txn_count,
    -- Moving average 7 ngày
    ROUND(AVG(fb.closing_balance) OVER (
        PARTITION BY fb.account_sk
        ORDER BY d.full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 0) AS ma7_balance
FROM fact_daily_balance fb
JOIN dim_date d ON fb.date_key = d.date_key
WHERE fb.account_sk = 4
ORDER BY d.full_date;


-- ========================================
-- QUERY (B): Fraud Detection — Giao dịch > 3σ
-- Z-score = (value - mean) / stddev
-- |Z-score| > 3 → Bất thường (99.7% rule)
-- ========================================

-- B1: Tính Z-score cho mỗi giao dịch debit của từng tài khoản
WITH account_stats AS (
    -- Tính mean và stddev cho mỗi tài khoản (chỉ debit transactions)
    SELECT
        account_sk,
        AVG(abs_amount)                     AS mean_amount,
        STDDEV_POP(abs_amount)              AS stddev_amount,
        COUNT(*)                            AS txn_count
    FROM fact_transactions
    WHERE txn_amount < 0                    -- Chỉ debit (rút/chuyển/phí)
    GROUP BY account_sk
    HAVING COUNT(*) >= 3                    -- Cần ít nhất 3 giao dịch để tính stddev
)
SELECT
    ft.txn_id,
    d.full_date,
    ft.txn_timestamp,
    c.customer_name,
    c.risk_rating,
    a.account_type,
    ft.abs_amount,
    ast.mean_amount,
    ast.stddev_amount,

    -- Z-Score
    ROUND(
        (ft.abs_amount - ast.mean_amount) / NULLIF(ast.stddev_amount, 0)
    , 4)                                    AS z_score,

    -- Flag
    CASE
        WHEN ABS((ft.abs_amount - ast.mean_amount) / NULLIF(ast.stddev_amount, 0)) > 3
        THEN '🚨 FRAUD ALERT'
        WHEN ABS((ft.abs_amount - ast.mean_amount) / NULLIF(ast.stddev_amount, 0)) > 2
        THEN '⚠️ SUSPICIOUS'
        ELSE '✅ NORMAL'
    END                                     AS fraud_flag,

    ft.counterparty

FROM fact_transactions ft
JOIN account_stats ast      ON ft.account_sk  = ast.account_sk
JOIN dim_date d             ON ft.txn_date_key = d.date_key
JOIN dim_account a          ON ft.account_sk  = a.account_sk
JOIN dim_customer_bank c    ON ft.customer_sk = c.customer_sk
WHERE ft.txn_amount < 0                     -- Chỉ debit
ORDER BY z_score DESC NULLS LAST;

/*
Kết quả mong đợi cho Account 1004:
- Giao dịch #9:  500,000    → Z-score ≈ -0.55 → ✅ NORMAL
- Giao dịch #10: 300,000    → Z-score ≈ -0.59 → ✅ NORMAL
- Giao dịch #11: 95,000,000 → Z-score ≈ 1.73  → (nếu chỉ 3 records, stddev lớn)
  Với nhiều data hơn, giao dịch 95M sẽ vượt 3σ → 🚨 FRAUD ALERT
*/


-- B2: Fraud detection nâng cao — Rolling window 30 ngày
WITH rolling_stats AS (
    SELECT
        ft.txn_id,
        ft.account_sk,
        ft.txn_timestamp,
        ft.abs_amount,
        -- Rolling mean & stddev 30 ngày trước (không tính giao dịch hiện tại)
        AVG(ft.abs_amount) OVER w   AS rolling_mean,
        STDDEV(ft.abs_amount) OVER w AS rolling_stddev,
        COUNT(*) OVER w              AS window_size
    FROM fact_transactions ft
    WHERE ft.txn_amount < 0
    WINDOW w AS (
        PARTITION BY ft.account_sk
        ORDER BY ft.txn_timestamp
        RANGE BETWEEN INTERVAL '30 days' PRECEDING AND INTERVAL '1 second' PRECEDING
    )
)
SELECT
    txn_id,
    txn_timestamp,
    abs_amount,
    rolling_mean,
    rolling_stddev,
    CASE
        WHEN window_size >= 5 AND rolling_stddev > 0
        THEN ROUND((abs_amount - rolling_mean) / rolling_stddev, 4)
    END AS rolling_z_score
FROM rolling_stats
WHERE window_size >= 5                       -- Đủ data để tính
ORDER BY txn_timestamp;


-- B3: Update fraud flag sau khi detect
UPDATE fact_transactions ft
SET
    is_flagged_fraud = TRUE,
    fraud_score = LEAST(ABS(sub.z_score) / 5.0, 1.0)   -- Normalize score 0→1
FROM (
    SELECT
        ft2.txn_id,
        (ft2.abs_amount - ast.mean_amount) / NULLIF(ast.stddev_amount, 0) AS z_score
    FROM fact_transactions ft2
    JOIN (
        SELECT account_sk, AVG(abs_amount) AS mean_amount, STDDEV_POP(abs_amount) AS stddev_amount
        FROM fact_transactions WHERE txn_amount < 0 GROUP BY account_sk HAVING COUNT(*) >= 3
    ) ast ON ft2.account_sk = ast.account_sk
    WHERE ft2.txn_amount < 0
      AND ABS((ft2.abs_amount - ast.mean_amount) / NULLIF(ast.stddev_amount, 0)) > 3
) sub
WHERE ft.txn_id = sub.txn_id;


-- ========================================
-- QUERY (C): Regulatory Audit Trail
-- ========================================

-- C1: Full audit trail cho 1 tài khoản
-- Kiểm toán viên cần xem: AI, NÀO, Ở ĐÂU, LÚC NÀO, BAO NHIÊU, TỪ ĐÂU
SELECT
    ft.txn_id                              AS "Mã GD",
    ft.txn_timestamp                       AS "Thời điểm",
    c.customer_name                        AS "Khách hàng",
    c.segment                              AS "Phân khúc",
    c.risk_rating                          AS "Xếp hạng rủi ro",
    a.account_type                         AS "Loại TK",
    a.account_bk                           AS "Số TK",
    b.branch_name                          AS "Chi nhánh",
    b.city                                 AS "Thành phố",
    tt.txn_type_label                      AS "Loại GD",
    ft.txn_amount                          AS "Số tiền (có dấu)",
    ft.abs_amount                          AS "Số tiền (tuyệt đối)",
    ft.counterparty                        AS "Đối tác",
    ft.is_flagged_fraud                    AS "Cờ gian lận",
    ft.fraud_score                         AS "Điểm rủi ro",
    ft.source_system                       AS "Hệ thống nguồn",
    ft.etl_batch_id                        AS "Batch ETL",
    ft.etl_loaded_at                       AS "Thời điểm load"
FROM fact_transactions ft
JOIN dim_date d             ON ft.txn_date_key = d.date_key
JOIN dim_account a          ON ft.account_sk   = a.account_sk
JOIN dim_customer_bank c    ON ft.customer_sk  = c.customer_sk
JOIN dim_branch b           ON ft.branch_sk    = b.branch_sk
JOIN dim_txn_type tt        ON ft.txn_type_sk  = tt.txn_type_sk
WHERE a.account_bk = 1004                     -- Tài khoản cần audit
ORDER BY ft.txn_timestamp;


-- C2: Audit summary — Báo cáo kiểm toán tổng hợp theo tháng
SELECT
    DATE_TRUNC('month', d.full_date)        AS audit_month,
    b.branch_name,
    tt.txn_type_label,
    COUNT(*)                                AS txn_count,
    SUM(ft.abs_amount)                      AS total_volume,
    SUM(CASE WHEN ft.is_flagged_fraud THEN 1 ELSE 0 END) AS flagged_count,
    ROUND(
        SUM(CASE WHEN ft.is_flagged_fraud THEN 1 ELSE 0 END)::DECIMAL /
        NULLIF(COUNT(*), 0) * 100, 2
    )                                       AS fraud_rate_pct
FROM fact_transactions ft
JOIN dim_date d         ON ft.txn_date_key = d.date_key
JOIN dim_branch b       ON ft.branch_sk    = b.branch_sk
JOIN dim_txn_type tt    ON ft.txn_type_sk  = tt.txn_type_sk
GROUP BY GROUPING SETS (
    (DATE_TRUNC('month', d.full_date), b.branch_name, tt.txn_type_label),
    (DATE_TRUNC('month', d.full_date), b.branch_name),
    (DATE_TRUNC('month', d.full_date))
)
ORDER BY audit_month, b.branch_name NULLS LAST, tt.txn_type_label NULLS LAST;


-- C3: Regulatory compliance — Tài khoản có balance âm (violation)
SELECT
    a.account_bk                            AS "Số TK",
    c.customer_name                         AS "Khách hàng",
    c.risk_rating                           AS "Xếp hạng rủi ro",
    d.full_date                             AS "Ngày vi phạm",
    fb.closing_balance                      AS "Balance cuối ngày",
    fb.total_debits                         AS "Tổng rút trong ngày",
    fb.txn_count                            AS "Số GD trong ngày"
FROM fact_daily_balance fb
JOIN dim_date d             ON fb.date_key    = d.date_key
JOIN dim_account a          ON fb.account_sk  = a.account_sk
JOIN dim_customer_bank c    ON fb.customer_sk = c.customer_sk
WHERE fb.closing_balance < 0
ORDER BY fb.closing_balance ASC;

/*
Kết quả: Account 1004 ngày 2025-01-03 có balance -80,800,000
→ Vi phạm quy định → Escalate cho Risk Management
*/
```

---

## 📊 Star Schema Diagrams

### Đề 1 — Fleet Management

```
                    ┌──────────────┐
                    │   dim_date   │
                    │──────────────│
                    │ date_key (PK)│
                    │ fiscal_year  │
                    │ fiscal_qtr   │
                    └──────┬───────┘
                           │
┌───────────────┐   ┌──────┴───────┐   ┌──────────────────┐
│  dim_customer │───│ fact_invoices│───│ dim_invoice_status│
│  (SCD Type 2) │   │──────────────│   │──────────────────│
│───────────────│   │ invoice_id   │   │ status_sk (PK)   │
│ customer_sk   │   │ date_key(FK) │   │ status_code      │
│ customer_bk   │   │ customer_sk  │   │ is_revenue_real. │
│ status        │   │ head_sk (FK) │   └──────────────────┘
│ effective_date│   │ status_sk(FK)│
│ expiration_dt │   │ service_amt  │
│ is_current    │   │ parts_amt    │
└───────────────┘   │ total_amt    │
                    └──────┬───────┘
                           │
                    ┌──────┴───────┐
                    │   dim_head   │
                    │──────────────│
                    │ head_sk (PK) │
                    │ head_name    │
                    │ capacity     │
                    └──────────────┘
```

### Đề 3 — Banking

```
                    ┌──────────────┐
                    │   dim_date   │
                    │ (Conformed)  │
                    └──────┬───────┘
                           │
                           │  date_key
     ┌─────────────────────┼─────────────────────┐
     │                     │                     │
┌────┴──────────┐   ┌──────┴───────┐   ┌────────┴───────────┐
│ fact_daily_   │   │    fact_     │   │ fact_account_       │
│ balance       │   │ transactions │   │ lifecycle           │
│ (Periodic     │   │ (Transaction │   │ (Accumulating       │
│  Snapshot)    │   │  Fact)       │   │  Snapshot)          │
│───────────────│   │──────────────│   │─────────────────────│
│ date_key      │   │ txn_id       │   │ account_sk          │
│ account_sk    │   │ txn_date_key │   │ open_date_key       │
│ opening_bal   │   │ account_sk   │   │ first_txn_date_key  │
│ closing_bal   │   │ txn_amount   │   │ dormant_date_key    │
│ total_credits │   │ fraud_score  │   │ close_date_key      │
│ total_debits  │   │ counterparty │   │ lifetime_credits    │
└───────────────┘   └──────────────┘   └─────────────────────┘
     │                     │                     │
     └─────────┬───────────┼─────────┬───────────┘
               │           │         │
        ┌──────┴──┐  ┌─────┴────┐ ┌──┴───────────┐
        │dim_acct │  │dim_branch│ │dim_customer  │
        │         │  │          │ │_bank         │
        │─────────│  │──────────│ │──────────────│
        │acct_sk  │  │branch_sk │ │customer_sk   │
        │acct_type│  │city      │ │segment       │
        │open_date│  │          │ │risk_rating   │
        └─────────┘  └──────────┘ └──────────────┘
```

---

## ✅ Checklist ôn tập — Tự đánh giá

### Lý thuyết cốt lõi

| # | Câu hỏi tự kiểm tra | ✅ |
|---|---|---|
| 1 | Phân biệt được 3 loại Fact Table? Cho ví dụ cho mỗi loại? | ☐ |
| 2 | Giải thích tại sao `inventory_balance` là Semi-Additive? | ☐ |
| 3 | Viết được DDL cho SCD Type 2 với surrogate key? | ☐ |
| 4 | Giải thích Grain là gì? Tại sao phải xác định Grain trước? | ☐ |
| 5 | Conformed Dimension giải quyết vấn đề gì? | ☐ |

### Kỹ năng thiết kế

| # | Câu hỏi tự kiểm tra | ✅ |
|---|---|---|
| 6 | Từ OLTP schema, xác định được Grain cho Fact table? | ☐ |
| 7 | Phân loại đúng measures: Additive vs Semi-Additive vs Non-Additive? | ☐ |
| 8 | Thiết kế dim_date với Fiscal Year logic tùy chỉnh? | ☐ |
| 9 | Implement SCD Type 2 ETL (INSERT/UPDATE logic)? | ☐ |
| 10 | Tạo Materialized View cho dashboard performance? | ☐ |

### Queries nâng cao

| # | Câu hỏi tự kiểm tra | ✅ |
|---|---|---|
| 11 | Viết query Basket Analysis với Support, Confidence, Lift? | ☐ |
| 12 | Viết Cohort Analysis query (CLTV by signup month)? | ☐ |
| 13 | Xử lý Late-Arriving Data với Watermark concept? | ☐ |
| 14 | Query Semi-Additive measure đúng cách (end-of-period)? | ☐ |
| 15 | Implement Fraud Detection với Z-score (3σ rule)? | ☐ |
| 16 | Viết Regulatory Audit Trail query với GROUPING SETS? | ☐ |

### Phỏng vấn Practice

| # | Câu hỏi phỏng vấn thường gặp | ✅ |
|---|---|---|
| 17 | "Thiết kế DWH cho hệ thống đặt xe (ride-hailing)" — xác định grain? | ☐ |
| 18 | "Khách hàng thay đổi địa chỉ — SCD nào phù hợp?" — giải thích trade-off? | ☐ |
| 19 | "Data đến muộn 3 ngày — xử lý thế nào?" — watermark vs reprocessing? | ☐ |
| 20 | "Dashboard cần response < 200ms — giải pháp?" — MV, pre-aggregation, caching? | ☐ |

---

## 🔑 Key Takeaways

> **Nguyên tắc #1**: Xác định **Grain** trước khi thiết kế bất kỳ thứ gì.

> **Nguyên tắc #2**: Semi-Additive measures (balance, inventory) → KHÔNG BAO GIỜ SUM theo thời gian. Dùng `LAST_VALUE`, `AVG`, hoặc lọc `end-of-period`.

> **Nguyên tắc #3**: SCD Type 2 cho phép **point-in-time analysis** — JOIN fact với dimension version tại thời điểm sự kiện xảy ra.

> **Nguyên tắc #4**: Materialized View + CONCURRENTLY refresh = giải pháp low-latency cho dashboards mà không cần tech stack phức tạp.

> **Nguyên tắc #5**: Fraud detection dùng **statistical outlier** (Z-score) trên rolling window — không cần ML cho baseline.

---

*📅 Tài liệu này thuộc **Pillar 4 (Days 6–7)** trong kế hoạch ôn tập 14 ngày Data Engineer Interview Prep.*
*Cập nhật lần cuối: 2025-08-12*
