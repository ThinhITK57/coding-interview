# Cài đặt PostgreSQL cho Fleet Platform (Odoo OLTP + Airflow Metadata)

> **Vai trò trong kiến trúc**: PostgreSQL đóng 2 vai trò:
> 1. **Odoo OLTP DB** — nguồn dữ liệu cho Debezium CDC (cần `wal_level=logical`)
> 2. **Airflow Metadata DB** — lưu trạng thái DAGs, task instances
>
> Cả 2 là database riêng biệt trên cùng 1 Postgres instance.

---

## 1. Cài đặt PostgreSQL

```bash
ssh aiguystory@master

# Cài đặt PostgreSQL 15+ (Ubuntu 22.04/24.04)
sudo apt update
sudo apt install -y postgresql postgresql-contrib

# Kiểm tra version
psql --version

# Postgres tự start sau cài, kiểm tra:
sudo systemctl status postgresql
```

---

## 2. Cấu hình WAL cho CDC (BẮT BUỘC)

Đây là bước **quan trọng nhất** — nếu không set `wal_level=logical`, Debezium KHÔNG THỂ đọc WAL.

```bash
# Tìm file postgresql.conf
sudo -u postgres psql -c "SHOW config_file;"
# Thường là: /etc/postgresql/15/main/postgresql.conf

# Chỉnh sửa
sudo nano /etc/postgresql/15/main/postgresql.conf
```

Thay đổi các dòng sau:

```ini
# ======================== CDC REQUIRED ========================
# Bắt buộc cho Debezium log-based CDC
wal_level = logical

# Số replication slot tối đa (Debezium dùng 1 slot)
max_replication_slots = 4

# Số WAL sender process (mỗi slot cần 1 sender)
max_wal_senders = 4

# Giữ WAL segments đủ lâu để Debezium kịp đọc khi restart
# 1GB đủ cho lab; production tùy write throughput
wal_keep_size = 1024

# ======================== PERFORMANCE ========================
# Cho phép kết nối từ Debezium Connect, Spark, Airflow
listen_addresses = '*'
max_connections = 100
```

---

## 3. Cấu hình pg_hba.conf (cho phép kết nối remote)

```bash
sudo nano /etc/postgresql/15/main/pg_hba.conf
```

Thêm cuối file:

```
# Fleet Platform — cho phép kết nối từ tất cả nodes trong cluster
# TYPE  DATABASE        USER            ADDRESS                 METHOD
host    all             all             0.0.0.0/0               md5

# Replication — Debezium cần quyền replication
host    replication     fleet_cdc       0.0.0.0/0               md5
```

---

## 4. Restart PostgreSQL

```bash
sudo systemctl restart postgresql

# Verify wal_level
sudo -u postgres psql -c "SHOW wal_level;"
# Phải trả về: logical
```

---

## 5. Tạo databases và users

```bash
sudo -u postgres psql
```

```sql
-- =============================================================
-- 1. Database & User cho Odoo OLTP (nguồn CDC)
-- =============================================================
CREATE USER fleet_app WITH PASSWORD 'fleet_app_2024';
CREATE DATABASE fleet_oltp OWNER fleet_app;

-- User riêng cho Debezium CDC (cần quyền REPLICATION)
CREATE USER fleet_cdc WITH PASSWORD 'fleet_cdc_2024' REPLICATION;
GRANT CONNECT ON DATABASE fleet_oltp TO fleet_cdc;

-- =============================================================
-- 2. Database & User cho Airflow Metadata
-- =============================================================
CREATE USER airflow_user WITH PASSWORD 'airflow_2024';
CREATE DATABASE airflow_metadata OWNER airflow_user;

-- =============================================================
-- 3. Verify
-- =============================================================
\l
-- Phải thấy: fleet_oltp, airflow_metadata
\du
-- Phải thấy: fleet_app, fleet_cdc (có replication), airflow_user

\q
```

---

## 6. Cấp quyền CDC cho Debezium trên fleet_oltp

```bash
sudo -u postgres psql -d fleet_oltp
```

```sql
-- Debezium cần SELECT trên các bảng để đọc snapshot ban đầu
-- và cần quyền trên publication cho logical replication
GRANT USAGE ON SCHEMA public TO fleet_cdc;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO fleet_cdc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO fleet_cdc;

-- Tạo publication cho Debezium (sẽ subscribe tất cả bảng)
CREATE PUBLICATION fleet_cdc_publication FOR ALL TABLES;

\q
```

---

## 6.5 Cấu hình REPLICA IDENTITY (BẮT BUỘC cho CDC)

> ⚠️ **LƯU Ý THỨ TỰ THỰC THI**:
> Các lệnh `ALTER TABLE ... REPLICA IDENTITY FULL` dưới đây chỉ chạy được **sau khi các bảng dữ liệu đã được tạo** (ở **Ticket 2** bằng file `oltp/sql/01_schema.sql`).
> Nếu bạn mới cài đặt Postgres (Ticket 1) và chưa tạo bảng, bước này sẽ báo lỗi `relation "..." does not exist`. File `01_schema.sql` đã tự động tích hợp sẵn các lệnh này ngay sau phần `CREATE TABLE`.
>
> **Lý do kiến trúc (Why)**:
> Postgres mặc định dùng `REPLICA IDENTITY DEFAULT` — khi UPDATE hoặc DELETE,
> WAL chỉ ghi **primary key** của row cũ. Debezium đọc WAL → CDC event chỉ chứa
> PK trong before image → **mất toàn bộ giá trị cũ của các cột khác**.
>
> **Hệ quả nếu không sửa**:
> - **SCD Type 2 (`customers`)**: Spark cần biết status CŨ ("mới") để đóng record
>   trước khi mở record mới ("thường niên"). Với DEFAULT → không có status cũ → **SCD2 hỏng**.
> - **Báo cáo doanh thu (`invoices`, `work_orders`)**: Hóa đơn bị sửa giá hoặc hủy,
>   cần giá trị CŨ để tính delta. Với DEFAULT → chỉ PK → **sai báo cáo**.
> - **Redis sync (`heads`, `parts_inventory`)**: Row bị DELETE, cần data cũ để xóa
>   đúng entry trong Redis GEO/Hash.

```bash
# LƯU Ý: Chạy lệnh dưới đây SAU KHI đã khởi tạo schema ở Ticket 2 (01_schema.sql):
sudo -u postgres psql -d fleet_oltp
```

```sql
-- =============================================================
-- Đặt REPLICA IDENTITY FULL cho tất cả bảng CDC
-- WAL sẽ ghi TOÀN BỘ row cũ khi UPDATE/DELETE
-- Debezium CDC event sẽ có đầy đủ before + after image
-- =============================================================

ALTER TABLE public.customers       REPLICA IDENTITY FULL;
ALTER TABLE public.heads           REPLICA IDENTITY FULL;
ALTER TABLE public.parts_inventory REPLICA IDENTITY FULL;
ALTER TABLE public.work_orders     REPLICA IDENTITY FULL;
ALTER TABLE public.invoices        REPLICA IDENTITY FULL;

-- =============================================================
-- Verify: tất cả phải hiện 'f' (= FULL)
-- =============================================================
SELECT relname, relreplident
FROM pg_class
WHERE relname IN ('customers', 'heads', 'parts_inventory', 'work_orders', 'invoices');

-- Kết quả mong đợi:
--     relname       | relreplident
-- ------------------+--------------
--  customers        | f
--  heads            | f
--  parts_inventory  | f
--  work_orders      | f
--  invoices         | f

\q
```

### Trade-off của REPLICA IDENTITY FULL

| | DEFAULT | FULL |
|---|---|---|
| WAL size khi UPDATE | Nhỏ (chỉ PK) | Lớn hơn (toàn bộ row cũ) |
| CDC before image | Chỉ PK | Đầy đủ tất cả columns |
| INSERT bị ảnh hưởng? | Không | Không (INSERT không có before image) |
| Phù hợp cho | Bảng write-heavy, consumer chỉ cần biết key | Bảng cần audit trail, SCD, delta |

**Trong dự án này**: 5 bảng OLTP giao dịch, **không phải** high-write-volume (telemetry đi thẳng
vào Kafka, không qua Postgres). Overhead WAL hoàn toàn chấp nhận được. Đổi lại: CDC event có
đầy đủ data cho mọi consumer hạ nguồn — Redis sync, Spark batch, SCD2.

---

## 7. Test kết nối từ các nodes khác

> **Lưu ý**: PostgreSQL **server** chỉ cài trên `master`. Slave1/slave2 không có
> lệnh `psql`. Cần cài **postgresql-client** (chỉ client, không cài server) trước.

### 7.1 Cài postgresql-client trên slave1 và slave2

```bash
# Trên slave1
ssh aiguystory@slave1
sudo apt update
sudo apt install -y postgresql-client

# Trên slave2
ssh aiguystory@slave2
sudo apt update
sudo apt install -y postgresql-client
```

### 7.2 Test kết nối

```bash
# Từ slave1
ssh aiguystory@slave1
psql -h master -U fleet_app -d fleet_oltp -c "SELECT 1;"
psql -h master -U fleet_cdc -d fleet_oltp -c "SELECT 1;"
psql -h master -U airflow_user -d airflow_metadata -c "SELECT 1;"

# Từ slave2
ssh aiguystory@slave2
psql -h master -U fleet_app -d fleet_oltp -c "SELECT 1;"
```

Mỗi lệnh sẽ hỏi password (xem mục 5), kết quả phải trả về:
```
 ?column?
----------
        1
(1 row)
```

> **Mục đích**: Spark workers chạy trên slave1/slave2 kết nối Postgres trên master để đọc data qua JDBC.

