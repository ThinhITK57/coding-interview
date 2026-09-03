# P16 — CẨM NANG TOÀN DIỆN VỀ CÁC KIỂU DỮ LIỆU KHI THIẾT KẾ DATABASE & DATA WAREHOUSE
## Chi Tiết Kỹ Thuật: Dung Lượng (Bytes), Dải Giá Trị, Bẫy Hiệu Năng & Quy Chuẩn Lựa Chọn Chuẩn Senior (PostgreSQL, Spark, Parquet & DWH)

> **Tôn Chỉ Của Kiến Trúc Sư Cơ Sở Dữ Liệu:**
> *"Chọn sai kiểu dữ liệu là khởi đầu của mọi thảm họa hiệu năng: 
> 1. Dùng `FLOAT` lưu tiền tệ $\implies$ Sai lệch số dư kế toán do lỗi làm tròn nhị phân IEEE 754.
> 2. Dùng `BIGINT` bừa bãi cho bảng 100 triệu dòng $\implies$ Lãng phí hàng Gigabyte RAM của Buffer Pool (`shared_buffers`) và làm chậm Index Scan.
> 3. Dùng `VARCHAR(255)` thay vì `TEXT` hoặc `ENUM` $\implies$ Hiểu sai cơ chế lưu trữ của database hiện đại.
> Một Kỹ sư Dữ liệu giỏi phải chọn kiểu dữ liệu **vừa khít với miền giá trị thực tế**, tối ưu hóa tối đa dung lượng bộ nhớ và tốc độ xử lý I/O."*

---

# MỤC LỤC TỔNG QUAN

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ NHÓM 1: CÁC KIỂU SỐ NGUYÊN (INTEGER DATA TYPES)                                                        │
│   • SMALLINT (2 Bytes), INTEGER / INT (4 Bytes), BIGINT (8 Bytes)                                      │
│   • SERIAL, BIGSERIAL vs IDENTITY GENERATED ALWAYS (Quy chuẩn khóa chính tự tăng)                      │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ NHÓM 2: CÁC KIỂU SỐ THỰC & TIỀN TỆ / TÀI CHÍNH (NUMERIC & FLOATING-POINT)                              │
│   • NUMERIC(p, s) / DECIMAL(p, s) (Arbitrary Precision — Bắt buộc cho tài chính, tiền tệ)              │
│   • REAL (Float4 — 4 Bytes) & DOUBLE PRECISION (Float8 — 8 Bytes) (Dùng cho IoT, AI/ML, Tọa độ)        │
│   • Bẫy kinh điển: Tại sao tuyệt đối KHÔNG dùng FLOAT để lưu số tiền?                                  │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ NHÓM 3: CÁC KIỂU CHUỖI KÝ TỰ & DANH MỤC (STRING & CHARACTER TYPES)                                    │
│   • CHAR(n), VARCHAR(n), TEXT (Giải mã huyền thoại về hiệu năng trong PostgreSQL)                      │
│   • ENUM (Kiểu liệt kê danh mục tối ưu 4 Bytes)                                                        │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ NHÓM 4: CÁC KIỂU THỜI GIAN & NGÀY THÁNG (DATE & TIME TYPES)                                            │
│   • DATE (4 Bytes), TIME (8 Bytes)                                                                     │
│   • TIMESTAMP (Without TZ) vs TIMESTAMPTZ (With Time Zone — Chuẩn quốc tế)                             │
│   • INTERVAL (Khoảng thời gian)                                                                        │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ NHÓM 5: KIỂU LOGIC, ĐỊNH DANH DUY NHẤT & BÁN CẤU TRÚC (BOOLEAN, UUID, JSON/JSONB)                      │
│   • BOOLEAN (1 Byte)                                                                                   │
│   • UUID (16 Bytes nhị phân vs 36 Bytes chuỗi)                                                         │
│   • JSON (Text thô) vs JSONB (Binary decomposed format, hỗ trợ GIN Index)                              │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ NHÓM 6: CÁC KIỂU DỮ LIỆU ĐẶC THÙ (KHÔNG GIAN POSTGIS & AN NINH MẠNG INET)                              │
│   • GEOMETRY / GEOGRAPHY (Tọa độ Lat/Lng GPS xe tải)                                                  │
│   • INET, CIDR, MACADDR (Lưu địa chỉ IP & MAC máy chủ, log an ninh mạng VCS)                          │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ NHÓM 7: BẢNG SO SÁNH ÁNH XẠ KIỂU DỮ LIỆU (POSTGRESQL VS PYSPARK VS PARQUET)                            │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# NHÓM 1: CÁC KIỂU SỐ NGUYÊN (INTEGER DATA TYPES)

```
┌───────────┬──────────────┬──────────────────────────────────────────┬──────────────────────────────────────────┐
│ Kiểu Dữ Liệu│ Dung Lượng │ Dải Giá Trị (Value Range)                │ Trường Hợp Sử Dụng Điển Hình             │
├───────────┼──────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ SMALLINT  │ **2 Bytes**  │ $-32.768$ đến $+32.767$                  │ Tháng (1-12), Tuổi, Mã trạng thái (0-10),│
│ (INT2)    │ (16 bits)    │                                          │ Số buồng sửa xe của 1 trạm (max 20)      │
├───────────┼──────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ INTEGER   │ **4 Bytes**  │ $-2.147.483.648$ đến $+2.147.483.647$    │ ID Khách hàng, ID Trạm, Số lượng tồn kho,│
│ (INT / INT4)│ (32 bits)  │ (~2.1 tỷ)                                │ `date_sk` (YYYYMMDD) trong DWH           │
├───────────┼──────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ BIGINT    │ **8 Bytes**  │ $-9 \times 10^{18}$ đến $+9 \times 10^{18}$│ Khóa Surrogate Key bảng Fact DWH, ID Hóa │
│ (INT8)    │ (64 bits)    │ (~9 triệu tỷ)                            │ đơn ERP lớn, Gói tin IoT Telemetry       │
└───────────┴──────────────┴──────────────────────────────────────────┴──────────────────────────────────────────┘
```

### 💡 Bẫy Thiết Kế & Lời Khuyên Của Senior:
1. **Bẫy Tràn Khóa Chính (Primary Key Integer Overflow)**:
   * Nếu bảng `invoices` hoặc `telemetry_logs` dùng `INT (4 Bytes)`, khi hệ thống đạt $2.147.483.648$ bản ghi, câu lệnh `INSERT` tiếp theo sẽ văng lỗi: **`ERROR: integer out of range`** làm tê liệt toàn bộ hệ thống!
   * $\implies$ **Quy tắc**: Các bảng giao dịch (Transactional / Fact Tables) có tốc độ tăng trưởng nhanh **BẮT BUỘC dùng `BIGINT`**. Bảng danh mục (Dimension/Lookup) ít biến động dùng `INT` hoặc `SMALLINT`.
2. **Quy Chuẩn Tạo Khóa Tự Tăng (Identity Column vs Serial)**:
   * *Cách cũ*: `id SERIAL PRIMARY KEY` (Tạo ngầm 1 sequence độc lập, dễ bị lỗi mất đồng bộ khi backup/restore).
   * *Cách chuẩn SQL quốc tế (Khuyên dùng từ Postgres 10+)*:
     ```sql
     id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
     ```
     *(Ngăn chặn việc dev vô tình gõ tay chèn đè giá trị vào cột ID tự tăng).*

---

# NHÓM 2: CÁC KIỂU SỐ THỰC & TIỀN TỆ / TÀI CHÍNH (NUMERIC & FLOATING-POINT)

```
┌──────────────────┬──────────────┬──────────────────────────────────┬──────────────────────────────────────────┐
│ Kiểu Dữ Liệu     │ Dung Lượng   │ Độ Chính Xác (Precision)         │ Trường Hợp Sử Dụng                       │
├──────────────────┼──────────────┼──────────────────────────────────┼──────────────────────────────────────────┤
│ NUMERIC(p, s) /  │ Biến đổi     │ **Chính xác tuyệt đối 100%**     │ **Tiền tệ, Hóa đơn, Doanh thu, Lương,    │
│ DECIMAL(p, s)    │ (Variable)   │ Lên tới 1000 chữ số              │ Thuế VAT, Chi phí linh kiện**            │
├──────────────────┼──────────────┼──────────────────────────────────┼──────────────────────────────────────────┤
│ REAL (FLOAT4)    │ **4 Bytes**  │ 6 chữ số thập phân (Không chính  │ Nhiệt độ động cơ, Tốc độ xe, Cảm biến IoT│
│                  │              │ xác tuyệt đối - IEEE 754)        │ dung lượng thấp                          │
├──────────────────┼──────────────┼──────────────────────────────────┼──────────────────────────────────────────┤
│ DOUBLE PRECISION │ **8 Bytes**  │ 15 chữ số thập phân (Tính toán   │ **Tọa độ GPS (Latitude/Longitude)**,     │
│ (FLOAT8)         │              │ FPU phần cứng cực nhanh)         │ Trọng số Machine Learning, Khoảng cách km│
└──────────────────┴──────────────┴──────────────────────────────────┴──────────────────────────────────────────┘
```

### 🚨 BẪY KINH ĐIỂN NHẤT: TẠI SAO TUYỆT ĐỐI CẤM DÙNG `FLOAT` ĐỂ LƯU TIỀN BẠC?
* Các kiểu `FLOAT / DOUBLE PRECISION` biểu diễn số thực dưới dạng nhị phân theo chuẩn **IEEE 754**. Trong hệ nhị phân, các phân số thập phân như $0.1$ hoặc $0.7$ là số vô hạn tuần hoàn $\implies$ **Luôn xảy ra sai số làm tròn (Floating-point Rounding Error)**.
* **Minh họa sự cố**:
  ```sql
  -- Chạy thử trên PostgreSQL:
  SELECT (0.1::FLOAT8 + 0.2::FLOAT8) = 0.3::FLOAT8; -- TRẢ VỀ: FALSE! (Vì kết quả là 0.30000000000000004)
  
  -- Khi dùng NUMERIC:
  SELECT (0.1::NUMERIC + 0.2::NUMERIC) = 0.3::NUMERIC; -- TRẢ VỀ: TRUE!
  ```
* $\implies$ Nếu dùng `FLOAT` tính doanh thu 10 triệu đơn hàng, sau 1 năm số dư kế toán sẽ bị lệch hàng chục triệu đồng!
* **Quy chuẩn**: 
  * Lưu tiền Việt Nam Đồng (VND): `NUMERIC(15, 2)` (15 chữ số, 2 số thập phân $\to$ lưu tối đa $9.999$ tỷ đồng).
  * Tọa độ GPS xe tải: `DOUBLE PRECISION` (hoặc `NUMERIC(9, 6)` để chính xác đến $0.11\text{ mét}$).

---

# NHÓM 3: CÁC KIỂU CHUỖI KÝ TỰ & DANH MỤC (STRING & CHARACTER TYPES)

```
┌───────────────┬──────────────────────────┬─────────────────────────────────────────────────────────────────────┐
│ Kiểu Dữ Liệu  │ Dung Lượng Lưu Trữ       │ Cơ Chế Hoạt Động & Khuyên Dùng                                      │
├───────────────┼──────────────────────────┼─────────────────────────────────────────────────────────────────────┤
│ CHAR(n)       │ **Cố định n Bytes**      │ Đệm khoảng trắng (Space-padded) cho đủ n ký tự. **Chỉ dùng cho mã   │
│               │                          │ cố định độ dài**: Mã quốc gia `VN` (`CHAR(2)`), Mã tiền tệ `VND`    │
├───────────────┼──────────────────────────┼─────────────────────────────────────────────────────────────────────┤
│ VARCHAR(n)    │ **Độ dài thật + 1-4 B**  │ Chuỗi có giới hạn tối đa n ký tự. Giúp Validate dữ liệu đầu vào:    │
│               │                          │ `VARCHAR(20)` cho Biển số xe, `VARCHAR(100)` cho Email              │
├───────────────┼──────────────────────────┼─────────────────────────────────────────────────────────────────────┤
│ TEXT /        │ **Độ dài thật + 1-4 B**  │ Chuỗi không giới hạn độ dài (Tối đa 1GB). Dùng cho: Ghi chú sửa     │
│ VARCHAR (unb.)│                          │ chữa, Mô tả hư hỏng xe, Nội dung bài viết                           │
├───────────────┼──────────────────────────┼─────────────────────────────────────────────────────────────────────┤
│ ENUM          │ **4 Bytes nội bộ**       │ Kiểu danh mục định nghĩa trước. Rất nhẹ, kiểm soát chặt giá trị:    │
│               │                          │ Trạng thái đơn (`'DRAFT'`, `'IN_PROGRESS'`, `'DONE'`, `'CANCELLED'`)│
└───────────────┴──────────────────────────┴─────────────────────────────────────────────────────────────────────┘
```

### 💡 Giải Mã Huyền Thoại Hiệu Năng: `VARCHAR(255)` vs `TEXT` Trong PostgreSQL:
* **Huyền thoại sai lầm**: Nhiều người nghĩ `VARCHAR(255)` chạy nhanh hơn `TEXT`.
* **Sự thật kiến trúc PostgreSQL**: Về mặt lưu trữ vật lý bên dưới (Underlying Storage Engine), `VARCHAR(n)`, `VARCHAR` và `TEXT` **ĐỀU DÙNG CHUNG CÙNG MỘT CẤU TRÚC DỮ LIỆU (Cấu trúc `varlena` có header 1 hoặc 4 bytes)**. Hiệu năng đọc/ghi của chúng là **hoàn toàn giống nhau $100\%$**.
* Điểm khác biệt duy nhất: `VARCHAR(n)` có thêm bước kiểm tra độ dài chuỗi khi Insert/Update.
* **Ưu thế vượt trội của `ENUM`**:
  ```sql
  CREATE TYPE order_status_enum AS ENUM ('PENDING', 'PROCESSING', 'COMPLETED', 'CANCELLED');
  ```
  Lưu `order_status_enum` chỉ tốn đúng **4 Bytes** trên đĩa (thay vì tốn $10-15\text{ Bytes}$ nếu lưu dạng chuỗi `VARCHAR`), giúp nén bảng nhỏ hơn $30\%$ và tăng tốc độ lọc Index Scan đáng kể.

---

# NHÓM 4: CÁC KIỂU THỜI GIAN & NGÀY THÁNG (DATE & TIME TYPES)

```
┌──────────────────────────┬──────────────┬──────────────────────────────────────────────────────────────────────┐
│ Kiểu Dữ Liệu             │ Dung Lượng   │ Định Dạng & Ý Nghĩa Kỹ Thuật                                         │
├──────────────────────────┼──────────────┼──────────────────────────────────────────────────────────────────────┤
│ DATE                     │ **4 Bytes**  │ `YYYY-MM-DD` (Ví dụ: `2026-08-19`). Dùng cho Ngày sinh, Ngày hóa đơn│
├──────────────────────────┼──────────────┼──────────────────────────────────────────────────────────────────────┤
│ TIME                     │ **8 Bytes**  │ `HH:MI:SS.SSSSSS` (Ví dụ: `14:30:00`). Giờ mở cửa trạm               │
├──────────────────────────┼──────────────┼──────────────────────────────────────────────────────────────────────┤
│ TIMESTAMP (WITHOUT TZ)   │ **8 Bytes**  │ Lưu thời gian "chết" không có múi giờ. **Dễ gây lỗi khi đa quốc gia**│
├──────────────────────────┼──────────────┼──────────────────────────────────────────────────────────────────────┤
│ TIMESTAMPTZ (WITH TZ)    │ **8 Bytes**  │ **CHUẨN VÀNG**: PostgreSQL tự động chuyển đổi sang UTC khi lưu trữ   │
│                          │              │ và convert sang múi giờ client khi hiển thị!                         │
├──────────────────────────┼──────────────┼──────────────────────────────────────────────────────────────────────┤
│ INTERVAL                 │ **16 Bytes** │ Lưu khoảng thời gian: `'30 minutes'`, `'1 day'`, `'3 months'`        │
└──────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────┘
```

### 🚨 Bẫy Múi Giờ (Timezone Trap):
* Nếu dùng `TIMESTAMP` (Without Timezone), khi server đặt tại Singapore (UTC+8) ghi dữ liệu `2026-08-19 14:00:00`, dev ở Việt Nam (UTC+7) đọc lên vẫn thấy `14:00:00` $\implies$ Bị lệch 1 tiếng thực tế!
* $\implies$ **Quy tắc**: Toàn bộ hệ thống giao dịch, Telemetry IoT xe tải, Audit log **BẮT BUỘC dùng `TIMESTAMPTZ`**.

---

# NHÓM 5: LOGIC, ĐỊNH DANH DUY NHẤT & BÁN CẤU TRÚC (BOOLEAN, UUID, JSON/JSONB)

```
┌───────────────┬──────────────────────────┬─────────────────────────────────────────────────────────────────────┐
│ Kiểu Dữ Liệu  │ Dung Lượng Lưu Trữ       │ Đặc Tính & Ứng Dụng Thực Tế                                         │
├───────────────┼──────────────────────────┼─────────────────────────────────────────────────────────────────────┤
│ BOOLEAN       │ **1 Byte**               │ Lưu `TRUE`, `FALSE`, hoặc `NULL` (3 trạng thái). Cờ `is_active`     │
├───────────────┼──────────────────────────┼─────────────────────────────────────────────────────────────────────┤
│ UUID          │ **16 Bytes nhị phân**    │ Định danh phân tán toàn cầu 128-bit (`a0eebc99-9c0b-4ef8-...`).     │
│               │ (vs 36 Bytes chuỗi)      │ Cực kỳ tối ưu để làm ID phân tán chống xung đột khi offline.        │
├───────────────┼──────────────────────────┼─────────────────────────────────────────────────────────────────────┤
│ JSON          │ Biến đổi (Text thô)      │ Lưu nguyên bản chuỗi JSON text. Khi query `->>` phải parse lại từ   │
│               │                          │ đầu. **Rất chậm khi truy vấn, không hỗ trợ Indexing.**              │
├───────────────┼──────────────────────────┼─────────────────────────────────────────────────────────────────────┤
│ JSONB         │ Biến đổi (Binary parsed) │ **CHUẨN VÀNG CHO BÁN CẤU TRÚC**: Phân rã JSON thành nhị phân.       │
│               │                          │ **Hỗ trợ GIN Index siêu nhanh**, tự loại bỏ khoảng trắng và key trùng│
└───────────────┴──────────────────────────┴─────────────────────────────────────────────────────────────────────┘
```

### 💡 So Sánh Sức Mạnh Của `JSONB` với GIN Index:
Khi lưu các thông số cảm biến xe tải thay đổi linh hoạt (`{"brake_temp": 120, "tire_pressure": 32, "oil_level": "good"}`):
```sql
-- Tạo bảng với JSONB
CREATE TABLE truck_sensor_data (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    truck_id INT NOT NULL,
    payload JSONB NOT NULL
);

-- Tạo chỉ mục GIN trên toàn bộ cấu trúc JSONB
CREATE INDEX idx_sensor_payload_gin ON truck_sensor_data USING GIN (payload);

-- Truy vấn tìm xe có nhiệt độ phanh > 100 độ chạy trong < 1ms:
SELECT * FROM truck_sensor_data WHERE payload @> '{"oil_level": "good"}';
```

---

# NHÓM 6: CÁC KIỂU DỮ LIỆU ĐẶC THÙ (POSTGIS & AN NINH MẠNG INET)

1. **Kiểu Không Gian PostGIS (`GEOMETRY` / `GEOGRAPHY`)**:
   * Lưu trữ tọa độ không gian `Point(Longitude, Latitude)`.
   * Hỗ trợ hàm không gian PostGIS: `ST_DWithin()`, `ST_Distance()` kết hợp chỉ mục không gian **GiST (Generalized Search Tree)** để tìm các xe tải trong bán kính 5km quanh trạm.
2. **Kiểu Mạng & An Ninh Mạng (`INET`, `CIDR`, `MACADDR`) — Đặc thù VCS**:
   * `INET` (7 hoặc 19 Bytes): Lưu địa chỉ IPv4 / IPv6 và subnet mask. Tự động kiểm tra tính hợp lệ của IP, hỗ trợ toán tử tìm kiếm IP thuộc dải mạng (`WHERE client_ip << '192.168.1.0/24'`).
   * `MACADDR` (6 Bytes): Lưu địa chỉ MAC của card mạng máy chủ (`08:00:2b:01:02:03`).

---

# NHÓM 7: BẢNG SO SÁNH ÁNH XẠ KIỂU DỮ LIỆU
### (PostgreSQL OLTP $\longleftrightarrow$ Apache PySpark $\longleftrightarrow$ HDFS Parquet DWH)

Khi xây dựng Data Pipeline từ PostgreSQL qua Kafka sang Spark và lưu vào Parquet Data Lake, việc ánh xạ kiểu dữ liệu chính xác là điều kiện sống còn để tránh lỗi **Schema Mismatch Exception**:

```
┌──────────────────────────────┬──────────────────────────────┬──────────────────────────────┬──────────────────────────┐
│ Nghiệp Vụ Thực Tế            │ PostgreSQL (OLTP)            │ Apache PySpark (DataFrame)   │ Apache Parquet (Lakehouse│
├──────────────────────────────┼──────────────────────────────┼──────────────────────────────┼──────────────────────────┤
│ Khóa Surrogate Key DWH       │ `BIGINT`                     │ `LongType()`                 │ `INT64`                  │
│ Mã danh mục nhỏ (Tháng, Status)│ `SMALLINT`                 │ `ShortType()`                │ `INT32`                  │
│ Số tiền Hóa đơn / Doanh thu  │ `NUMERIC(15, 2)`             │ `DecimalType(15, 2)`         │ `FIXED_LEN_BYTE_ARRAY`   │
│ Tọa độ GPS / Vận tốc xe      │ `DOUBLE PRECISION`           │ `DoubleType()`               │ `DOUBLE`                 │
│ Tên khách hàng / Mã xe       │ `VARCHAR(100)` / `TEXT`      │ `StringType()`               │ `BYTE_ARRAY (UTF8)`      │
│ Ngày hóa đơn                 │ `DATE`                       │ `DateType()`                 │ `INT32 (DATE)`           │
│ Thời gian giao dịch chuẩn    │ `TIMESTAMPTZ`                │ `TimestampType()`            │ `INT64 (TIMESTAMP_MICROS)│
│ Cờ trạng thái active         │ `BOOLEAN`                    │ `BooleanType()`              │ `BOOLEAN`                │
│ Danh sách linh kiện sửa chữa │ `JSONB` / Mảng `INT[]`       │ `ArrayType(StructType(...))` │ `LIST / GROUP`           │
└──────────────────────────────┴──────────────────────────────┴──────────────────────────────┴──────────────────────────┘
```

---

### 🏆 TỔNG KẾT BẢN LĨNH SENIOR KHI TRẢ LỜI PHỎNG VẤN:
1. **Tiền tệ**: Luôn khẳng định dùng `NUMERIC / DECIMAL`, giải thích rõ lỗi mất độ chính xác của chuẩn nhị phân IEEE 754 trên `FLOAT`.
2. **Khóa ID**: Phân định rõ `BIGINT` cho bảng giao dịch khổng lồ và `INT / SMALLINT` cho bảng danh mục để tiết kiệm RAM Buffer Pool.
3. **Thời gian**: Luôn dùng `TIMESTAMPTZ` để tự động hóa UTC.
4. **Bán cấu trúc**: Luôn chọn `JSONB` thay vì `JSON` thô để khai thác sức mạnh của chỉ mục `GIN`.
5. **Chuỗi**: Khẳng định `VARCHAR(255)` và `TEXT` có hiệu năng như nhau trong PostgreSQL, tận dụng `ENUM` khi cần tối ưu bộ nhớ danh mục.
