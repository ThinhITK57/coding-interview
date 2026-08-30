# 📚 TỔNG HỢP KIẾN THỨC NỀN TẢNG CHUYÊN SÂU (MIDDLE / SENIOR DE CHEAT-SHEET)

Tài liệu này tổng hợp toàn bộ các nguyên lý kiến trúc, trade-offs kỹ thuật, và các câu hỏi lý thuyết chiều sâu phục vụ ôn tập phỏng vấn Senior Data Engineer.

---

## 1. LOG-BASED CDC & KAFKA DEBEZIUM ARCHITECTURE

### 1.1 So sánh 4 phương pháp CDC (Change Data Capture)
| Tiêu chí | Log-Based CDC (Debezium) | Query-Based Polling | Trigger-Based CDC | Native Database Replication |
|---|---|---|---|---|
| **Cơ chế** | Đọc file WAL/Binlog ổ đĩa | `SELECT * WHERE updated_at > t` | Gắn `AFTER INSERT/UPDATE/DELETE` trigger | Replay WAL sang Standby DB |
| **Tải OLTP DB** | **0% Query Load** | Cao (Scan table/index) | Trung bình (Tăng ghi transaction) | Thấp (Physical I/O) |
| **Bắt DELETE** | ✅ Có (qua WAL `op: d`) | ❌ Không | ✅ Có (nếu lưu audit table) | ✅ Có |
| **Trạng thái trung gian**| ✅ Giữ 100% | ❌ Bị rò rỉ (chỉ thấy state cuối) | ✅ Giữ được | ✅ Giữ được |
| **Cột bị loại bỏ** | Cần `REPLICA IDENTITY FULL` | Không cần | Tự ghi vào audit table | Tùy chọn |

### 1.2 Kafka Multi-Broker Reliability Formulas
- **Replication Factor (RF)**: Số bản copy của mỗi Partition rải trên các Brokers khác nhau ($RF = 3$).
- **In-Sync Replicas (ISR)**: Tập hợp các Broker Replicas đang đồng bộ dữ liệu kịp thời với Leader Broker.
- **Minimum In-Sync Replicas (`min.insync.replicas = 2`)**: Số lượng ISR tối thiểu phải xác nhận ghi thành công trước khi Producer nhận ack.
- **Chịu lỗi tối đa (Fault Tolerance)**:
  $$\text{Max Brokers Tolerated Down} = RF - \text{min.insync.replicas} = 3 - 2 = 1 \text{ broker}$$
  Nếu 2 Brokers sập cùng lúc $\rightarrow$ $ISR < 2 \rightarrow$ Producer nhận lỗi `NotEnoughReplicasException`, bảo vệ dữ liệu không bao giờ bị ghi khi chỉ còn 1 bản copy đơn lẻ.

---

## 2. PYSPARK ARCHITECTURE, STREAMING & OPTIMIZATION

### 2.1 PySpark Internal Execution Engines
- **Catalyst Optimizer**: Trình tối ưu hóa câu lệnh SQL/DataFrame qua 4 bước: 
  `Analysis` $\rightarrow$ `Logical Optimization` $\rightarrow$ `Physical Planning` $\rightarrow$ `Code Generation (Janino)`.
  Tự động áp dụng *Predicate Pushdown* (đẩy lệnh WHERE xuống nguồn đọc) và *Projection Pruning* (chỉ chọn các cột cần thiết).
- **Tungsten Engine**: Trình quản lý bộ nhớ trực tiếp (Off-heap Memory Management) dùng định dạng UnsafeRow (binary), tránh chi phí Garbage Collection (GC) của Java Virtual Machine.

### 2.2 So sánh các thuật toán Spark Join
| Thuật toán Join | Điều kiện kích hoạt | Cơ chế hoạt động | Ưu/Nhược điểm |
|---|---|---|---|
| **Broadcast Hash Join (BHJ)** | 1 bảng nhỏ (< `spark.sql.autoBroadcastJoinThreshold` ~10MB) | Driver copy toàn bộ bảng nhỏ tới RAM của tất cả Executor | **Nhanh nhất**, 0% Shuffle qua mạng |
| **Sort Merge Join (SMJ)** | Bảng lớn Join bảng lớn (Default) | Shuffle cả 2 bảng theo Join Key, Sort key trên từng node rồi Merge | Cực kỳ ổn định cho Big Data, tốn chi phí Shuffle I/O |
| **Shuffle Hash Join** | 1 bảng vừa, 1 bảng lớn | Shuffle dữ liệu theo Join Key rồi dựng Hash Table trên RAM | Nhanh hơn SMJ nếu bảng vừa vừa RAM |

---

## 3. DATA WAREHOUSE MODELING & SCD TYPE 2

### 3.1 Kimball Dimensional Modeling Checklist
- **Fact Table**: Bảng chứa các số đo định lượng (Metrics: `total_amount`, `labor_cost`, `quantity`) và các Khóa ngoại (Foreign Keys) nối tới bảng Dim.
- **Dimension Table**: Bảng chứa bối cảnh mô tả (Context: `company_name`, `address`, `status`).
- **Conformed Dimension**: Bảng chiều chuẩn hóa dùng chung cho nhiều bảng Fact trong Data Warehouse (ví dụ `dim_date`, `dim_customer`).
- **Factless Fact Table**: Bảng Fact không chứa số đo định lượng, chỉ chứa các FK để ghi nhận sự kiện xảy ra (ví dụ: Sự kiện No-show hủy lịch hẹn sửa chữa).

### 3.2 So sánh các loại SCD (Slowly Changing Dimensions)
- **SCD Type 0**: Đóng băng dữ liệu (Retain Original). Không bao giờ thay đổi.
- **SCD Type 1**: Ghi đè dữ liệu (Overwrite). Mất hoàn toàn dữ liệu cũ trong lịch sử.
- **SCD Type 2**: Thêm dòng mới (Add New Row). Giữ 100% lịch sử bằng các cột `effective_date`, `expiration_date`, `is_current`. (Được áp dụng trong dự án này).
- **SCD Type 3**: Thêm cột mới (Add New Attribute). Chỉ lưu lại trạng thái trước đó (Previous Value).

---

## 4. APACHE AIRFLOW EXECUTOR & ORCHESTRATION

### 4.1 So sánh Airflow Executors
- **SequentialExecutor**: Chạy từng task tuần tự trên 1 process, dùng SQLite DB (Chỉ dùng test local).
- **LocalExecutor**: Chạy nhiều tasks song song dưới dạng Multiprocessing trên cùng 1 máy Airflow Server. Dùng Postgres Metadata DB. (Áp dụng trong dự án này).
- **CeleryExecutor**: Phân tán các task ra nhiều worker machines thông qua Message Queue (RabbitMQ / Redis Broker).
- **KubernetesExecutor**: Mỗi task được khởi tạo thành 1 Pod độc lập trên cụm Kubernetes, tự động scale down về 0 khi task chạy xong.

### 4.2 Airflow Task Lifecycle States
```
None -> Scheduled -> Queued -> Running -> Success / Failed / Upstream_Failed
```
- **`Upstream_Failed`**: Task bị bỏ qua không chạy vì task cha (upstream) phía trước bị thất bại.

---

## 5. PUSH-BASED SERVING VS PULL-BASED POLLING

### 5.1 So sánh 3 công nghệ Web Communication
| Tiêu chí | WebSockets | Server-Sent Events (SSE) | HTTP Long Polling |
|---|---|---|---|
| **Giao thức** | TCP `ws://` / `wss://` (Full-Duplex) | HTTP `text/event-stream` (Single-Direction) | HTTP Request/Response (Half-Duplex) |
| **Hướng truyền** | 2 chiều (Client $\leftrightarrow$ Server) | 1 chiều (Server $\rightarrow$ Client) | 1 chiều lặp lại |
| **Độ trễ** | **< 15 ms** (Rất thấp) | Thấp | Cao (Chờ timeout request) |
| **Tải Server** | Cực kỳ nhẹ (Giữ 1 TCP connection) | Nhẹ | Rất nặng (Liên tục tạo/đóng kết nối) |
| **Use case** | Live Dashboard, Real-time Chat | News Feed, Tỷ giá chứng khoán | Legacy Fallback |
