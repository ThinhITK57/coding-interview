# Ticket 6 — Push-Based Serving Layer (Redis Pub/Sub -> WebSocket Backend)

> **Mục tiêu**:
> 1. Thấu hiểu và triển khai kiến trúc **Push-based Serving Layer**: Thay vì Dashboard phải liên tục Polling (gửi request lặp lại) làm tăng tải Redis/DB, lớp Backend Async Python tự động **subscribe Redis Pub/Sub** và **Chủ động PUSH (đẩy)** dữ liệu tổng hợp mới xuống Dashboard ngay khi Airflow/Spark batch job hoàn tất.
> 2. Quản lý trạng thái kết nối & Tự động đồng bộ khi Reconnect (Initial State Sync).
> 3. Dashboard Executive trực quan (Vanilla CSS Glassmorphic + Chart.js) cập nhật số liệu thời gian thực không cần F5 / Reload trang.

---

## Cấu trúc thư mục Ticket 6

```
fleet-platform/
└── websocket_backend/
    ├── websocket_serving_backend.py ← WebSocket Async Python Server (Port 8765)
    ├── dashboard_mockup.html        ← Modern Executive Live Dashboard UI
    └── TICKET-6-GUIDE.md            ← Hướng dẫn chạy & Interview Q&A
```

---

## 1. Yêu cầu môi trường (Pre-requisites)

1. Python packages `websockets` và `redis` (hỗ trợ `redis.asyncio`):
   ```bash
   pip install websockets redis
   ```
2. Redis Server đang chạy trên máy `master` (port 6379).

---

## 2. Bước 1: Khởi động WebSocket Serving Backend

Trên máy `master`:

```bash
cd /path/to/fleet-platform/websocket_backend

# Chạy server ở background hoặc terminal 1:
python3 websocket_serving_backend.py --port 8765
```

**Output mong đợi:**
```
=================================================================
 Fleet Platform — WebSocket Serving Backend
 Listening on : ws://0.0.0.0:8765
 Redis Host   : master:6379
=================================================================
2026-07-29 22:20:00 [INFO] ✅ Connected to Redis successfully.
2026-07-29 22:20:00 [INFO] 📡 Redis Pub/Sub listener subscribed to 'channel:report-updates'. Waiting for events...
```

---

## 3. Bước 2: Mở Live Executive Dashboard trên Trình Duyệt

1. Mở file `dashboard_mockup.html` trong trình duyệt web (Chrome / Firefox / Edge).
2. Kiểm tra góc trên bên phải: Status badge hiển thị dấu chấm xanh lá **`Connected (Push Ready)`**.
3. Nhật ký tín hiệu ở cuối trang sẽ hiện log:
   `[System] Initializing WebSocket client connection to ws://master:8765...`
   `✅ WebSocket Connection Established. Live Push updates enabled.`
   `⚡ PUSH EVENT RECEIVED [INITIAL_STATE]...`

---

## 4. Bước 3: Thử nghiệm Real-Time Push End-to-End

Để chứng minh dữ liệu được **Push tự động từ Spark → Redis → WebSocket → Dashboard**:

Mở terminal thứ 2 trên máy `master` và chạy lệnh phát tín hiệu giả lập hoặc chạy job Spark:

```bash
# Phương án A: Phát tín hiệu Pub/Sub trực tiếp từ redis-cli
redis-cli -h master PUBLISH channel:report-updates '{"event":"TEST_PUSH","granularity":"monthly"}'

# Phương án B: Chạy lại PySpark Batch Job tổng hợp báo cáo
python3 /path/to/fleet-platform/analytics/jobs/batch_dwh_aggregation.py --granularity monthly
```

**Kết quả quan sát ngay lập tức trên Dashboard:**
1. Nhật ký Stream hiển thị dòng log mới `⚡ PUSH EVENT RECEIVED [REPORT_UPDATE]...`
2. Các ô KPI (Tổng doanh thu, Doanh thu dịch vụ, Doanh thu linh kiện, Tỉ suất lợi nhuận) **tự động thay đổi con số**.
3. Biểu đồ cột (Bar chart) và Biểu đồ tròn (Doughnut chart) **tự động chuyển động (Animate) cập nhật tỉ lệ mới** mà KHÔNG cần bấm F5 hay Reload trang!
