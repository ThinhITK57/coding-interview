# TÀI LIỆU KỸ THUẬT: CHIẾN LƯỢC CRAWL DỮ LIỆU TỪ CLARIZEN API
## (DETAILED DATA INGESTION & EXTRACTION SPECIFICATION)

> **Dự án:** `crawler-prefecthq-02`  
> **Nguồn cấp:** Planview Clarizen REST API v2.0  
> **Điểm lưu trữ:** Lakehouse Bronze (MinIO S3 / Parquet)  
> **Vị trí file:** `working/crawler-prefecthq-02/crawler-prefecthq/docs/CRAWL_STRATEGY_SPEC.md`  

---

## 1. Mục Tiêu Chiến Lược
1. **Tối ưu hóa tải trên Clarizen API:** Giảm thiểu 90% số lượng request dư thừa nhờ cơ chế phân tầng tần suất dựa trên tốc độ biến động thực tế của từng thực thể nghiệp vụ.
2. **Loại bỏ nguy cơ Race Condition:** Điều phối thứ tự cào dữ liệu theo các sóng phụ thuộc (DAG execution waves).
3. **Đảm bảo tính toàn vẹn lịch sử:** Kết hợp cào tăng dần (Lookback buffer) với chu kỳ đối soát toàn diện (Weekly full reconciliation).

---

## 2. Chi Tiết Từng Luồng Cào Dữ Liệu (Endpoint Extraction Specs)

### 2.1. Thực thể `tasks` (Công việc dự án)
- **Đặc thù nghiệp vụ:** Là thực thể có tần suất biến động cao nhất. Hàng trăm nhân viên cập nhật tiến độ, chấm timesheet, và Jira sync liên tục trong ngày làm việc.
- **Tần suất cào:** Mỗi 2 tiếng một lần trong khung giờ làm việc (08:00 đến 18:00, từ Thứ Hai đến Thứ Sáu).
- **Cơ chế:** **Incremental Extraction** theo trường `LastUpdatedOn`.
- **Lookback Window Buffer:** 15 phút (để bắt kịp các bản ghi vừa được commit ở server Clarizen mà có độ trễ đồng bộ).
- **Phục hồi toàn diện (Reconciliation):** 1 lần/tuần vào lúc 23:00 tối Chủ Nhật chạy ở chế độ **Full Sync** (tự động gỡ bỏ bộ lọc `where` theo trường watermark) để quét sạch các bản ghi bị xóa mềm hoặc cập nhật ngoại lệ.
- **Prefect Deployments:**
  - `epm-tasks-incremental-2h` (Cron: `0 8-18/2 * * 1-5`)
  - `epm-tasks-full-weekly` (Cron: `0 23 * * 0`)

### 2.2. Thực thể `projects` (Dự án) & `targets` (Chỉ tiêu định lượng)
- **Đặc thù nghiệp vụ:**
  - Dự án thường được cập nhật trạng thái vào 2 mốc chính: giờ nghỉ trưa (12:00) và cuối ngày làm việc (18:30).
  - Chỉ tiêu định lượng (Targets) đi liền với tiến độ dự án và kỳ họp giao ban.
- **Tần suất cào:** 2 lần / ngày.
- **Lịch trình cụ thể:**
  - Ca trưa: `projects` chạy lúc 12:00, `targets` chạy lúc 12:15.
  - Ca tối: `projects` chạy lúc 18:30, `targets` chạy lúc 18:45.
- **Cơ chế:** **Incremental Extraction** theo trường `LastUpdatedOn` với Lookback 30 phút.
- **Prefect Deployments:**
  - `epm-projects-targets-midday` / `epm-targets-midday`
  - `epm-projects-targets-evening` / `epm-targets-evening`

### 2.3. Thực thể `objectives` (Mục tiêu BSC) & `c_assignments` (Phiếu giao việc)
- **Đặc thù nghiệp vụ:**
  - Cây mục tiêu BSC cấp chiến lược có dung lượng nhỏ (khoảng 200 - 500 bản ghi), thay đổi theo quý hoặc theo năm.
  - Phiếu giao nhiệm vụ (PGNV) gắn với các trọng số và liên kết cha-con phức tạp.
- **Tần suất cào:** 1 lần / ngày vào rạng sáng.
- **Cơ chế:** **Full Sync** (Cào toàn bộ bảng).
- **Thời gian chạy:**
  - `objectives`: 01:00 AM.
  - `c_assignments`: 01:30 AM.
- **Lợi ích:** Quá trình cào diễn ra dưới 15 giây, triệt tiêu hoàn toàn nguy cơ lệch trọng số hoặc đứt gãy quan hệ phân cấp cây mục tiêu.
- **Prefect Deployments:**
  - `epm-objectives-full-daily` (Cron: `0 1 * * *`)
  - `epm-assignments-full-daily` (Cron: `30 1 * * *`)

### 2.4. Thực thể `user_access_log` (Nhật ký truy cập hệ thống)
- **Đặc thù nghiệp vụ:** Dữ liệu log đăng nhập chỉ phát sinh theo ngày và có tính chất append-only (chỉ thêm mới, không sửa).
- **Tần suất cào:** 1 lần / ngày vào lúc 02:00 AM rạng sáng.
- **Cơ chế:** **Incremental Extraction** lấy trọn vẹn toàn bộ log của ngày hôm trước (T-1, từ 00:00:00 đến 23:59:59 của ngày T-1).
- **Prefect Deployment:**
  - `epm-access-log-daily` (Cron: `0 2 * * *`)

### 2.5. Master Ingestion DAG Pipeline
- **Thời gian chạy:** 03:00 AM rạng sáng mỗi ngày.
- **Cơ chế:** Quét qua toàn bộ 5 bảng theo các sóng phụ thuộc (Dependency Waves) được tính toán từ `tables_registry.json`:
  - **Sóng 1 (Độc lập):** `objectives`
  - **Sóng 2 (Phụ thuộc cấp 1):** `projects`, `c_assignments`
  - **Sóng 3 (Phụ thuộc cấp 2):** `targets`
  - **Sóng 4 (Phụ thuộc cấp 3):** `tasks`
- **Prefect Deployment:**
  - `epm-all-endpoints-dag-master` (Cron: `0 3 * * *`)

---

## 3. Cơ Chế Checkpoint và Khắc Phục Sự Cố
- Trạng thái con trỏ phân trang được lưu tại thư mục `./checkpoints/{endpoint_name}.json`.
- Cấu trúc Checkpoint:
  ```json
  {
    "endpoint": "tasks",
    "batch_id": "20260915_080000_tasks",
    "last_watermark": "2026-09-15T07:58:32Z",
    "last_offset": 500,
    "current_page": 3,
    "completed": false
  }
  ```
- Khi tiến trình cào gặp sự cố mạng hoặc khởi động lại, `CheckpointStore` sẽ tự động nạp `last_offset` và tiếp tục gửi request từ trang dở dang, đảm bảo **At-least-once extraction** mà không làm trùng dữ liệu trên tầng Silver.
