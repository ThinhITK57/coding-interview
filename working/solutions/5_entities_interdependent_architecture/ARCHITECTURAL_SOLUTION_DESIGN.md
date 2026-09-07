# THIẾT KẾ KIẾN TRÚC GIẢI PHÁP: 5 THỰC THỂ LIÊN KẾT PHỤ THUỘC
**Enterprise Data Ingestion & Transformation Architecture (Planview Clarizen Lakehouse)**

---

## 1. ĐẶT VẤN ĐỀ VÀ BỐI CẢNH DỰ ÁN

Trong hệ thống điều hành doanh nghiệp, dữ liệu không bao giờ đứng độc lập mà có sự liên hệ ràng buộc chặt chẽ. Hệ thống bao gồm 5 thực thể chính:
* **Task** (Công việc / Nhiệm vụ — đã hoàn thiện bóc tách 186 trường)
* **Project** (Dự án đầu tư / Dự án kinh doanh)
* **User** (Cán bộ nhân viên / Lãnh đạo)
* **Target** (Mục tiêu chiến lược / OKR / KPI)
* **Giao-Ban-Ket-Luan** (Biên bản giao ban / Kết luận cuộc họp / Chỉ đạo lãnh đạo / Action Items)

Ngày mai, khi bộ phận Phân tích Nghiệp vụ (BA) bàn giao danh sách chi tiết các trường của 4 mục còn lại (`Project`, `User`, `Target`, `Giao-Ban-Ket-Luan`), hệ thống cần có một **khung kiến trúc vững chắc sẵn sàng nạp ngay lập tức** mà không cần đập đi xây lại code core pipeline.

---

## 2. PHÂN TÍCH RỦI RO KIẾN TRÚC (ARCHITECTURAL RISK ANALYSIS)

Khi 5 endpoint có quan hệ dữ liệu với nhau, việc chạy cào dữ liệu thô sơ (Flat loop / Random Parallel) sẽ đối mặt với 3 rủi ro chết người:

### ⚠️ Rủi ro 1: Vi phạm trật tự phụ thuộc (Topological Dependency Violation)
Nếu cào `Giao-Ban-Ket-Luan` hoặc `Task` trước khi cào `Project` và `User`:
* Dữ liệu giao việc xuất hiện trong kho, nhưng thông tin dự án cha và nhân sự thực thi chưa có.
* Khi dbt thực hiện biến đổi dữ liệu, các phép JOIN sẽ bị thiếu dữ liệu cha.

### ⚠️ Rủi ro 2: Cơn ác mộng "Late-Arriving Data" trong chạy Tăng dần (Incremental Sync)
Trong chế độ cào tăng dần theo cửa sổ thời gian (`LastModified >= WINDOW_START`):
* **Tình huống thực tế**: Ngày hôm nay lãnh đạo họp giao ban và ban hành một Kết luận mới (`Giao-Ban-Ket-Luan`) chỉ đạo cho một Dự án đã khởi tạo từ 6 tháng trước (`PRJ-2025-01`).
* **Hệ quả**:
  * API `Giao-Ban-Ket-Luan` trả về bản ghi kết luận hôm nay (có trường `Project = "PRJ-2025-01"`).
  * API `Project` chỉ lọc các dự án có thay đổi hôm nay $\rightarrow$ **Hoàn toàn KHÔNG trả về `PRJ-2025-01`**!
  * Nếu dbt dùng `INNER JOIN` $\rightarrow$ Kết luận giao ban của lãnh đạo bị **biến mất hoàn toàn khỏi báo cáo** (**Silent Data Loss**)!
  * Nếu dbt dùng `LEFT JOIN` $\rightarrow$ Tên dự án bị `NULL`, dashboard lãnh đạo hiển thị số liệu bị khuyết!

### ⚠️ Rủi ro 3: Bế tắc do Vòng lặp quan hệ (Circular Dependency Deadlock)
Trong bảng `User`, có trường `DirectManager` tự trỏ lại chính `User` (Self-referencing hierarchy). Trong bảng `Project`, Quản trị dự án trỏ tới `User`. Nếu không phân biệt giữa **Quan hệ Khóa cứng (Hard Extraction Dependency)** và **Quan hệ Dữ liệu mềm (Soft Reference)**, bộ lập lịch sẽ rơi vào vòng lặp vô tận (Infinite Loop / Deadlock).

---

## 3. BỘ GIẢI PHÁP KIẾN TRÚC 3 TẦNG (3-TIER ENTERPRISE SOLUTION)

Để triệt tiêu hoàn toàn 3 rủi ro trên, giải pháp được thiết kế theo 3 tầng bảo vệ độc lập:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ TẦNG 1: ĐIỀU PHỐI THEO LÀN SÓNG TÔ-PÔ (TOPOLOGICAL WAVE ORCHESTRATION)       │
│                                                                             │
│  Wave 1: [User] + [Project]            (Chạy song song 2 bảng gốc)          │
│     │                                                                       │
│     ▼                                                                       │
│  Wave 2: [Task] + [Target]             (Chờ Wave 1 hoàn thành mới chạy)     │
│     │                                                                       │
│     ▼                                                                       │
│  Wave 3: [Giao-Ban-Ket-Luan]           (Chờ Wave 2 hoàn thành mới chạy)     │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ TẦNG 2: XỬ LÝ LATE-ARRIVING DATA BẰNG KIMBALL INFERRED DIMENSION            │
│ (Ralph Kimball Inferred Dimension Pattern - Spark DedupEngine)              │
│                                                                             │
│  - Phát hiện khóa ngoại con (Project='PRJ-2025-01') chưa có trong kho?      │
│  - Tự động sinh bản ghi giả lập (Inferred Stub Record) với Epoch 1970.     │
│  - Bảo toàn 100% phép JOIN không bị mất dòng.                               │
│  - Khi bản ghi thật về sau, Spark Window Ranking tự động đè lên bản ghi tạm.│
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ TẦNG 3: LAKEHOUSE STORAGE & DBT REFERENTIAL INTEGRITY TESTING               │
│                                                                             │
│  - Bronze: Lưu trữ Parquet phân vùng ingest_date=YYYY-MM-DD.                │
│  - Silver/Gold: dbt duy trì Bảng Chiều Toàn Cục (dim_projects, dim_users).  │
│  - dbt test: relationships tự động kiểm toán toàn vẹn khóa ngoại.           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### TẦNG 1: Điều phối theo Làn sóng Tô-pô (Topological Waves)

Pipeline sử dụng **Thuật toán Kahn** để tự động giải mã đồ thị phụ thuộc DAG từ file `tables_registry_5_entities.json` thành 3 làn sóng thực thi:

| Làn sóng (Wave) | Danh sách Thực thể | Lý do phân loại & Cơ chế chạy |
| :--- | :--- | :--- |
| **Wave 1** | `User`, `Project` | **Master Dimensions**: Bảng danh mục gốc, không phụ thuộc vào bảng khác. Chạy **song song (Concurrency = 2)** để rút ngắn 50% tổng thời gian cào. |
| **Wave 2** | `Task`, `Target` | **Core Entities & Objectives**: Phụ thuộc vào `Project` và `User`. Chỉ kích hoạt khi **Wave 1 đã nạp xong 100%**. |
| **Wave 3** | `Giao-Ban-Ket-Luan` | **Action Items & Decisions**: Điểm hội tụ của toàn bộ 4 thực thể trên. Kích hoạt sau cùng khi toàn bộ hệ sinh thái dự án đã sẵn sàng. |

* **Đặc tính kỹ thuật**: Nếu 1 endpoint trong Wave 1 thất bại (ví dụ API User gặp lỗi 503), Prefect/Worker sẽ **dừng ngay lập tức** các Wave sau (Fail-Fast), bảo vệ kho dữ liệu không bị nạp rác.

---

### TẦNG 2: Hóa giải bài toán Late-Arriving Data bằng "Kimball Inferred Dimensions"

Module [race_condition_router.py](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/transform/race_condition_router.py) trong mã nguồn áp dụng chính xác phương pháp luận của Ralph Kimball:

1. **Sinh bản ghi danh nghĩa (Inferred Stub Generation)**:
   * Khi trích xuất `Giao-Ban-Ket-Luan`, nếu phát hiện `Project = "PRJ-999"` chưa tồn tại trong bảng `Project`:
   * Spark DataFrame tự động sinh một bản ghi đại diện (Stub) vào bảng `Project`:
     ```json
     {
       "SYSID": "PRJ-999",
       "Name": "Inferred Stub [PRJ-999] - Pending API Sync",
       "LastModified": "1970-01-01T00:00:00Z",
       "_is_inferred": true,
       "_inferred_timestamp": "2026-09-08T01:45:00Z"
     }
     ```
2. **Bảo toàn tính toán trong dbt**:
   * Toàn bộ các câu lệnh `JOIN` giữa `Giao-Ban-Ket-Luan` và `dim_projects` đều khớp 100%. Báo cáo chỉ đạo giao ban của lãnh đạo xuất hiện đầy đủ trên Dashboard mà không bị rụng bất kỳ dòng nào.
3. **Cơ chế Tự chữa lành (Self-Healing via Window Ranking)**:
   * Khi API `Project` được chạy full sync hoặc dự án đó được cập nhật sau này, bản ghi thật được kéo về.
   * Thuật toán **Deduplication bằng Window Ranking Spark 2.3.2**:
     ```sql
     ROW_NUMBER() OVER (PARTITION BY SYSID ORDER BY LastModified DESC)
     ```
   * Do `LastModified` của bản ghi thật (năm 2026) luôn luôn lớn hơn Epoch năm 1970 của bản ghi tạm, bản ghi thật sẽ **tự động ghi đè hoàn toàn** bản ghi tạm. Hệ thống tự hoàn thiện dữ liệu một cách trơn tru!

---

### TẦNG 3: Kiểm soát tính Toàn vẹn tại Tầng dbt (Referential Integrity Auditing)

1. **Bảng Chiều Toàn Cục (Accumulated Dimension Table)**:
   * Tầng Bronze cào theo ngày, nhưng dbt duy trì các bảng Dimension toàn cục (`dim_users`, `dim_projects`, `dim_targets`, `dim_tasks`) tổng hợp toàn bộ lịch sử từ ngày đầu tiên.
   * Nhờ đó, bất kỳ bản ghi `Giao-Ban-Ket-Luan` nào sinh ra cũng đều có thể tra cứu ngược về dự án trong quá khứ.
2. **Bộ kiểm thử tự động (`dbt test`)**:
   * Khai báo trong `schema.yml`:
     ```yaml
     version: 2
     models:
       - name: fct_meeting_conclusions_action_items
         columns:
           - name: project_id
             tests:
               - relationships:
                   to: ref('dim_projects')
                   field: project_id
           - name: task_id
             tests:
               - relationships:
                   to: ref('dim_tasks')
                   field: task_id
     ```
   * Bất kỳ vi phạm khóa ngoại nào vượt quá ngưỡng cho phép (ví dụ > 1%) sẽ được phát hiện ngay tại giai đoạn build dbt, ngăn chặn việc đưa dữ liệu sai lệch lên báo cáo quản trị.

---

## 4. HƯỚNG DẪN THỰC THI CHO NGÀY MAI (ACTION PLAN FOR TOMORROW)

Khi BA bàn giao danh sách trường của 4 mục còn lại, bạn chỉ cần thực hiện 3 bước đơn giản:

1. **Bước 1: Điền trường vào `tables_registry_5_entities.json`**:
   * Mở file `tables_registry_5_entities.json`.
   * Dán danh sách trường vào mảng `"fields"` của từng thực thể (`User`, `Project`, `Target`, `Giao_Ban_Ket_Luan`).
2. **Bước 2: Chạy kiểm thử khô (Dry-run)**:
   ```bash
   python prefect_flow.py --all --dry-run
   ```
   * Kiểm tra xem 3 Wave thực thi đã được nhận diện chính xác hay chưa.
3. **Bước 3: Kích hoạt Pipeline chạy thực tế**:
   * Chạy trực tiếp:
     ```bash
     python prefect_flow.py --all --env prod
     ```
   * Hoặc khởi động qua Docker Worker:
     ```bash
     docker-compose up -d
     ```
