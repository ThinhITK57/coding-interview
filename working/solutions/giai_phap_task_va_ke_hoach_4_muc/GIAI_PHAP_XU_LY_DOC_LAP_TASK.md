# GIẢI PHÁP KỸ THUẬT: XỬ LÝ ĐỘC LẬP THỰC THỂ TASK
**Căn cứ 100% trên 186 trường dữ liệu thực tế tại `task_data_raw.txt`**

---

## 1. HIỆN TRẠNG DỮ LIỆU & NGUYÊN TẮC BẤT DI BẤT DỊCH

* **Thực tế hiện tại**:
  * Chúng ta **CHỈ CÓ DUY NHẤT** dữ liệu của mục **Task** (186 trường được mô tả chi tiết trong `task_data_raw.txt`).
  * 4 mục còn lại (`Project`, `Giao-ban-kết-luận`, `Target`, `User`) **hoàn toàn CHƯA CÓ THÔNG TIN**: chưa có API endpoint, chưa có cấu trúc body, chưa có danh sách trường và chưa có dữ liệu mẫu.
* **Nguyên tắc kỹ thuật**:
  * **Tuyệt đối không phỏng đoán** schema hay bịa đặt trường dữ liệu cho 4 mục kia.
  * Thiết kế pipeline sao cho mục **Task chạy độc lập 100%**, hoàn thiện từ tầng Crawl $\rightarrow$ Spark Flatten 186 trường $\rightarrow$ Storage Trino $\rightarrow$ dbt Model (EVM, Snapshot tiến độ).
  * Xử lý các trường mang tính chất tham chiếu trong bảng Task một cách an toàn để kho dữ liệu không bị lỗi khi 4 bảng kia chưa xuất hiện.

---

## 2. PHÂN LOẠI 186 TRƯỜNG CỦA TASK TỪ `task_data_raw.txt`

Trong 186 trường thực tế của Task, dữ liệu được chia làm 2 nhóm rõ rệt:

### Nhóm 1: Dữ liệu Nội tại của Task (Self-Contained Data — Chiếm đa số)
Đây là các trường phục vụ tính toán trực tiếp mà không cần phụ thuộc vào bất kỳ bảng nào khác:
* **Quản trị Giá trị Thu được (EVM - Earned Value Management)**:
  * `EarnedValue` ($EV$), `PlannedBudget` ($PV$), `ActualCost` ($AC$).
  * `CPI` (Cost Performance Index), `SPI` (Schedule Performance Index).
  * `CurrencyEAC`, `CurrencyETC`, `TCPI`.
* **Tiến độ & Trạng thái Công việc**:
  * `PercentCompleted`, `ExpectedProgress`, `TrackStatus`, `State`, `InternalStatus`.
* **Thời gian & Lịch trình**:
  * `StartDate`, `DueDate`, `ActualStartDate`, `ActualEndDate`, `Duration`, `ActualDuration`.
  * Bộ trường Baseline: `BaselineStartDate`, `BaselineDueDate`, `BaselineWork`.
* **Khối lượng & Giờ công**:
  * `Work`, `BudgetedHours`, `ActualEffort`, `RemainingEffort`.
* **Tài chính & Hạch toán**:
  * Phân tách chi phí: `BudgetCostLR` (nhân công) vs `BudgetCostNLR` (ngoài nhân công), `CAPEX` vs `OPEX`.

$\rightarrow$ **Toàn bộ Nhóm 1 này được xử lý, làm sạch và đưa vào mô hình dbt ngay lập tức mà không cần bất kỳ thông tin nào từ bên ngoài.**

---

### Nhóm 2: Các trường có tính chất "Tham chiếu" (Reference Attributes)
Khi soi chiếu file `task_data_raw.txt`, chúng ta thấy có một số trường mang giá trị định danh/mã trỏ ra các đối tượng bên ngoài:

| Trường trong Task | Thuộc nhóm trong `task_data_raw.txt` | Ý nghĩa nghiệp vụ thực tế | Bản chất kỹ thuật khi xử lý |
| :--- | :--- | :--- | :--- |
| `Project`, `ParentProject` | Mục 1: Cấu trúc phân cấp | Mã dự án cha mà Task này trực thuộc | Lưu trữ nguyên trạng dưới dạng `STRING` (mã tham chiếu) |
| `Parent`, `Phase` | Mục 1: Cấu trúc phân cấp | Đối tượng cha trực tiếp / Giai đoạn dự án | Lưu trữ nguyên trạng dưới dạng `STRING` |
| `Manager`, `EntityOwner` | Mục 7: Cộng tác & Phân quyền | Mã người dùng phụ trách / sở hữu Task | Lưu trữ nguyên trạng dưới dạng `STRING` |
| `CreatedBy`, `LastUpdatedBy` | Mục 7: Siêu dữ liệu hệ thống | Mã người dùng tạo và sửa đổi | Lưu trữ nguyên trạng dưới dạng `STRING` |
| `Milestone`, `Deliverable` | Mục 1: Thuộc tính phân loại | Đánh dấu mốc quan trọng / sản phẩm bàn giao | Lưu trữ kiểu `BOOLEAN` / `STRING` của Task |
| `ExternalID` | Mục 1: Định danh hệ thống | Mã định danh từ hệ thống ngoài (văn bản/cuộc họp) | Lưu trữ nguyên trạng dạng `STRING` |
| `Overview1` $\rightarrow$ `Overview5` | Mục 7: Mô tả mở rộng | Các trường văn bản ghi chú chỉ đạo/nội dung | Lưu trữ kiểu `STRING` |

---

## 3. GIẢI PHÁP KỸ THUẬT: ĐẢM BẢO TASK CHẠY HOÀN TOÀN ĐỘC LẬP

### 1. Tách biệt lỏng (Loose Coupling) tại tầng Ingestion & Storage
* **Không ép buộc Foreign Key**: Trong Lakehouse (Trino / Parquet), bảng `Task` được lưu trữ độc lập tại đường dẫn:
  `/data/bronze/clarizen/task/ingest_date=YYYY-MM-DD/`
* Các trường tham chiếu như `Project`, `Manager` được lưu thành các cột bình thường (`project_id`, `manager_id`).
* **Lợi ích**: Dù hệ thống chưa có bảng `Project` hay `User`, bảng `Task` vẫn được ghi nhận, lưu trữ và đọc bình thường mà **không gặp bất kỳ lỗi ràng buộc (Constraint Violation) nào**.

### 2. Tầng Biến đổi (Spark Transformation)
* **Làm phẳng đệ quy (JSON Flattener)**: Bóc tách toàn bộ 186 trường của Task từ mảng JSON thô của API Clarizen ra cấu trúc bảng phẳng 1 tầng.
* **Khử trùng lặp Idempotent**: Sử dụng Spark Window Ranking:
  $$\text{ROW\_NUMBER}() \text{ OVER} (\text{PARTITION BY } SYSID \text{ ORDER BY } LastUpdatedOn \text{ DESC})$$
  Đảm bảo mỗi Task chỉ có 1 bản ghi mới nhất, loại bỏ trùng lặp nếu API cào lại nhiều lần.

### 3. Tầng Warehouse & dbt (Data Modeling cho Task)
Xây dựng mô hình dbt phục vụ riêng cho thực thể Task:
* **`stg_clarizen_tasks`**: Làm sạch, chuẩn hóa kiểu dữ liệu cho 186 trường.
* **`dim_tasks`**: Bảng Chiều chứa toàn bộ thông tin mô tả, trạng thái, phân loại WBS của Task.
* **`fct_task_daily_snapshot`**: Bảng Sự kiện chụp ảnh tiến độ hàng ngày, tính toán toàn bộ các chỉ số EVM ($PV, EV, AC, CPI, SPI$) và chi phí CAPEX/OPEX.

> **Kết luận**: Với thiết kế này, mục **Task đã hoàn toàn sẵn sàng chạy thật từ A-Z ngay hôm nay**, đạt chuẩn chất lượng cao nhất mà không bị cản trở bởi việc thiếu 4 mục còn lại.
