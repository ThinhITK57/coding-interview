# Tài liệu mô tả nghiệp vụ – JIRA Raw (HIVE)

Tài liệu này mô tả **nghiệp vụ, ý nghĩa bảng và cột** cho 3 bảng dữ liệu JIRA được export và lưu trữ tại **HIVE – raw layer**. Các bảng phục vụ theo dõi **tuân thủ quy trình**, **KPI/KQI**, và **bug production**.

---

## 1. Bảng `jira_raw.process_compliance`

### 1.1. Mô tả nghiệp vụ
Bảng lưu trữ dữ liệu **đánh giá mức độ tuân thủ quy trình (Process Compliance)** của các issue JIRA liên quan đến sản phẩm hoặc quy trình nội bộ.

Dữ liệu được sử dụng để:
- Đánh giá mức độ tuân thủ SLA / quy trình chuẩn
- Theo dõi issue trễ hạn so với **due date**
- Tổng hợp báo cáo tuân thủ theo sản phẩm
- Làm đầu vào cho KPI chất lượng vận hành

### 1.2. Mô tả bảng
- **Layer**: raw
- **Nguồn**: JIRA Export (field chuẩn + custom field)
- **Tần suất cập nhật**: theo kỳ export
- **Định dạng**: PARQUET
- **Định vị dữ liệu**: `/opt/datasets/crawlers/vcs/jira/data/process_compliance`

### 1.3. Mô tả cột
| Cột | Kiểu dữ liệu | Ý nghĩa |
|---|---|---|
| key | STRING | Mã issue JIRA (khóa định danh logic) |
| summary | STRING | Tiêu đề ngắn gọn của issue |
| issuetype | STRING | Loại issue (Task, Process, Compliance, …) |
| duedate | STRING | Ngày đến hạn xử lý issue (dạng string từ JIRA) |
| ttqt_product_name | STRING | Tên sản phẩm / quy trình được đánh giá tuân thủ |
| process_compliance_rate | DOUBLE | Tỷ lệ tuân thủ quy trình (0–1 hoặc %) |
| source_file | STRING | File nguồn export JIRA (phục vụ audit & lineage) |
| duedate_ts | BIGINT | Unix timestamp (ms) của duedate |

### 1.4. Use cases chính
- Báo cáo **tỷ lệ tuân thủ quy trình** theo sản phẩm
- Theo dõi issue vi phạm quy trình
- Làm chỉ số đầu vào cho KPI quản trị chất lượng

---

## 2. Bảng `jira_raw.kpi_kqi`

### 2.1. Mô tả nghiệp vụ
Bảng lưu trữ dữ liệu **KPI / KQI được quản lý qua JIRA**, phản ánh kết quả đo lường hiệu quả vận hành và chất lượng dịch vụ theo kỳ báo cáo.

Bảng này phục vụ:
- Theo dõi kết quả KPI theo tháng / quý
- Đánh giá mức độ hoàn thành mục tiêu
- Tổng hợp báo cáo điều hành, báo cáo quản trị

### 2.2. Mô tả bảng
- **Layer**: raw
- **Nguồn**: JIRA KPI/KQI issues
- **Định dạng**: PARQUET
- **Định vị dữ liệu**: `/opt/datasets/crawlers/vcs/jira/data/kpi_kqi`

### 2.3. Mô tả cột
| Cột | Kiểu dữ liệu | Ý nghĩa |
|---|---|---|
| key | STRING | Mã issue KPI/KQI trong JIRA |
| summary | STRING | Tên hoặc mô tả KPI/KQI |
| quarterly_kpi_result | DOUBLE | Kết quả KPI lũy kế theo quý |
| monthly_accumulated_result | DOUBLE | Kết quả KPI lũy kế theo tháng |
| execution_result | DOUBLE | Kết quả thực hiện tại thời điểm báo cáo |
| reporting_date | STRING | Ngày / kỳ báo cáo (string) |
| unit_of_measure | DOUBLE | Đơn vị đo KPI (%, điểm, số lượng, … – dạng numeric) |
| target | STRING | Mục tiêu KPI/KQI cần đạt |
| service_product | STRING | Dịch vụ hoặc sản phẩm liên quan |
| condition | STRING | Điều kiện đánh giá (đạt / không đạt, công thức, …) |
| source_file | STRING | File nguồn export JIRA |
| reporting_date_ts | BIGINT | Unix timestamp (ms) của reporting_date |

### 2.4. Use cases chính
- Dashboard KPI/KQI điều hành
- So sánh **target vs actual**
- Đánh giá hiệu suất dịch vụ / sản phẩm

---

## 3. Bảng `jira_raw.bug_prod`

### 3.1. Mô tả nghiệp vụ
Bảng lưu trữ danh sách **bug phát sinh trên môi trường production**, dùng để theo dõi chất lượng phần mềm và hiệu quả xử lý sự cố.

Dữ liệu hỗ trợ:
- Phân tích số lượng bug production
- Theo dõi vòng đời bug (created → resolved)
- Đánh giá mức độ nghiêm trọng và hiệu suất xử lý

### 3.2. Mô tả bảng
- **Layer**: raw
- **Nguồn**: JIRA Bug (Production)
- **Định dạng**: PARQUET
- **Định vị dữ liệu**: `/opt/datasets/crawlers/vcs/jira/data/bug_prod`

### 3.3. Mô tả cột
| Cột | Kiểu dữ liệu | Ý nghĩa |
|---|---|---|
| key | STRING | Mã bug JIRA |
| projectkey | STRING | Mã project |
| projectname | STRING | Tên project |
| summary | STRING | Mô tả ngắn bug |
| issuetype | STRING | Loại issue (Bug) |
| status | STRING | Trạng thái bug (Open, In Progress, Resolved, …) |
| assignee | STRING | Người phụ trách xử lý bug |
| resolved | STRING | Thời điểm bug được resolve (string) |
| priority | STRING | Mức độ ưu tiên (Blocker, Critical, Major, …) |
| created | STRING | Thời điểm bug được tạo |
| source_file | STRING | File nguồn export |
| created_ts | BIGINT | Unix timestamp (ms) thời điểm tạo bug |
| resolved_ts | BIGINT | Unix timestamp (ms) thời điểm resolve bug |

### 3.4. Use cases chính
- Báo cáo **bug production theo project / priority**
- Phân tích **MTTR, lead time xử lý bug**
- Đánh giá chất lượng release và vận hành

---

## Gợi ý phát triển tiếp
- Silver layer:
  - Chuẩn hóa timestamp sang TIMESTAMP
  - Chuẩn hóa giá trị % và đơn vị đo
- Gold layer:
  - Fact_bug
  - Fact_kpi_result
  - Process compliance dashboard
