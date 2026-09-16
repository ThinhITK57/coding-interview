# Tài liệu mô tả nghiệp vụ – JIRA Raw (HIVE)

Tài liệu này mô tả **nghiệp vụ, ý nghĩa bảng và cột** cho **2 bảng dữ liệu JIRA** được export và lưu trữ tại **HIVE – raw layer**.

---

## 1. Bảng `jira_raw.task_operation`

### 1.1. Mô tả nghiệp vụ
Bảng **`jira_raw.task_operation`** lưu trữ danh sách **các task vận hành (operation task)** được quản lý trên JIRA, phục vụ cho việc theo dõi công việc nội bộ, phân công nhiệm vụ và đánh giá hiệu quả vận hành giữa các nhóm và phòng ban.

Dữ liệu được sử dụng để:
- Theo dõi vòng đời xử lý task vận hành
- Đánh giá khối lượng công việc theo cá nhân / nhóm
- Phân tích ưu tiên công việc và thời gian bắt đầu xử lý
- Làm cơ sở báo cáo năng suất và hiệu quả vận hành

---

### 1.2. Mô tả bảng
- **Tên bảng**: `jira_raw.task_operation`
- **Layer**: Raw
- **Nguồn dữ liệu**: JIRA (Operation Task)
- **Định dạng**: PARQUET
- **Vị trí lưu trữ**: `/opt/datasets/crawlers/vcs/jira/data/task_operation`

---

### 1.3. Mô tả cột
| Tên cột | Kiểu dữ liệu | Ý nghĩa |
|---|---|---|
| key | STRING | Mã task JIRA (định danh duy nhất) |
| summary | STRING | Tiêu đề / mô tả ngắn của task |
| issuetype | STRING | Loại issue (Task, Operation, …) |
| status | STRING | Trạng thái xử lý task |
| updated | STRING | Thời điểm task được cập nhật gần nhất (string) |
| created | STRING | Thời điểm task được tạo (string) |
| assignee | STRING | Người được giao thực hiện task |
| priority | STRING | Mức độ ưu tiên của task |
| assignor | STRING | Người giao task |
| work_group | STRING | Nhóm công việc / team thực hiện |
| department_center | STRING | Trung tâm / phòng ban phụ trách |
| start_date | STRING | Thời điểm bắt đầu xử lý task |
| source_file | STRING | File nguồn export JIRA (phục vụ audit & lineage) |
| updated_ts | BIGINT | Unix timestamp (ms) của updated |
| created_ts | BIGINT | Unix timestamp (ms) của created |
| start_date_ts | BIGINT | Unix timestamp (ms) của start_date |

---

### 1.4. Use cases chính
- Báo cáo số lượng task theo nhóm / phòng ban
- Phân tích thời gian bắt đầu và tiến độ xử lý task
- Đánh giá hiệu suất cá nhân và đội nhóm vận hành

---

## 2. Bảng `jira_raw.kpi_binh_staff`

### 2.1. Mô tả nghiệp vụ
Bảng **`jira_raw.kpi_binh_staff`** lưu trữ dữ liệu **KPI của nhân sự / đội vận hành Bình Staff**, được quản lý và cập nhật thông qua JIRA.

Dữ liệu phản ánh tình trạng nhân sự, mức độ đáp ứng yêu cầu vận hành, điều kiện hợp đồng, sản phẩm phụ trách và các chỉ số liên quan đến độ trễ, tính sẵn sàng và ngoại lệ.

Bảng phục vụ cho:
- Theo dõi và đánh giá KPI nhân sự
- Báo cáo năng lực vận hành theo sản phẩm
- Quản lý ngoại lệ và điều kiện đặc thù

---

### 2.2. Mô tả bảng
- **Tên bảng**: `jira_raw.kpi_binh_staff`
- **Layer**: Raw
- **Nguồn dữ liệu**: JIRA (Staff KPI)
- **Định dạng**: PARQUET
- **Vị trí lưu trữ**: `/opt/datasets/crawlers/vcs/jira/data/kpi_binh_staff`

---

### 2.3. Mô tả cột
| Tên cột | Kiểu dữ liệu | Ý nghĩa |
|---|---|---|
| key | STRING | Mã issue KPI nhân sự |
| summary | STRING | Tên hoặc mô tả KPI |
| issuetype | STRING | Loại issue (KPI / Task) |
| status | STRING | Trạng thái đánh giá KPI |
| updated | STRING | Thời điểm cập nhật gần nhất |
| created | STRING | Thời điểm tạo KPI |
| description | STRING | Mô tả chi tiết nội dung KPI |
| agent_online | DOUBLE | Số lượng / tỷ lệ nhân sự online |
| agent_latest | DOUBLE | Số lượng nhân sự cập nhật mới nhất |
| has_soc247 | STRING | Có tham gia SOC 24/7 hay không (Yes/No) |
| is_exception | STRING | Đánh dấu KPI thuộc trường hợp ngoại lệ |
| latest_version | STRING | Phiên bản áp dụng mới nhất |
| contract_type | STRING | Loại hợp đồng nhân sự |
| product_type | STRING | Loại sản phẩm / dịch vụ phụ trách |
| exception_reason | STRING | Lý do ngoại lệ (nếu có) |
| report_date | STRING | Ngày báo cáo KPI |
| update_start_date | STRING | Ngày bắt đầu áp dụng cập nhật |
| update_date | STRING | Ngày cập nhật thực tế |
| product_name | STRING | Tên sản phẩm phụ trách |
| update_frequency | STRING | Tần suất cập nhật KPI |
| version | STRING | Phiên bản KPI |
| latency_delay | DOUBLE | Chỉ số độ trễ / delay |
| source_file | STRING | File nguồn export JIRA |
| updated_ts | BIGINT | Unix timestamp (ms) của updated |
| created_ts | BIGINT | Unix timestamp (ms) của created |
| report_date_ts | BIGINT | Unix timestamp (ms) của report_date |
| update_start_date_ts | BIGINT | Unix timestamp (ms) của update_start_date |
| update_date_ts | BIGINT | Unix timestamp (ms) của update_date |

---

### 2.4. Use cases chính
- Báo cáo KPI nhân sự theo kỳ
- Đánh giá năng lực vận hành theo sản phẩm
- Theo dõi ngoại lệ và độ trễ KPI

---

## Gợi ý phát triển tiếp
- Chuẩn hóa timestamp sang TIMESTAMP ở Silver layer
- Chuẩn hóa giá trị KPI, cờ Yes/No
- Xây dựng fact_staff_kpi và fact_operation_task ở Gold layer
