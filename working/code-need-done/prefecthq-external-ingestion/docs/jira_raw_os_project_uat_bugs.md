# Bảng jira_raw.os_project_uat_bugs

## 1. Mô tả nghiệp vụ
Bảng **`jira_raw.os_project_uat_bugs`** lưu trữ danh sách **bug phát sinh trong giai đoạn nghiệm thu (UAT – User Acceptance Test)** của các **dự án OS**, được trích xuất từ hệ thống **JIRA**.

Dữ liệu phản ánh các lỗi được phát hiện khi kiểm thử, nghiệm thu dự án trước khi đưa vào production, nhằm:
- Đánh giá **chất lượng nghiệm thu UAT** của dự án OS
- Theo dõi số lượng và trạng thái bug trong giai đoạn nghiệm thu
- Phân tích tiến độ xử lý bug UAT trước khi go-live
- Làm cơ sở báo cáo chất lượng dự án cho quản lý và điều hành

---

## 2. Mô tả bảng
- **Tên bảng**: `jira_raw.os_project_uat_bugs`
- **Layer**: Raw
- **Nguồn dữ liệu**: JIRA (UAT Bug – OS Project)
- **Định dạng lưu trữ**: PARQUET
- **Vị trí lưu trữ**: `/opt/datasets/crawlers/vcs/jira/data/os_project_uat_bugs`
- **Phạm vi dữ liệu**: Bug phát sinh trong giai đoạn nghiệm thu (UAT)

---

## 3. Mô tả cột
| Tên cột | Kiểu dữ liệu | Ý nghĩa |
|---|---|---|
| key | STRING | Mã bug JIRA (định danh duy nhất của issue) |
| summary | STRING | Mô tả ngắn nội dung bug UAT |
| issuetype | STRING | Loại issue trong JIRA (Bug) |
| status | STRING | Trạng thái hiện tại của bug (Open, In Progress, Resolved, Closed, …) |
| updated | STRING | Thời điểm bug được cập nhật gần nhất (dạng string từ JIRA) |
| created | STRING | Thời điểm bug được tạo (dạng string từ JIRA) |
| projectkey | STRING | Mã dự án OS trong JIRA |
| projectname | STRING | Tên đầy đủ của dự án OS |
| source_file | STRING | File nguồn chứa dữ liệu export JIRA (phục vụ audit & lineage) |
| updated_ts | BIGINT | Unix timestamp (ms) của thời điểm cập nhật bug |
| created_ts | BIGINT | Unix timestamp (ms) của thời điểm tạo bug |

---

## 4. Ý nghĩa và mục đích sử dụng
- Theo dõi **số lượng bug UAT theo dự án OS**
- Phân tích **trạng thái xử lý bug** trong giai đoạn nghiệm thu
- Đánh giá mức độ sẵn sàng của dự án trước khi triển khai chính thức
- Làm dữ liệu đầu vào cho các báo cáo:
  - Chất lượng nghiệm thu dự án
  - Tỷ lệ bug còn tồn đọng trước go-live
  - Thời gian xử lý bug UAT

---

## 5. Gợi ý phát triển tiếp (Silver / Gold)
- **Silver layer**:
  - Chuẩn hóa `created`, `updated` sang kiểu TIMESTAMP
  - Chuẩn hóa trạng thái bug (status mapping)

- **Gold layer / KPI gợi ý**:
  - Tổng số bug UAT theo dự án
  - Tỷ lệ bug đã resolve trước nghiệm thu
  - Aging bug trong giai đoạn UAT

---

## 6. Đối tượng sử dụng dữ liệu
- Ban quản lý dự án OS
- Đội kiểm thử / QA
- Đội vận hành & triển khai hệ thống
- Báo cáo quản trị chất lượng dự án
