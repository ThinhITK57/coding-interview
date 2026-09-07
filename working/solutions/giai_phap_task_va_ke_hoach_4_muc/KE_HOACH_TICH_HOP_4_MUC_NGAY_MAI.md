# KẾ HOẠCH TÍCH HỢP 4 MỤC CÒN LẠI VÀO NGÀY MAI
**Quy trình Tiếp nhận từ BA & Tích hợp vào Pipeline Hiện tại**

---

## 1. MỤC TIÊU CỦA KẾ HOẠCH

Ngày mai, bộ phận Phân tích Nghiệp vụ (BA) sẽ tổng hợp và bàn giao thông tin của 4 mục còn lại:
1. **`Project`** (Dự án)
2. **`Giao-ban-kết-luận`** (Kết luận cuộc họp / Chỉ đạo điều hành)
3. **`Target`** (Mục tiêu / Chỉ tiêu OKR/KPI)
4. **`User`** (Người dùng / Nhân sự)

Tài liệu này đóng vai trò là **Sổ tay Hướng dẫn Tác chiến (Action Checklist)** để khi nhận được thông tin, bạn có thể đưa ngay vào hệ thống trong vòng 15-30 phút mà không phải sửa code core.

---

## 2. CHECKLIST 4 CÂU HỎI CẦN LẤY TỪ BA CHO TỪNG MỤC

Khi BA bàn giao thông tin ngày mai, bạn chỉ cần yêu cầu BA cung cấp đúng 4 thông số kỹ thuật cho mỗi mục:

| STT | Thông số kỹ thuật | Mục đích sử dụng trong Pipeline | Ví dụ minh họa |
| :---: | :--- | :--- | :--- |
| **1** | **API Path / Endpoint** | Đường dẫn API để gọi lấy dữ liệu | `/v2.0/services/data/entityQuery` (hoặc path riêng) |
| **2** | **Danh sách các trường (`fields`)** | Các cột cần cào về từ API | Danh sách tên cột (SYSID, Name, Budget, ...) |
| **3** | **Khóa chính (`primary_key`) & Watermark** | Phục vụ deduplicate và cào tăng dần | Khóa chính thường là `SYSID`, watermark là `LastModified` |
| **4** | **Trường liên kết (Join Key) với Task** | Khóa để dbt kết nối với bảng Task | Ví dụ: `Project.SYSID` khớp với cột `Task.Project` |

---

## 3. QUY TRÌNH 4 BƯỚC TÍCH HỢP VÀO HỆ THỐNG

### Bước 1: Khai báo vào `tables_registry.json`
Toàn bộ pipeline đã được tích hợp cơ chế nạp cấu hình động qua [config/loader.py](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/config/loader.py). Bạn **không cần sửa `config.json` hay code Python**, chỉ cần mở file [tables_registry.json](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/tables_registry.json) và dán thông tin BA cung cấp:

```json
{
  "tables": [
    {
      "table_name": "project",
      "entity_type": "Project",
      "primary_key": "SYSID",
      "watermark_field": "LastModified",
      "fields": [
        "SYSID",
        "Name",
        "Budget",
        "StartDate",
        "DueDate"
        // ... dán danh sách trường BA đưa vào đây
      ]
    }
  ]
}
```

---

### Bước 2: Chạy kiểm thử khô (Dry-run) từng mục mới
Trước khi cào dữ liệu thật, chạy kiểm tra tính hợp lệ của cấu hình:

```powershell
# Kiểm tra riêng mục mới (ví dụ project)
python prefect_flow.py --endpoint project --dry-run

# Kiểm tra toàn bộ
python prefect_flow.py --all --dry-run
```
*Kết quả in ra `[INFO] Config loaded and validated` là thành công.*

---

### Bước 3: Kích hoạt cào và lưu trữ dữ liệu
Chạy cào dữ liệu thực tế cho mục mới:

```powershell
# Chạy cào riêng 1 mục
python prefect_flow.py --endpoint project --env dev

# Hoặc chạy toàn bộ các mục đã cấu hình
python prefect_flow.py --all --env prod
```
*Dữ liệu thô sẽ tự động được ghi vào Parquet tại thư mục `/data/bronze/clarizen/{tên_mục}/` và tạo bảng tự động trên Trino.*

---

### Bước 4: Tầng dbt — Ráp nối 4 bảng với bảng Task
Khi dữ liệu 4 bảng đã về kho, việc ghép nối được thực hiện hoàn toàn tại tầng dbt thông qua các trường tham chiếu mà Task đã lưu sẵn:

```sql
-- Ví dụ model dbt kết nối Task với Project và User
SELECT
    t.task_id,
    t.task_name,
    t.percent_completed,
    t.cpi,
    t.spi,
    -- Ghép với bảng Project dựa trên trường Project đã có ở Task:
    p.project_name,
    p.total_budget,
    -- Ghép với bảng User dựa trên trường Manager đã có ở Task:
    u.user_name AS manager_name,
    u.email AS manager_email
FROM {{ ref('dim_tasks') }} t
LEFT JOIN {{ ref('stg_projects') }} p ON t.project_id = p.project_id
LEFT JOIN {{ ref('stg_users') }} u ON t.manager_id = u.user_id
```

---

## 4. TÓM TẮT LỢI ĐIỂM KHI BÁO CÁO VỚI LEADER

1. **Tiến độ rõ ràng**: Mục `Task` (phức tạp nhất với 186 trường) đã được hoàn thiện 100% và kiểm thử chạy trơn tru.
2. **Kiến trúc mở (Pluggable)**: 4 mục còn lại chưa có schema không làm ảnh hưởng đến tiến độ của Task. Hệ thống đã có sẵn khung tiếp nhận cấu hình tự động.
3. **Ngày mai không bị động**: Khi BA đưa tài liệu 4 bảng, chỉ mất 15 phút điền trường vào `tables_registry.json` là pipeline tự động cào và nạp vào Trino ngay lập tức.
