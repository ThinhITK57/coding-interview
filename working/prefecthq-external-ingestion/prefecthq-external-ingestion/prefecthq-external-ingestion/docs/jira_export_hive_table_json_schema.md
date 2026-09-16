## Tổng quan dữ liệu
Bảng này lưu **dữ liệu JIRA đã được export** (raw layer) và nạp vào HIVE để phục vụ phân tích tiến độ, hiệu suất và báo cáo theo project.

- **Layer**: raw
- **Nguồn**: JIRA Export (CSV / API)
- **Định dạng**: PARQUET
- **Mục đích**:
  - Phân tích issue, workload, effort
  - Tổng hợp theo project / assignee / trạng thái
  - Làm nguồn cho Silver / Gold layer

---

## Thông tin bảng
```json
{
  "database": "jira_raw",
  "table": "ulnl",
  "location": "/opt/datasets/crawlers/vcs/jira/data/ulnl",
  "format": "parquet",
  "description": "Dữ liệu issue JIRA đã export, dùng cho phân tích tiến độ và effort theo project"
}
```

---

## JSON mô tả schema cột
```json
{
  "columns": [
    {
      "name": "key",
      "type": "string",
      "description": "Mã issue JIRA (ví dụ: PROJ-123)",
      "nullable": false
    },
    {
      "name": "projectkey",
      "type": "string",
      "description": "Mã project trong JIRA (PROJ)",
      "nullable": false
    },
    {
      "name": "projectname",
      "type": "string",
      "description": "Tên đầy đủ của project",
      "nullable": true
    },
    {
      "name": "summary",
      "type": "string",
      "description": "Tiêu đề ngắn gọn của issue",
      "nullable": true
    },
    {
      "name": "issuetype",
      "type": "string",
      "description": "Loại issue (Bug, Task, Story, Epic, …)",
      "nullable": true
    },
    {
      "name": "status",
      "type": "string",
      "description": "Trạng thái hiện tại của issue (To Do, In Progress, Done, …)",
      "nullable": true
    },
    {
      "name": "assignee",
      "type": "string",
      "description": "Người được giao xử lý issue",
      "nullable": true
    },
    {
      "name": "resolved",
      "type": "string",
      "description": "Thời điểm issue được resolve (dạng string từ JIRA export)",
      "nullable": true
    },
    {
      "name": "ttsx_product",
      "type": "string",
      "description": "Sản phẩm / hệ thống liên quan (field tùy chỉnh trong JIRA)",
      "nullable": true
    },
    {
      "name": "total_net_effort",
      "type": "double",
      "description": "Tổng effort thực tế (net effort), thường tính bằng giờ hoặc man-day",
      "nullable": true
    },
    {
      "name": "source_file",
      "type": "string",
      "description": "Tên file nguồn chứa bản export JIRA (phục vụ trace & lineage)",
      "nullable": false
    },
    {
      "name": "resolved_ts",
      "type": "bigint",
      "description": "Unix timestamp (ms) của thời điểm resolved",
      "nullable": true
    }
  ]
}
```

---

## Ý nghĩa nghiệp vụ (Business Meaning)
- **key**: Khóa chính logic của issue
- **projectkey / projectname**: Dùng để nhóm, lọc và báo cáo theo project
- **issuetype / status**: Phục vụ phân tích luồng công việc (workflow)
- **assignee**: Đánh giá workload và hiệu suất cá nhân
- **total_net_effort**: Chỉ số quan trọng để đo effort thực tế
- **resolved / resolved_ts**: Phân tích lead time, cycle time
- **source_file**: Hỗ trợ audit, replay và data lineage

---

## Gợi ý thiết kế nâng cao
- Silver layer:
  - Chuẩn hóa `resolved` → TIMESTAMP
  - Chuẩn hóa tên assignee
  - Chuẩn hóa effort về cùng đơn vị

- Partition gợi ý:
```sql
PARTITIONED BY (projectkey STRING)
```

- Gold use cases:
  - Velocity theo sprint / project
  - Effort theo assignee
  - Aging issue / lead time

---

## Mapping sang Data Catalog (ví dụ)
```json
{
  "domain": "Engineering Productivity",
  "owner": "PMO / Engineering",
  "sensitivity": "Internal",
  "update_frequency": "Daily"
}
```

