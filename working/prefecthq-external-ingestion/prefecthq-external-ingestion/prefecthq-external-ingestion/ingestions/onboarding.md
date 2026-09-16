# Onboarding — MinIO Flat-File Ingestion

## Flow

Đọc file Excel/CSV từ MinIO (các bên nghiệp vụ upload file manually lên), chuẩn hoá tên cột, xuất Parquet và upload lên MinIO output. Chạy test bằng Prefect local tại `http://localhost:4200`.

Toàn bộ cấu hình bảng nằm trong list `RESOURCES` ở cuối `flat_file_flow.py`. Thêm bảng mới = thêm một dict vào list đó, không cần đụng vào logic core.

---

## Khởi động

```bash
source venv/bin/activate
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY  # bắt buộc, tránh lỗi proxy
python flat_file_flow.py
```

Cần có file `.env` với: `PREFECT_API_URL`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_INPUT_BUCKET`, `MINIO_OUTPUT_BUCKET`.

---

## Cấu trúc quan trọng

```
resources/
  mappings/{domain}/{table}.json       ← map tên cột gốc → snake_case
  parquet_schema/{domain}/{table}.json ← schema cache (tự sinh, xoá khi file đổi cấu trúc)
```

`domain` là tên nhóm dữ liệu, ví dụ: `finance_raw`, `hr_raw`, `cx_cso_raw`.  
Tên folder mapping phải **khớp chính xác** với giá trị `domain` trong resource descriptor.

---

## Thêm bảng mới

1. Upload file lên MinIO bucket
2. Tạo file `resources/mappings/{domain}/{resource_name}.json`
3. Thêm dict vào `RESOURCES` trong `flat_file_flow.py`
4. Chạy thử — log `No mapping for col=...` thì bổ sung vào mapping file

**Mapping file mẫu:**
```json
{
  "Mã nhân viên": "employee_code",
  "Ngày nghỉ việc": "resigned_date"
}
```
Tên cột đã là tiếng Anh/snake_case thì để `{}`.

**Resource descriptor mẫu:**
```python
{
    "domain":        "finance_raw",
    "resource_name": "ten_bang",
    "object_key":    "folder/file.xlsx",  # đường dẫn trong MinIO bucket
    "sheet_name":    "Sheet1",
    "header_row":    0,   # dòng chứa tên cột
    "drop_rows":     1,   # thường = header_row + 1
    "crawl_mode":    "static",  # hoặc "modified_and_new" để append
}
```

---

## Các trường hợp hay gặp

| Tình huống | Cách xử lý |
|---|---|
| Nhiều file gộp 1 bảng | Dùng key `sources: [...]` thay cho `object_key` |
| Folder chứa nhiều CSV | `file_type: "csv_folder"`, `object_key` trỏ vào prefix folder |
| Cần pivot / dedup / tính cột mới | Khai báo `post_process: ten_ham` — hàm nhận và trả về DataFrame |
| Header Excel nhiều dòng | `drop_rows: 0`, xử lý flatten trong hàm `post_process` |
| Thêm cột tĩnh (tag metadata) | `add_columns: {"data_type": "CP"}` |

---

## Lỗi thường gặp

| Lỗi | Nguyên nhân | Xử lý |
|---|---|---|
| `Failed to reach API` | Proxy chặn kết nối localhost | `unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY` |
| `Worksheet named 'X' not found` | Tên sheet sai (dấu tiếng Việt, khoảng trắng) | Kiểm tra tên sheet thực tế bằng `openpyxl` |
| `KeyError: 'ten_cot'` trong post_process | Mapping load 0 entries — sai tên domain hoặc thiếu file mapping | Kiểm tra log `Mapping loaded (N entries)` |
| `No CSV files found` | Sai prefix folder trong `object_key` | List objects trong MinIO để xác nhận đúng path |
| `could not convert string to float: ''` | Schema cache cũ không khớp dữ liệu mới | Xoá file trong `resources/parquet_schema/{domain}/{table}.json` |
