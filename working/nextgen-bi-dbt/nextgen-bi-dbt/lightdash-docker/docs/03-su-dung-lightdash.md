# Hướng dẫn sử dụng Lightdash

## 1) Đăng nhập và khởi tạo

1. Mở trình duyệt tại http://localhost:8080
2. Tạo tài khoản admin đầu tiên theo hướng dẫn trên màn hình
3. Tạo Organization và Project

## 2) Kết nối tới dbt project trong workspace

dbt project hiện tại nằm ở [../nextgen_bi](../nextgen_bi) và được mount vào container Lightdash theo biến DBT_PROJECT_DIR.

Quy trình để sử dụng semantic layer:

1. Đảm bảo project dbt có models và schema đã được parse
2. Trong Lightdash, tạo kết nối kho dữ liệu phù hợp
3. Trỏ tới đường dẫn dbt project trong luồng setup của Lightdash
4. Đồng bộ metadata để Lightdash nhận dimensions, metrics

## 3) Cấu hình warehouse (gợi ý)

Hệ thống hiện sử dụng backend Postgres nội bộ cho Lightdash metadata, không bắt buộc trùng với warehouse phân tích.

Khi tạo warehouse trong UI Lightdash, dùng thông tin của kho dữ liệu thực tế (Trino, Postgres, BigQuery, Snowflake...) theo môi trường của bạn.

Thiết lập gợi ý cho Trino + `nextgen_bi`:

1. Type: Trino
2. Host: host Trino thực tế
3. Port: 8080
4. Catalog: `hive`
5. Schema: `bi_silver` (đọc nguồn) hoặc schema materialized models của bạn
6. dbt project path: `/usr/app/dbt`

Checklist nhanh trước khi refresh metadata:

1. [../nextgen_bi/profiles.yml](../nextgen_bi/profiles.yml) đã thay `host`, `user`, `auth` khỏi giá trị mẫu
2. Parse thành công:

```bash
cd nextgen_bi
dbt parse --no-partial-parse --profiles-dir .
```

3. Nếu dùng staging models để query, chạy thêm `dbt run` để tạo relation trên Trino

## 4) Luồng sử dụng cơ bản

1. Chọn Explore
2. Chọn dimensions và metrics
3. Áp bộ lọc và sort
4. Lưu chart vào dashboard
5. Chia sẻ dashboard cho team

## 5) Thực hành tốt

- Đặt tên chart và dashboard rõ nghĩa
- Quản lý truy cập theo nhóm
- Kiểm tra SQL generated trước khi chia sẻ rộng
- Đồng bộ lại metadata sau khi thay đổi models dbt

## 6) Lệnh hỗ trợ dbt trước khi đồng bộ

Chạy trong [../nextgen_bi](../nextgen_bi):

```bash
dbt parse --no-partial-parse --profiles-dir .
```

Nếu parse lỗi, sửa lỗi dbt trước rồi mới đồng bộ Lightdash.
