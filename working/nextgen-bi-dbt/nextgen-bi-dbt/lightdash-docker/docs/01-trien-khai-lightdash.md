# Hướng dẫn triển khai Lightdash

## 1) Điều kiện tiên quyết

- Đã cài Docker Desktop (Windows)
- Đã cài Docker Compose plugin
- Có Trino cluster đang chạy và truy cập được từ máy/container Lightdash
- Mở port local:
  - 8080 cho Lightdash
  - 5432 nội bộ container Postgres
  - 9000, 9001 cho MinIO
  - 3001 cho headless browser

Yêu cầu tối thiểu để truy vấn dữ liệu `nextgen_bi` qua Trino:

- Trino có catalog `hive`
- Có schema `bi_silver` chứa các bảng nguồn
- User truy cập Trino có quyền đọc schema `bi_silver`

## 2) Chuẩn bị biến môi trường

Tạo file .env.lightdash ở thư mục gốc dự án dựa theo file mẫu [../.env.lightdash.example](../.env.lightdash.example).

Giá trị bắt buộc:

- PGPASSWORD: mật khẩu Postgres nội bộ
- LIGHTDASH_SECRET: chuỗi bí mật để mã hóa dữ liệu trong Lightdash

Khuyến nghị:

- Đặt LIGHTDASH_SECRET tối thiểu 32 ký tự ngẫu nhiên
- Không commit file .env.lightdash lên git

Ví dụ:

```env
PGPASSWORD=StrongPassword_ChangeMe
LIGHTDASH_SECRET=ReplaceWithLongRandomSecretAtLeast32Chars
PGHOST=db
PGPORT=5432
PGUSER=postgres
PGDATABASE=postgres
PORT=8080
SITE_URL=http://localhost:8080
DBT_PROJECT_DIR=./nextgen_bi
S3_ENDPOINT=http://minio:9000
S3_REGION=us-east-1
S3_BUCKET=lightdash
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_FORCE_PATH_STYLE=true
```

## 3) Khởi tạo hệ thống

Chạy lệnh tại thư mục gốc dự án:

```bash
docker compose --env-file .env.lightdash up -d --remove-orphans
```

Kiểm tra trạng thái:

```bash
docker compose ps
```

## 4) Kiểm tra sau triển khai

- Truy cập giao diện: http://localhost:8080
- Kiểm tra log Lightdash:

```bash
docker compose logs -f lightdash
```

- Kiểm tra Postgres sẵn sàng:

```bash
docker compose exec db pg_isready -U postgres -d postgres
```

## 5) Kết nối Trino cho project `nextgen_bi`

1. Vào Lightdash UI và tạo warehouse connection loại Trino
2. Khai báo theo môi trường thực tế:
 - Host: hostname Trino thực tế (không dùng `trino-host` placeholder)
 - Port: 8080 (hoặc theo cluster của bạn)
 - User: user có quyền đọc dữ liệu
 - Catalog: `hive`
 - Schema: `bi_silver` (hoặc schema deploy models của bạn)
3. Chỉ định dbt project path: `/usr/app/dbt`
4. Refresh metadata và kiểm tra Explore

Lưu ý: nếu Lightdash chạy trong Docker, host Trino phải truy cập được từ container `lightdash`.

## 6) Dừng hệ thống

Dừng tạm thời:

```bash
docker compose stop
```

Dừng và xóa container (giữ volume dữ liệu):

```bash
docker compose down
```

Dừng và xóa cả volume dữ liệu:

```bash
docker compose down -v
```

## 7) Ghi chú về Windows

Nếu gặp lỗi kết nối Docker daemon, kiểm tra Docker Desktop đang chạy trước khi chạy docker compose.
