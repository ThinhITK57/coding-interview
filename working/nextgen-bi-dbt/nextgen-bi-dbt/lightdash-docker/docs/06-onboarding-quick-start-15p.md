# Onboarding nhanh 15 phút cho thành viên mới

Mục tiêu: giúp thành viên mới chạy được Lightdash local, mở được giao diện và hiểu luồng làm việc cơ bản với dbt trong 15 phút.

## 0-3 phút: Chuẩn bị

1. Cài Docker Desktop và mở ứng dụng.
2. Vào thư mục dự án [..](..).
3. Tạo file env local từ mẫu:

```bash
cp .env.lightdash.example .env.lightdash
```

4. Sửa tối thiểu 2 biến trong .env.lightdash:
 - PGPASSWORD
 - LIGHTDASH_SECRET

## 3-7 phút: Khởi động hệ thống

Chạy lần lượt:

```bash
./scripts/docker-build.sh
./scripts/docker-run.sh
```

Kiểm tra dịch vụ:

```bash
docker compose --env-file .env.lightdash ps
```

## 7-10 phút: Truy cập và xác minh

1. Mở http://localhost:8080.
2. Tạo tài khoản admin đầu tiên (nếu là lần chạy đầu).
3. Xác nhận vào được màn hình chính của Lightdash.

Nếu lỗi, kiểm tra log:

```bash
docker compose --env-file .env.lightdash logs -f lightdash
```

## 10-13 phút: Hiểu luồng dbt trong dự án

1. dbt project nằm ở [../nextgen_bi](../nextgen_bi).
2. Source khai báo tại [../nextgen_bi/models/sources/bi_silver__sources.yml](../nextgen_bi/models/sources/bi_silver__sources.yml).
3. Staging models theo domain tại [../nextgen_bi/models/staging](../nextgen_bi/models/staging).
4. Khi cập nhật bảng/model, làm theo playbook: [05-playbook-da-de-cap-nhat-models-tables.md](05-playbook-da-de-cap-nhat-models-tables.md).

## 13-15 phút: Quy tắc làm việc tối thiểu

1. Luôn chạy parse trước khi bàn giao:

```bash
cd nextgen_bi
dbt parse --no-partial-parse --profiles-dir .
```

2. Không commit secret (.env.lightdash).
3. Khi kết thúc buổi làm việc:

```bash
cd ..
./scripts/docker-remove.sh keep-data
```

## Lệnh nhớ nhanh

```bash
./scripts/docker-build.sh
./scripts/docker-run.sh
./scripts/docker-remove.sh keep-data
./scripts/docker-remove.sh purge
```

## Tài liệu cần đọc tiếp

1. Triển khai: [01-trien-khai-lightdash.md](01-trien-khai-lightdash.md)
2. Vận hành: [02-van-hanh-lightdash.md](02-van-hanh-lightdash.md)
3. Sử dụng: [03-su-dung-lightdash.md](03-su-dung-lightdash.md)
4. SOP ca trực: [04-checklist-sop-ca-truc.md](04-checklist-sop-ca-truc.md)
