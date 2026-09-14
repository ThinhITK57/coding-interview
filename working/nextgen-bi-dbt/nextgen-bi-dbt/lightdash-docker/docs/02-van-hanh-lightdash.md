# Tài liệu vận hành Lightdash

## 1) Thành phần hệ thống

Theo [../docker-compose.yml](../docker-compose.yml), hệ thống gồm:

- lightdash: ứng dụng chính
- db: Postgres (pgvector)
- minio: lưu trữ S3-compatible
- headless-browser: phục vụ render/chụp hình

## 2) Lệnh vận hành thường xuyên

Khởi động:

```bash
docker compose --env-file .env.lightdash up -d
```

Xem trạng thái:

```bash
docker compose ps
```

Xem log theo service:

```bash
docker compose logs -f lightdash
docker compose logs -f db
docker compose logs -f minio
```

Khởi động lại service:

```bash
docker compose restart lightdash
```

## 3) Kiểm tra sức khỏe

Checklist nhanh:

1. lightdash ở trạng thái running
2. db ở trạng thái healthy
3. Truy cập được http://localhost:8080
4. Không có error liên tiếp trong log lightdash

Kiểm tra nhanh db:

```bash
docker compose exec db pg_isready -U ${PGUSER:-postgres} -d ${PGDATABASE:-postgres}
```

## 4) Sao lưu và phục hồi

### Sao lưu Postgres

```bash
docker compose exec -T db pg_dump -U ${PGUSER:-postgres} -d ${PGDATABASE:-postgres} > backup_lightdash.sql
```

### Phục hồi Postgres

```bash
cat backup_lightdash.sql | docker compose exec -T db psql -U ${PGUSER:-postgres} -d ${PGDATABASE:-postgres}
```

### Sao lưu MinIO data

MinIO được lưu trong volume minio-data. Có thể backup bằng cách copy volume hoặc dùng công cụ backup của Docker host.

## 5) Nâng cấp phiên bản

1. Chỉnh image tag lightdash/lightdash trong [../docker-compose.yml](../docker-compose.yml)
2. Pull image mới
3. Khởi động lại stack
4. Kiểm tra migration/log sau nâng cấp

Lệnh gợi ý:

```bash
docker compose pull
docker compose --env-file .env.lightdash up -d
```

## 6) Xử lý sự cố

### Lightdash không lên

- Kiểm tra LIGHTDASH_SECRET và PGPASSWORD trong file env
- Kiểm tra db đã healthy chưa
- Đọc log lightdash và db

### Không kết nối được MinIO

- Kiểm tra S3_ENDPOINT và thông tin truy cập
- Kiểm tra minio service đang running

### Mất dữ liệu sau khi down

- Bạn có thể đã dùng down -v. Nếu cần giữ dữ liệu, chỉ dùng down không kèm -v.
