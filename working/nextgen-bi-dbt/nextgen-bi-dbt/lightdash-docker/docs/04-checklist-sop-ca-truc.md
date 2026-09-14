# Checklist SOP Ca Trực (Lightdash)

Tài liệu này là SOP ngắn gọn để team vận hành Lightdash theo chu kỳ Daily, Weekly, Monthly.

## Daily Checklist

1. Kiểm tra trạng thái dịch vụ
 - Chạy `docker compose ps`
 - Xác nhận `lightdash` running, `db` healthy
2. Kiểm tra truy cập ứng dụng
 - Truy cập http://localhost:8080
 - Xác nhận đăng nhập và mở được ít nhất 1 dashboard
3. Kiểm tra log lỗi mới
 - Chạy `docker compose logs --since=24h lightdash`
 - Ghi nhận lỗi lặp lại (database, auth, S3, scheduler)
4. Kiểm tra dung lượng cơ bản
 - Kiểm tra disk host nơi chứa Docker volumes
 - Cảnh báo nếu dung lượng còn dưới ngưỡng vận hành nội bộ
5. Ghi nhận ca trực
 - Cập nhật trạng thái vào kênh/team log trực

## Weekly Checklist

1. Backup dữ liệu Postgres
 - Chạy `docker compose exec -T db pg_dump -U ${PGUSER:-postgres} -d ${PGDATABASE:-postgres} > backup_lightdash_weekly.sql`
 - Lưu backup vào nơi lưu trữ an toàn
2. Kiểm tra khả năng phục hồi
 - Kiểm tra file backup có dung lượng hợp lệ
 - Thực hiện restore test trên môi trường test nếu có
3. Rà soát cảnh báo và lỗi tồn đọng
 - Tổng hợp lỗi trong tuần từ logs
 - Tạo ticket cho lỗi chưa xử lý
4. Kiểm tra tài nguyên
 - Theo dõi xu hướng CPU/RAM/Disk của host
 - Đề xuất tăng tài nguyên nếu cần
5. Kiểm tra đồng bộ dbt/Lightdash
 - Xác nhận metadata vẫn đồng bộ với project dbt hiện tại

## Monthly Checklist

1. Rà soát bảo mật
 - Kiểm tra chính sách quản lý bí mật `LIGHTDASH_SECRET`, `PGPASSWORD`
 - Đổi secret theo chính sách nội bộ (nếu áp dụng)
2. Rà soát phiên bản
 - Kiểm tra phiên bản image `lightdash/lightdash`
 - Lập kế hoạch nâng cấp và rollback
3. Diễn tập khôi phục
 - Thực hiện bài test restore dữ liệu end-to-end trên môi trường test
4. Dọn dẹp vận hành
 - Xóa backup hết hạn theo chính sách lưu trữ
 - Dọn artifact/log cũ không còn cần thiết
5. Báo cáo tháng
 - Tổng hợp uptime, sự cố chính, hành động cải tiến

## Mẫu bàn giao ca trực

1. Thời gian ca:
2. Trạng thái dịch vụ (`lightdash`, `db`, `minio`, `headless-browser`):
3. Sự cố phát sinh:
4. Hành động đã xử lý:
5. Việc còn tồn đọng:
6. Người nhận bàn giao:

## Liên kết liên quan

- Triển khai: [01-trien-khai-lightdash.md](01-trien-khai-lightdash.md)
- Vận hành: [02-van-hanh-lightdash.md](02-van-hanh-lightdash.md)
- Sử dụng: [03-su-dung-lightdash.md](03-su-dung-lightdash.md)