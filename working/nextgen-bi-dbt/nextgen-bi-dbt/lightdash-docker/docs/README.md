# Tài liệu hệ thống Lightdash

Bộ tài liệu này mô tả cách triển khai, vận hành và sử dụng Lightdash self-host cho dự án này.

## Mục lục

- Triển khai: [01-trien-khai-lightdash.md](01-trien-khai-lightdash.md)
- Vận hành: [02-van-hanh-lightdash.md](02-van-hanh-lightdash.md)
- Sử dụng: [03-su-dung-lightdash.md](03-su-dung-lightdash.md)
- SOP ca trực: [04-checklist-sop-ca-truc.md](04-checklist-sop-ca-truc.md)
- Playbook DA/DE cập nhật models-tables: [05-playbook-da-de-cap-nhat-models-tables.md](05-playbook-da-de-cap-nhat-models-tables.md)
- Onboarding nhanh 15 phút: [06-onboarding-quick-start-15p.md](06-onboarding-quick-start-15p.md)

## Phạm vi

- Docker Compose stack tại [../docker-compose.yml](../docker-compose.yml)
- Biến môi trường mẫu tại [../.env.lightdash.example](../.env.lightdash.example)
- dbt project được mount mặc định từ [../nextgen_bi](../nextgen_bi)

## Luồng tổng quan

1. Chuẩn bị file env và giá trị bí mật
2. Khởi tạo hệ thống bằng docker compose
3. Đăng nhập Lightdash và kết nối project dbt
4. Vận hành hằng ngày: theo dõi, sao lưu, cập nhật
