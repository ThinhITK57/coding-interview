# 01: Chuẩn hóa quy ước đặt tên Data-Driven và Đặc tả Hợp đồng Dữ liệu

**What to build:** Xây dựng bảng từ điển thuật ngữ (Data Domain Glossary) và tài liệu đặc tả hợp đồng dữ liệu cho 6 Data Marts. Loại bỏ hoàn toàn các tiền tố viết tắt nội bộ (`br01` - `br06`), thay bằng tên miền nghiệp vụ chuẩn (`bsc_yearly`, `cvct_execution_report`, `task_report`, `project_report`, `user_access_traffic`, `board_objectives`). Quy định cấu trúc cột gồm cả Khóa ID và Tên hiển thị được làm giàu từ Dimension.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Tạo bảng Glossary chuẩn hóa định danh nghiệp vụ cho 6 Data Marts (bỏ hoàn toàn `br01` - `br06`).
- [ ] Đặc tả chi tiết từng cột cho 6 Data Marts: giữ nguyên 100% cột Khóa ID và bổ sung các cột Tên hiển thị tương ứng.
- [ ] Ghi chú đầy đủ công thức tính toán, quy tắc so khớp và logic fallback cho từng trường dữ liệu.
