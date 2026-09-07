# GIẢI PHÁP XỬ LÝ MỤC TASK & KẾ HOẠCH TÍCH HỢP 4 MỤC CÒN LẠI
**Hệ thống Enterprise ELT Pipeline (Planview Clarizen Lakehouse)**

---

## 📌 BỐI CẢNH HIỆN TẠI

* **Mục Task**: Đã leak ra trước và đã có đầy đủ thông tin chi tiết của **186 trường** trong file `task_data_raw.txt`.
* **4 mục còn lại** (`Project`, `Giao-ban-kết-luận`, `Target`, `User`): **Hiện tại CHƯA CÓ THÔNG TIN**, ngày mai bộ phận BA mới tổng hợp và bàn giao danh sách trường.

Thư mục này được tạo ra để phục vụ 2 mục tiêu:
1. **Làm rõ giải pháp xử lý độc lập cho mục Task**: Đảm bảo pipeline cào, làm sạch 186 trường, tính toán EVM và nạp vào Trino thành công 100% ngay hôm nay mà không bị nghẽn bởi 4 mục kia.
2. **Chuẩn bị sẵn sàng quy trình tích hợp cho ngày mai**: Để khi BA đưa danh sách trường của 4 mục kia, chỉ mất 15 phút là đưa vào hệ thống chạy được ngay mà không cần sửa code core.

---

## 📂 DANH MỤC TÀI LIỆU TRONG THƯ MỤC

| Tên File | Vai trò & Nội dung chi tiết |
| :--- | :--- |
| [**GIAI_PHAP_XU_LY_DOC_LAP_TASK.md**](./GIAI_PHAP_XU_LY_DOC_LAP_TASK.md) | **Giải pháp Kỹ thuật Xử lý Task Độc lập**: Phân tích 186 trường của Task từ `task_data_raw.txt` thành 2 nhóm (Dữ liệu nội tại EVM/tiến độ & Dữ liệu tham chiếu ngoại vi). Thiết kế cơ chế lưu trữ không ép khóa ngoại để Task chạy trơn tru ngay hôm nay. |
| [**KE_HOACH_TICH_HOP_4_MUC_NGAY_MAI.md**](./KE_HOACH_TICH_HOP_4_MUC_NGAY_MAI.md) | **Kế hoạch & Sổ tay Tác chiến Ngày mai**: Checklist 4 câu hỏi cần lấy từ BA cho 4 mục, quy trình 4 bước điền vào `tables_registry.json`, chạy dry-run, cào dữ liệu và câu lệnh SQL mẫu trong dbt để ghép nối với Task. |

---

## 🚀 TÓM TẮT DÀNH CHO BUỔI HỌP VỚI LEADER

1. **Về mục Task**:
   * Đã phân tích toàn diện 186 trường dữ liệu từ Planview Clarizen.
   * Đã thiết kế hoàn chỉnh pipeline: Crawl $\rightarrow$ Spark JSON Flattener $\rightarrow$ Deduplication bằng Window Ranking $\rightarrow$ Trino Storage $\rightarrow$ dbt Model tính toán các chỉ số EVM chuẩn PMI ($PV, EV, AC, CPI, SPI$).
2. **Về 4 mục còn lại**:
   * Chúng ta không phỏng đoán trường dữ liệu bừa bãi khi chưa có tài liệu từ BA.
   * Kiến trúc pipeline đã được thiết kế mở (Pluggable): Ngày mai khi BA đưa danh sách trường, chỉ cần dán vào file cấu hình `tables_registry.json` là pipeline tự động nạp mà không phải viết lại code.
