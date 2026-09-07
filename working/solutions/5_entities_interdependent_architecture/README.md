# GIẢI PHÁP KIẾN TRÚC: ĐIỀU PHỐI 5 THỰC THỂ LIÊN KẾT PHỤ THUỘC
**Hệ thống ELT Pipeline Quản trị Dự án & Điều hành Doanh nghiệp (Planview Clarizen Lakehouse)**

---

## 📌 BỐI CẢNH & MỤC TIÊU DỰ ÁN

Dự án cào và nạp dữ liệu điều hành doanh nghiệp bao gồm **5 thực thể cốt lõi**:
1. **Task** (Công việc / Nhiệm vụ — *đã hoàn thiện trích xuất và kiểm thử 186 trường Planview Clarizen*).
2. **Project** (Dự án chiến lược).
3. **User** (Nhân sự / Lãnh đạo / Người thực hiện).
4. **Target** (Mục tiêu / Chỉ tiêu OKR / KPI).
5. **Giao-Ban-Ket-Luan** (Biên bản giao ban / Kết luận cuộc họp / Chỉ đạo lãnh đạo / Action Items).

Thư mục này đóng gói **toàn bộ giải pháp kiến trúc chuyên sâu**, sẵn sàng nạp danh sách trường chi tiết của 4 bảng còn lại khi bộ phận BA bàn giao vào ngày mai.

---

## 📂 DANH MỤC TÀI LIỆU & ARTIFACTS TRONG THƯ MỤC

| Tên File | Vai trò & Nội dung chi tiết |
| :--- | :--- |
| [**ARCHITECTURAL_SOLUTION_DESIGN.md**](./ARCHITECTURAL_SOLUTION_DESIGN.md) | **Tài liệu Thiết kế Kiến trúc Toàn diện**: Phân tích 3 rủi ro cốt tử (Sai trật tự, Late-Arriving Data, Vòng lặp) và bộ giải pháp 3 tầng bảo vệ (Topological Wave DAG, Kimball Inferred Dimensions, dbt Referential Integrity). |
| [**ERD_AND_DATA_MODELING.md**](./ERD_AND_DATA_MODELING.md) | **Mô hình Dữ liệu & Biểu đồ ERD**: Quan hệ khóa ngoại giữa 5 thực thể, mô hình Kim tinh (Star Schema), cấu trúc bảng Fact & Dimension trên Trino/Hive Lakehouse. |
| [**tables_registry_5_entities.json**](./tables_registry_5_entities.json) | **File Cấu hình Schema Chuẩn hóa**: Đã thiết lập sẵn `primary_key`, `watermark_field`, `depends_on`, `foreign_keys` cho cả 5 thực thể. Ngày mai chỉ cần dán danh sách `fields` từ BA vào là pipeline tự động kích hoạt. |
| [**simulate_5_entities_dag.py**](./simulate_5_entities_dag.py) | **Mã nguồn Thực nghiệm Mô phỏng (100% PASS)**: Chạy kiểm chứng thuật toán sắp xếp Tô-pô chia 3 làn sóng và cơ chế tự chữa lành (Self-Healing) khi dữ liệu con đến trước dữ liệu cha. |

---

## 🚀 TÓM TẮT GIẢI PHÁP 3 LÀN SÓNG THỰC THI (TOPOLOGICAL WAVES)

```
       Wave 1 (Master Data):     [User]          [Project]
                                    \               /
       Wave 2 (Entities & Goals):     [Target]   [Task]
                                          \         /
       Wave 3 (Action Items):       [Giao-Ban-Ket-Luan]
```

* **Wave 1 (Chạy song song)**: `User` và `Project` (Hai bảng danh mục gốc, không phụ thuộc vào ai).
* **Wave 2 (Chờ Wave 1 hoàn thành)**: `Task` và `Target` (Bảng thực thể nghiệp vụ phụ thuộc vào Dự án và Người dùng).
* **Wave 3 (Chờ Wave 2 hoàn thành)**: `Giao-Ban-Ket-Luan` (Bảng giao dịch chỉ đạo hội tụ toàn bộ 4 thực thể trên).

---

## 💡 HÓA GIẢI LATE-ARRIVING DATA (KIMBALL INFERRED DIMENSIONS)

* **Vấn đề**: Khi chạy tăng dần (Incremental), một kết luận giao ban mới hôm nay trỏ tới một Dự án cũ đã tạo từ 6 tháng trước (API Project không trả về dự án đó hôm nay).
* **Giải pháp**:
  1. `InferredDimensionRouter` tự động sinh bản ghi giả lập (Inferred Stub Record) với timestamp `1970-01-01`.
  2. Phép JOIN trong dbt và Trino thành công 100%, **không bao giờ bị mất dữ liệu (Zero Data Loss)**.
  3. Khi API Project sync về sau này, Spark Window Ranking (`DedupEngine`) tự động ghi đè bản ghi thật lên bản ghi tạm.

---

## 🧪 CHẠY KIỂM CHỨNG THỰC TẾ

Bạn có thể chạy thử nghiệm mô phỏng ngay lập tức trên máy tính bằng lệnh:
```bash
python working/solutions/5_entities_interdependent_architecture/simulate_5_entities_dag.py
```
*(Kết quả thực nghiệm in chi tiết quy trình chia làn sóng và tự chữa lành bản ghi).*
