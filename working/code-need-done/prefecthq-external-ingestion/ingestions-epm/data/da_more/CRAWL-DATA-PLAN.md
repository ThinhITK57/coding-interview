* **Đặc tính dữ liệu của 6 nhóm thực thể:**

| Thực thể API | Số bản ghi ước tính | Tần suất thay đổi dữ liệu | Chiến lược Crawl | Tần suất & Khung giờ chạy | Số Request tiêu tốn (Ước tính) |
|:---|:---:|:---|:---:|:---|:---:|
| **`tasks`** | ~50.000 | **Rất cao** (Nhân viên cập nhật timesheet, status, % complete liên tục) | **Incremental** (theo `LastUpdatedOn`) + **Full weekly** (Chủ nhật) | • Incremental: 2 giờ/lần (8:00 – 18:00, 6 lần/ngày)<br>• Full: 02:00 sáng Chủ nhật | • Incremental: ~5-15 req/lần $\times 6 \approx 60$ req<br>• Full: ~200 req/tuần |
| **`projects`** | ~2.000 | **Trung bình** (PM cập nhật mốc tiến độ, track status theo tuần/tháng) | **Incremental** hàng ngày + **Full** đầu tháng | • Incremental: 2 lần/ngày (12:00 & 18:30)<br>• Full: 01:00 sáng ngày 1 hàng tháng | • Incremental: ~2 req/lần $\times 2 = 4$ req<br>• Full: ~8 req |
| **`targets`** | ~10.000 | **Trung bình** (Cập nhật kết quả định kỳ theo tuần/kỳ đánh giá) | **Incremental** hàng ngày | • Incremental: 2 lần/ngày (12:30 & 19:00) | • Incremental: ~5 req/lần $\times 2 = 10$ req |
| **`c_assignments`** | ~1.000 | **Thấp** (Giao việc phát sinh theo đợt giao ban tháng/quý) | **Full Crawl** hàng ngày | • Full: 1 lần/ngày (03:00 sáng) | • Full: ~4 req/ngày |
| **`objectives` (BSC)**| ~500 | **Rất thấp** (Mục tiêu chiến lược năm, chốt theo quý) | **Full Crawl** hàng ngày | • Full: 1 lần/ngày (03:30 sáng) | • Full: ~2 req/ngày |
| **`user_access_log`** | ~5.000 logs/ngày| **Chỉ thêm mới** (Log sự kiện đăng nhập và phiên làm việc) | **Incremental** theo ngày ($T-1$) | • Incremental: 1 lần/ngày (01:00 sáng, lấy ngày hôm trước) | • Incremental: ~20 req/ngày |
| **TỔNG CỘNG** | | | | **Hàng ngày: ~100 – 120 requests/ngày** *(Dư 88% quota dự phòng)* | |

---