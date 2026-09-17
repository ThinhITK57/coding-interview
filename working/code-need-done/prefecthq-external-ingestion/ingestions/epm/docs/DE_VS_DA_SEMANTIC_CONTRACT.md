# HỢP ĐỒNG DỮ LIỆU GIỮA DATA ENGINEER & DATA ANALYST (DE vs DA CONTRACT)
## KIẾN TRÚC TẦNG METRICS CHO GenBI VÀ CÁC BÀI TOÁN NGHIỆP VỤ

> **Dự án:** `crawler-prefecthq-02`  
> **Áp dụng cho:** 6 Business Requirements (BR-01 đến BR-06)  
> **Vị trí tài liệu:** `working/crawler-prefecthq-02/crawler-prefecthq/docs/DE_VS_DA_SEMANTIC_CONTRACT.md`  

---

## 1. Tuyên Ngôn Kiến Trúc & Giới Hạn Trách Nhiệm

1. **Cam kết của Data Engineer (DE):**
   - Đảm bảo tính tươi mới (freshness), tính toàn vẹn (integrity), tính nhất quán (consistency) và hiệu năng truy vấn của dữ liệu gốc.
   - Chuẩn hóa kiểu dữ liệu, khử trùng lặp khóa chính `sysid`, nối các bảng theo đúng quan hệ khóa ngoại nghiệp vụ.
   - Cung cấp các View vật lý và Bảng Gold chỉ chứa **ĐÚNG VÀ ĐỦ** các cột được yêu cầu bởi 6 Business Requirements.
   - **Tuyệt đối KHÔNG tự ý suy diễn hoặc tính toán sẵn các cột chỉ số (Derived measures) vào bảng vật lý.**

2. **Quyền hạn & Trách nhiệm của Data Analyst (DA):**
   - Độc quyền định nghĩa các công thức kinh doanh, tỷ lệ hoàn thành (Achievement rate), khoảng cách chênh lệch (GAP), cờ quá hạn (Overdue flags), và phân loại rủi ro (Risk scoring).
   - Khai báo các công thức này dưới dạng **Metadata và Metrics** tại tầng **dbt Semantic Layer** (`meta.semantic_guidance`).
   - Hướng dẫn Trợ lý Trí tuệ Nhân tạo (GenBI) cách đọc hiểu metadata để sinh ra câu lệnh SQL động theo ngữ cảnh của người dùng cuối.

---

## 2. Ma Trận Đối Chiếu Chi Tiết Cho 6 Bài Toán Nghiệp Vụ

### BR-01: Báo cáo BSC trong năm
- **Nguồn dữ liệu vật lý:** `epm_targets` kết hợp `epm_c_assignments` và `epm_objectives`.
- **Bảng/View vật lý (DE bàn giao - 17 cột thuần túy):**
  1. `associated_objective` (STRING): Mã mục tiêu chiến lược
  2. `associated_item` (STRING): Mã dự án liên kết
  3. `target_type` (STRING): Loại chỉ tiêu
  4. `parent_target` (STRING): Chỉ tiêu cha
  5. `c_department` (STRING): Phòng ban thực hiện
  6. `name` (STRING): Tên chỉ tiêu
  7. `c_assignee` (STRING): Người cam kết thực hiện
  8. `unit` (STRING): Đơn vị tính
  9. `target_date_m` (DATE): Hạn chốt mốc M
  10. `target_value_m` (DOUBLE): Giá trị mục tiêu M
  11. `target_date_n` (DATE): Hạn chốt mốc N
  12. `target_value_n` (DOUBLE): Giá trị mục tiêu N
  13. `state` (STRING): Trạng thái vòng đời
  14. `status` (STRING): Hiện trạng tiến độ
  15. `target_result_m` (DOUBLE): Kết quả thực tế mốc M
  16. `target_result_n` (DOUBLE): Kết quả thực tế mốc N
  17. `assignor` (STRING): Người giao việc
- **Chỉ số suy diễn DA định nghĩa trong dbt Semantic Layer (KHÔNG có trong view vật lý):**
  - `achievement_rate_m` = `(target_result_m / target_value_m) * 100.0`
  - `achievement_rate_n` = `(target_result_n / target_value_n) * 100.0`
  - `gap_m` = `target_value_m - COALESCE(target_result_m, 0.0)`
  - `gap_n` = `target_value_n - COALESCE(target_result_n, 0.0)`
  - `is_achieved_m` = `CASE WHEN target_result_m >= target_value_m THEN 1 ELSE 0 END`

---

### BR-02: Báo cáo điều hành CVCT/KLCĐ
- **Nguồn dữ liệu vật lý:** `epm_targets` / `epm_tasks`.
- **Bảng/View vật lý (DE bàn giao - 8 cột thuần túy):**
  1. `assignee` (STRING): Cán bộ phụ trách
  2. `resources` (STRING): Nhân sự thực hiện
  3. `name` (STRING): Tên công việc/chỉ tiêu
  4. `status` (STRING): Trạng thái
  5. `due_date` (DATE): Hạn hoàn thành
  6. `percent_completed` (DOUBLE): % hoàn thành
  7. `target_value_m` (DOUBLE): Giá trị mục tiêu M
  8. `target_date_m` (DATE): Ngày hạn chốt M
- **Chỉ số suy diễn DA định nghĩa trong dbt Semantic Layer:**
  - `is_overdue` = `CASE WHEN due_date < CURRENT_DATE AND percent_completed < 100.0 THEN 1 ELSE 0 END`
  - `days_to_due` = `DATE_DIFF('day', CURRENT_DATE, due_date)`
  - `progress_gap` = `100.0 - percent_completed`
  - `is_completed` = `CASE WHEN percent_completed >= 100.0 THEN 1 ELSE 0 END`

---

### BR-03: Báo cáo Task
- **Nguồn dữ liệu vật lý:** `epm_tasks`.
- **Bảng/View vật lý (DE bàn giao - 15 cột thuần túy):**
  1. `task_type` (STRING): Loại task
  2. `assignee` (STRING): Người thực hiện
  3. `department` (STRING): Phòng ban
  4. `resources` (STRING): Số lượng nhân lực
  5. `parent_project` (STRING): Dự án cha
  6. `jira_status` (STRING): Trạng thái Jira
  7. `name` (STRING): Tên task
  8. `description` (STRING): Thuyết minh
  9. `work` (DOUBLE): Tổng giờ công kế hoạch
  10. `duration` (DOUBLE): Thời lượng ngày làm việc
  11. `epm_default` (STRING): EPM mặc định
  12. `start_date` (DATE): Ngày bắt đầu
  13. `due_date` (DATE): Hạn kết thúc
  14. `percent_completed` (DOUBLE): Tiến độ %
  15. `update_description` (STRING): Nội dung giải trình cập nhật
- **Chỉ số suy diễn DA định nghĩa trong dbt Semantic Layer:**
  - `is_delayed` = `CASE WHEN due_date < CURRENT_DATE AND percent_completed < 100.0 THEN 1 ELSE 0 END`
  - `remaining_work` = `work * (1.0 - percent_completed / 100.0)`
  - `planned_daily_effort` = `CASE WHEN duration > 0 THEN work / duration ELSE work END`

---

### BR-04: Báo cáo Dự án
- **Nguồn dữ liệu vật lý:** `epm_projects`.
- **Bảng/View vật lý (DE bàn giao - 11 cột thuần túy):**
  1. `name` (STRING): Tên dự án
  2. `project_type` (STRING): Loại dự án
  3. `due_date` (DATE): Hạn chót kết thúc
  4. `state` (STRING): Trạng thái vòng đời
  5. `status` (STRING): Tình trạng tiến độ
  6. `percent_completed` (DOUBLE): % hoàn thành
  7. `department` (STRING): Phòng ban chủ trì
  8. `assignor` (STRING): Người giao dự án
  9. `assignee` (STRING): Đầu mối tiếp nhận
  10. `project_manager` (STRING): Giám đốc dự án (PM)
  11. `resources` (STRING): Nguồn lực tham gia
- **Chỉ số suy diễn DA định nghĩa trong dbt Semantic Layer:**
  - `is_project_overdue` = `CASE WHEN due_date < CURRENT_DATE AND percent_completed < 100.0 THEN 1 ELSE 0 END`
  - `active_projects_count` = `COUNT(CASE WHEN state = '/State/Active' THEN 1 END)`
  - `portfolio_average_progress` = `AVG(percent_completed)`

---

### BR-05: Báo cáo Lưu lượng truy cập (User Access Log)
- **Nguồn dữ liệu vật lý:** `epm_user_access_log`.
- **Bảng/View vật lý (DE bàn giao - 7 cột thuần túy):**
  1. `login_date` (DATE): Ngày đăng nhập
  2. `name` (STRING): Tài khoản người dùng
  3. `first_name` (STRING): Tên
  4. `last_name` (STRING): Họ
  5. `groups` (STRING): Nhóm phòng ban
  6. `direct_manager` (STRING): Quản lý trực tiếp
  7. `job_title` (STRING): Chức danh công việc
- **Chỉ số suy diễn DA định nghĩa trong dbt Semantic Layer:**
  - `daily_active_users (DAU)` = `COUNT(DISTINCT name)`
  - `total_sessions` = `COUNT(*)`
  - `logins_per_user` = `COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT name), 0)`

---

### BR-06: Báo cáo Mục tiêu Ban Giám đốc (Mục tiêu BG)
- **Nguồn dữ liệu vật lý:** `epm_targets` kết hợp `epm_objectives` và `epm_c_assignments`.
- **Bảng/View vật lý (DE bàn giao - 17 cột thuần túy):**
  1. `associated_objective` (STRING): Mục tiêu BSC liên kết
  2. `associated_item` (STRING): Dự án liên kết
  3. `target_type` (STRING): Loại chỉ tiêu
  4. `parent_target` (STRING): Chỉ tiêu cha
  5. `c_department` (STRING): Phòng ban chịu trách nhiệm
  6. `name` (STRING): Tên chỉ tiêu Ban Giám đốc
  7. `c_assignee` (STRING): Người nhận việc
  8. `unit` (STRING): Đơn vị tính
  9. `c_target_date_m` (DATE): Hạn chốt mốc M
  10. `c_target_date_n` (DATE): Hạn chốt mốc N
  11. `c_target_value_m` (DOUBLE): Giá trị cam kết M
  12. `c_target_value_n` (DOUBLE): Giá trị cam kết N
  13. `c_target_result_m` (DOUBLE): Kết quả thực tế M
  14. `c_target_result_n` (DOUBLE): Kết quả thực tế N
  15. `state` (STRING): Trạng thái vòng đời
  16. `status` (STRING): Hiện trạng tiến độ
  17. `c_assignor` (STRING): Lãnh đạo Ban Giám đốc giao việc
- **Chỉ số suy diễn DA định nghĩa trong dbt Semantic Layer:**
  - `bg_achievement_rate_m` = `(c_target_result_m / c_target_value_m) * 100.0`
  - `bg_achievement_rate_n` = `(c_target_result_n / c_target_value_n) * 100.0`
  - `bg_gap_m` = `c_target_value_m - COALESCE(c_target_result_m, 0.0)`
  - `bg_gap_n` = `c_target_value_n - COALESCE(c_target_result_n, 0.0)`
