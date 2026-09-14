# Quản trị sản xuất (1) — Mô tả dashboard, chart và prompt tạo trên Lightdash

> Nguồn: `powerbi_output/production_management_1/`
> Quy mô: **49 trang · 181 chart · 277 bộ lọc · 94 bảng dữ liệu**
> Ký hiệu ⚠️ ✅ 🔁 🐍 xem ở [00_huong_dan_va_quy_uoc_prompt.md](00_huong_dan_va_quy_uoc_prompt.md)

## Báo cáo này nói về cái gì

Đây là bộ báo cáo lớn nhất của khối sản xuất, gồm **ba mảng gần như độc lập** nằm chung một file:

| Mảng | Nội dung | Người dùng chính |
|---|---|---|
| **A. OS / PYC** | Quản trị phần việc thuê ngoài: phiếu yêu cầu, man-month cam kết – thực tế – thanh toán, độ ổn định kế hoạch | Ban điều hành sản xuất, bộ phận quản lý đối tác |
| **B. KPIKQI khách hàng** | Cam kết chất lượng dịch vụ với từng nhóm khách (VVIP / VIP / Viettel / SOCP), theo từng sản phẩm bảo mật (EDR, NDR, NSM, SIEM, SOAR, KIAN, SOC Platform…) | Ban giám đốc, đội chăm sóc khách hàng |
| **C. Quản trị sản xuất công ty** | Sức khỏe nội bộ: release, cycle time, bug production, phân bổ nguồn lực, CSAT/NPS | Giám đốc trung tâm, quản lý dự án |

Vì gộp ba mảng nên có **rất nhiều trang trùng lặp** — riêng mảng B có 16 trang gần như y hệt nhau, chỉ khác đúng một bộ lọc sản phẩm. Phần cuối tài liệu có gợi ý gom lại.

## Bản đồ 49 dashboard

### Mảng A — OS / PYC

| # | Dashboard | Chart | Vai trò |
|---|---|---|---|
| [D0](#d0--pyc-os) | PYC OS | 1 | Bảng tổng hợp hiệu quả OS theo sản phẩm |
| [D1](#d1--ttsxcntt-os--độ-ổn-định-kế-hoạch) | TTSX&CNTT OS - ĐỘ ỔN ĐỊNH KẾ HOẠCH | 7 | Đo mức độ "đổi kế hoạch giữa chừng" |
| [D2](#d2--nội-bộ-ttsxcntt--nguồn-lực-os) | Nội bộ TTSX&CNTT - NGUỒN LỰC OS | 9 | Bản nội bộ của D10 |
| [D10](#d10--ttsxcntt--nguồn-lực-os) | TTSX&CNTT - NGUỒN LỰC OS | 11 | Trang OS chính thức |
| [D11](#d11--ttsxcntt--đơn-vị-os) | TTSX&CNTT - ĐƠN VỊ OS | 3 | Thanh toán OS theo đối tác / hợp đồng |
| [D12](#d12--release---chi-tiết) | Release - Chi tiết | 1 | Bảng release theo version |

### Mảng B — KPIKQI khách hàng

| # | Dashboard | Chart | Vai trò |
|---|---|---|---|
| [D5](#d5--kpikqi-tập-đoàn) | KPIKQI Tập đoàn | 2 | Chỉ tiêu báo cáo lên Tập đoàn |
| [D6](#d6--kpikqi-tập-đoàn-bảng-thô) | KPIKQI Tập đoàn *(bảng thô)* | 1 | Bảng dữ liệu gốc của D5 |
| [D7](#d7--1-socp-viettel) | 1. SOCP-VIETTEL | 5 | KPI nhóm khách nội bộ Viettel + SOC Platform |
| [D8](#d8--2-vvip) | 2. VVIP | 12 | KPI nhóm khách VVIP theo từng sản phẩm |
| [D9](#d9--kpikqi-soc) | KPIKQI-SOC | 10 | KPI dịch vụ giám sát an ninh (SOC) |
| [D13](#d13--5-báo-cáo-định-kỳ) | 5. BÁO CÁO ĐỊNH KỲ | 2 | Danh sách vấn đề – nguyên nhân – giải pháp |
| [D19](#d19--xu-hướng-kpikqi) | Xu hướng KPIKQI | 1 | Khách nào đang cải thiện / suy giảm |
| [D20](#d20--tooltip-nguyên-nhân-lỗi) | Tooltip Nguyên nhân lỗi | 1 | Bảng bung ra khi hover |
| [D35](#d35--3-vip) | 3. VIP | 10 | Bản VIP của D8 |
| [D41](#d41--kpikqi) | KPIKQI | 5 | Bản rút gọn của D7 |
| [D42](#d42--2-vvip-old) | 2. VVIP-old | 13 | Bản cũ của D8, có thêm 2 bảng hữu ích |
| [D43](#d43--4-dịch-vụ) | 4. DỊCH VỤ | 5 | KPI cấp dịch vụ (không gắn khách cụ thể) |
| [D47](#d47--kpikqi-sản-phẩm) | KPIKQI Sản phẩm | 1 | Bảng thô KPI sản phẩm |
| [D3, D14–D17, D24–D34, D37, D38, D48](#nhóm-trang-bảng-chi-tiết-kpikqi--19-trang-cùng-một-khuôn) | *(19 trang bảng chi tiết)* | 19 | Cùng một bảng, khác bộ lọc sản phẩm |

### Mảng C — Quản trị sản xuất công ty

| # | Dashboard | Chart | Vai trò |
|---|---|---|---|
| [D4](#d4--qtsx---quý-i) | QTSX - Quý I | 1 | Phân bổ man-month release theo mục đích |
| [D18](#d18--project-manmonth-fix-version) | Project *(manmonth fix version)* | 3 | Công đổ vào từng fix version |
| [D21](#d21--fix-versions) | FIX VERSIONS | 1 | Bảng thô fix version |
| [D22](#d22--check-chi-tiết) | Check Chi tiết | 1 | Bảng đối soát số liệu |
| [D23](#d23--project) | Project | 8 | Sức khỏe project: bug, defect, nguồn lực |
| [D36](#d36--cldv-bản-rút-gọn) | CLDV *(rút gọn)* | 7 | Chất lượng dịch vụ: CSAT, NPS, độ phủ |
| [D39](#d39--cldv-bản-đầy-đủ) | CLDV *(đầy đủ)* | 14 | Bản đầy đủ của D36 + sự cố, MTTD/MTTR |
| [D40](#d40--tổng-quan) | TỔNG QUAN | 13 | Trang chủ điều hành toàn công ty |
| [D44](#d44--qtsx-trung-tâm) | QTSX TRUNG TÂM | 7 | Sức khỏe sản xuất theo trung tâm |
| [D45](#d45--chi-tiết) | Chi tiết | 1 | Bảng tổng hợp mọi chỉ số theo project |
| [D46](#d46--ttsx) | TTSX | 6 | Bug nghiệm thu và hiệu quả theo trung tâm |

## Model dbt hay dùng nhất

| Model | Nội dung | Xuất hiện ở |
|---|---|---|
| `production_management_1_kpikqi_new` | **Bảng trung tâm của mảng B.** Kết quả KPI/KQI theo khách hàng × sản phẩm × chỉ tiêu × kỳ | Gần như mọi trang mảng B |
| `production_management_1_soc_results` | Kết quả KPI riêng cho dịch vụ SOC (MTTD, MTTR, alert bỏ lọt…) | D9, D14 |
| `production_management_1_kpikqi_all` | KPI cấp dịch vụ, không gắn khách hàng cụ thể | D37, D40, D43 |
| `production_management_1_pyc_jira` | **Bảng trung tâm của mảng A.** Phiếu yêu cầu OS lấy từ Jira | D0, D2, D10, D11, D44 |
| `production_management_1_pyc_snapshot` | Ảnh chụp lịch sử commit của PYC — dùng để đo số lần đổi kế hoạch | D1, D2 |
| `production_management_1_bug_production` / `_bug_os` / `_defect` | Ba tầng lỗi: production, nghiệm thu OS, defect nội bộ | D23, D40, D44, D46 |
| `production_management_1_fix_versions`, `_mm_release_versions`, `_mmrelease` | Man-month gắn với từng fix version / bản release | D4, D12, D18, D21, D23 |
| `production_management_1_fact_tong_hop_diem` | Điểm CSAT / NPS từ khảo sát khách hàng | D36, D39 |
| `production_management_1_project_name`, `_division`, `_dim_date` | Bảng chiều: dự án, khối/trung tâm, thời gian | Mọi trang |

## Cảnh báo trước khi bắt tay

- **180 DAX measure không dịch tự động được** — nhiều gấp rưỡi QTSX V2. Đợt 2026-08 đã viết tay xong nhóm `priority ≥ 2` (46 measure), xem [Trạng thái metric](#trạng-thái-metric-cập-nhật-2026-08) bên dưới. Nhóm khó nhất:
  - `userelationship` (29 measure): Power BI đổi quan hệ giữa bảng ngay trong measure, ví dụ cùng bảng PYC nhưng lúc tính theo *ngày commit*, lúc theo *ngày nghiệm thu*. Trên dbt phải tách thành nhiều model hoặc nhiều cột ngày rồi khai báo metric riêng cho từng mốc. Đây là lý do bạn thấy hàng loạt metric tên `mm_*_commit_date`, `mm_*_acceptance`, `mm_*_du_kien`.
  - `filter_shape` / `removefilters` (36 measure): measure tự bỏ qua bộ lọc của người dùng để tính mẫu số. Trên Lightdash cần viết bằng window function hoặc CTE riêng.
- **`production_management_1_dim_date` trước đây rỗng — ảnh hưởng 55/181 chart.** `Dim_Date` là bảng DAX `CALENDAR()`, VertiPaq không lưu dữ liệu cột sinh bởi `ADDCOLUMNS`, nên `Date`, `Year`, `Month`, `MonthName`, `Quarter`, `MonthYear`, `QuarterYear`, `WeekStart`, `WeekEnd` trong `Dim_Date.tsv` đều **rỗng** (chỉ `Tuần`, `DayInWeek`, `SortKey`, `QuarterSortKey`, `WeekSortKey` có giá trị). Model extract cũng không giữ được quan hệ nào giữa `Dim_Date` và bảng fact, nên các `__schema.yml` không có join sang `_dim_date`.
  - **Đã vá** bằng `powerbi_output/production_management_1/sql_overrides/production_management_1_dim_date.sql` — dựng lại toàn bộ lịch `CALENDAR(2024-12-01, 2030-12-31)` bằng SQL, không phụ thuộc bảng silver nữa. Đã đối chiếu với dữ liệu export: **2 222/2 222 dòng khớp tuyệt đối** trên cả 5 cột còn dữ liệu (`Tuần`, `DayInWeek`, `SortKey`, `QuarterSortKey`, `WeekSortKey`).
  - **Vẫn còn thiếu `meta.joins`** từ các bảng fact sang `_dim_date` (DataModel không giữ quan hệ nào để suy ra). Với measure `userelationship` thì không nên join dim_date: mỗi measure cần một mốc ngày riêng, một join không đủ — xem [D0](#d0--pyc-os) và model `_pyc_os_events` làm mẫu.
- **6 chart là Python visual** 🐍 (D23, D36, D40) — Lightdash không dựng lại được. Chúng đều là biểu đồ bong bóng nhiều chiều so sánh project. Đề xuất thay bằng **Scatter chart** (trục X = cycle time, trục Y = % bug production, kích thước = man-month) hoặc bảng có thanh dữ liệu.
- **Trang trùng lặp rất nhiều**: D2 ≈ D10, D8 ≈ D42, D36 ⊂ D39, D22 ≈ D45. Xem bảng gợi ý gom cuối tài liệu trước khi dựng.
- **1 chart hỏng sẵn trong file Power BI**, không phải lỗi migration: chart *PYC OS* của trang **TTSX** trỏ vào bảng `PYC OS` — bảng này **không tồn tại** trong model của báo cáo này (đó là tên bảng của báo cáo QTSX V2). Visual đã chết từ trong `.pbix`; bỏ qua khi dựng lại.

## Trạng thái metric *(cập nhật 2026-08)*

Metric viết tay để ở `powerbi_output/production_management_1/metrics_overrides.yml` và
các model viết tay trong `dbt/models/`; `powerbi_tools/run.py` merge vào `*__schema.yml`
nên chạy lại không mất. **Đã làm xong toàn bộ measure `priority ≥ 1`** — tức mọi measure
đang được ít nhất một chart sử dụng (90 measure).

**Độ phủ hiện tại: 408/419 lượt tham chiếu metric (97%) đã có sẵn trong dbt.**
Còn 9/181 chart thiếu metric, đều thuộc 5 measure cố ý không dựng lại (xem cuối mục).

**Bốn model trung gian viết tay** — mỗi cái giải một kiểu DAX mà join phẳng của Lightdash không làm được:

| Model | Giải bài toán gì | Metric |
|---|---|---|
| `production_management_1_pyc_os_events` | Measure `USERELATIONSHIP` của `PYC Jira` dùng **6 cột ngày khác nhau** (release / nghiệm thu / bàn giao / commit / thanh toán / dự kiến). Fan-out mỗi PYC thành 1 dòng cho mỗi mốc ngày, metric tự lọc `date_role`. | 28 |
| `production_management_1_pyc_snapshot_events` | `PYC-Snapshot`: vừa `USERELATIONSHIP` 2 cột ngày, vừa `SUMX(VALUES(ISSUE_KEY), MAX(...))`, vừa `SUMX(VALUES(MonthYear), …)`. Grain (PYC × mốc ngày × **tháng**) nướng sẵn cả ba. | 18 |
| `production_management_1_mm_ratio_by_project_month` | Measure "chia khối lượng cho man-month" (`Bug Production`, `ULNL`, `FixVersions`, `Worklogs`, `mm_QA_fixversion`, `MM OS QI-2026` ÷ `mamday hr`). Join thẳng sẽ **fan-out** làm mẫu số bị thổi lên; gộp về (tháng × sản phẩm) trước rồi mới chia. | 18 |
| `production_management_1_defect_rate_by_month` | `% Tỉ lệ defect` dùng `REMOVEFILTERS('Khối')` ở **cả tử lẫn mẫu** ⇒ con số không phụ thuộc khối. Gộp về grain **tháng**, cố ý bỏ cột khối. | 2 |

Các model này **không vô hình với pipeline**: `run.py` mỗi lần chạy đều kiểm `ref()` trỏ
đúng model, `source()` trỏ đúng bảng silver, và `sql` của metric chỉ dùng cột đã khai —
sai chỗ nào in ra `! model viết tay: …` và ghi vào `report.md` (mục *Model viết tay*).

**Đổi tên — measure `%` giờ có tiền tố `pct_`.** Trước đây bộ sinh bỏ ký tự `%` khi đặt
tên, nên `PYC RELEASE` và `%PYC RELEASE` ra **cùng một slug**: một trong hai bị ghi đè và
**biến mất khỏi `measures_todo.yml`**, còn `lightdash.field` trong `layout/charts/*.json`
thì trỏ chung một tên cho hai measure khác nghĩa. Riêng báo cáo này có **6 cặp** như vậy.

Đã sửa ở `powerbi_tools/run.py` (hàm `measure_slug`): mọi measure tên bắt đầu bằng `%`
đều được đặt tên `pct_…`. Nhờ đó `measures_todo.yml` từ **180 lên 187 measure** — 7 measure
trước đây bị nuốt nay đã hiện ra. **25 metric của báo cáo này đổi tên**, ví dụ:

| Measure Power BI | Tên cũ (sai) | Tên mới |
|---|---|---|
| `PYC RELEASE` / `%PYC RELEASE` | `pyc_release` cho cả hai | `pyc_release` / `pct_pyc_release` |
| `PYC RELEASE ĐÚNG HẠN` / `%PYC RELEASE ĐÚNG HẠN` | `pyc_release_on_time` cho cả hai | `pyc_release_on_time` / `pct_pyc_release_on_time` |
| `MM Fix Bug RELEASE` / `%mm fix bug release` | `mm_fix_bug_release` cho cả hai | `mm_fix_bug_release` / `pct_mm_fix_bug_release` |
| `MM RELEASE TÍNH NĂNG` / `%mm release tính năng` | `mm_release_tinh_nang` cho cả hai | `mm_release_tinh_nang` / `pct_mm_release_tinh_nang` |
| `MM RELEASE TÍNH NĂNG CHO KH` / `%…cho KH` | `mm_release_tinh_nang_cho_kh` cho cả hai | `mm_release_tinh_nang_cho_kh` / `pct_mm_release_tinh_nang_cho_kh` |
| `Worklog fix version` / `% worklog fix VERSION` | `worklog_fix_version` cho cả hai | `worklog_fix_version` / `pct_worklog_fix_version` |
| `%MM PYC RELEASE` | `mm_pyc_release` | `pct_mm_pyc_release` |
| `%CR` | `cr` | `pct_cr` |
| `% Tỉ lệ production bug` | `production_bug_rate` | `pct_production_bug_rate` |
| `% Tỉ lệ defect` | `rate_defect` | `pct_rate_defect` |
| `% Hiệu suất load người` | `hieu_suat_load_nguoi` | `pct_hieu_suat_load_nguoi` |
| `%LOGWORK FIXVERSIONS` / `%ULNL FIXVERSIONS` | `logwork_fixversions` / `ulnl_fixversions` | `pct_logwork_fixversions` / `pct_ulnl_fixversions` |
| `%Bug Production Thành công` | `bug_production_thanh_cong` | `pct_bug_production_thanh_cong` |
| `% PYC điều chỉnh Commit` | `pyc_dieu_chinh_commit` | `pct_pyc_dieu_chinh_commit` |

Các tên ⚠️ bắt đầu bằng dấu `%` trong bảng chart bên dưới đều theo quy tắc này.

**Đổi model.** Nhiều metric giờ nằm ở model khác với model ghi trong các prompt bên dưới.
Khi dựng chart, tra bảng này trước:

| Metric | Prompt ghi model | Thực tế nằm ở |
|---|---|---|
| `pyc_release`, `pyc_release_on_time`, `pyc_done_acceptance_thanh_cong`, `count_pyc`, `mm_pyc_*`, `mm_burned_ttsx_*`, `mm_commit_ttsx_*`, `mm_ttsx_*`, `mm_thanh_toan_ttsx`, `pct_cr`, `hieu_released`, `delta_thuc_passed_commit`, `bug_rate` | `_pyc_jira` | **`_pyc_os_events`** |
| `mm_commit_hien_tai`, `mm_snapshot_commit`, `mm_ttsx_commit_commit_date`, `count_pyc_snap`, `pyc_end_snap`, `pyc_on_time_snap`, `pct_pyc_done_on_time_snap`, `count_lan_dieu_chinh_hien_tai`, `max_count_lan_dieu_chinh`, `slcs_phat_sinh_1_month`, `count_pyc_co_dieu_chinh_commit`, `pyc_dieu_chinh_nhieu_lan`, `avg_count_lan_dieu_chinh_pyc`, `pct_pyc_dieu_chinh_commit`, `pct_pyc_dieu_chinh_nhieu_lan`, `commit_lan_dau`, `date_commit_hien_tai`, `count_date_dich_chuyen_commit` | `_pyc_snapshot` | **`_pyc_snapshot_events`** |
| `pct_production_bug_rate`, `count_bug_prd`, `count_bug_production`, `pct_hieu_suat_load_nguoi`, `mm_tong_fixversions`, `pct_logwork_fixversions`, `pct_ulnl_fixversions`, `mm_ulnl_release`, `sum_man_month`, `sum_ulnl_mm`, `ulnl_fix_version`, `pct_ulnl_done_release`, `sum_worklog_mm`, `pct_worklog`, `worklog_fix_version`, `pct_worklog_fix_version`, `mm_dev_team`, `mm_os` | `_bug_production` / `_ulnl_projects` / `_ulnl` / `_fix_versions` / `_mamday_hr` / `_worklogs` / `_mmrelease` | **`_mm_ratio_by_project_month`** |
| `pct_rate_defect` | `_defect` | **`_defect_rate_by_month`** |
| `pct_bug_production_thanh_cong` | `_project_vs_department` | **`_bug_production`** |
| `release_frequency_dgcl_ttk` | `_project_vs_department` | **`_dgcl_ttk`** |
| `count_bug_order`, `rate_acceptance_bug_thanh_cong`, `count_order_acceptance_bug` | `_project_vs_department` | **`_bug_os`** |

**Cố ý KHÔNG dựng lại — 5 measure, đều là trang trí chứ không phải số liệu:**

| Measure | Vì sao | Dựng bằng gì trên Lightdash |
|---|---|---|
| `report_week_new` | chuỗi `"Báo cáo tuần: Tuần N (dd/MM/yyyy - dd/MM/yyyy)"` | **Markdown tile**, hoặc Big value trên `max_report_date` của `_kpikqi_new` |
| `mau_sac_nen_so_voi_ky_n_1`, `mau_sac_font_so_voi_ky_n_1` | trả mã màu `#008000` / `#FFF000`… để tô nền và chữ | **Conditional formatting** của Table trên cột `so_with_period_n_1_td` |
| `danh_sach_kh_cai_thien`, `danh_sach_kh_suy_giam` | `CONCATENATEX` nối tên khách bằng ký tự xuống dòng | **Table** với dimension `customer_2`, lọc theo `quantity_kh_cai_thien` / `quantity_kh_suy_giam` |

Metric chuỗi trên Lightdash không lọc/sắp xếp/tổng hợp được nên không nên dựng lại;
ba nhóm trên đều có metric **số** tương ứng đã sẵn sàng.

**Ba metric lệch có chủ ý so với bản gốc** — đã ghi rõ trong `description` của từng metric:
`release_frequency_dgcl_ttk` (DAX lấy trung bình của các tỉ số theo khối, ở đây là tỉ số
của hai tổng), `pct_pyc_dieu_chinh_commit` (mẫu số là số PYC có trong bảng snapshot chứ không
phải toàn bộ PYC Jira), `tong_count_dv_fixed` (bộ lọc chuyển từ `KPIKQI_New[KPITYPE]` sang
`KPIKQI_ALL[kpi_type]` vì hai bảng không có quan hệ nào).

**Còn lại chưa làm:** chỉ còn measure `priority = 0` — **không chart nào dùng**, nên
không cần cho việc dựng lại dashboard. Danh sách đầy đủ ở `layout/measures_todo.yml`
(187 measure sau khi vá lỗi trùng slug).

---

## D0 — PYC OS

**Mục đích:** Một bảng duy nhất nhưng là bảng quan trọng nhất của mảng OS. Mỗi dòng là một sản phẩm TTSX, mỗi cột là một chặng trong vòng đời phiếu yêu cầu: release → nghiệm thu → bàn giao → thanh toán.

**Bộ lọc trang:** người phụ trách (`issue_current_assignee_name`), sản phẩm TTSX (`ttsx_product`), dự án, đối tác, năm, tháng, quý, khối, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **QUẢN TRỊ HIỆU QUẢ OS** · Table (pivot) | ✅ `production_management_1_pyc_os_events` *(model viết tay, xem ghi chú dưới)*<br>Hàng: `ttsx_product`<br>Cột: `year_value`, `month_year`<br>Giá trị: `pyc_release_on_time`, `pyc_release`, `pyc_done_acceptance_thanh_cong`, `pct_mm_pyc_release` (%), `pct_pyc_release` (%), `pct_pyc_release_on_time` (%), `mm_pyc_released`, `mm_pyc_acceptance_thanh_cong`, `mm_pyc_thuc_te_handover_date_ttsp`, `mm_pyc_thuc_te_ban_giao_acceptance` | Đọc theo hàng: sản phẩm này tháng vừa rồi release được bao nhiêu phiếu, trong đó bao nhiêu đúng hạn, bao nhiêu đã nghiệm thu thành công, và quy ra man-month là bao nhiêu. Ba cột MM cuối cho biết công đang nằm ở chặng nào. | Tạo Table pivot trên Lightdash từ model `production_management_1_pyc_os_events` (**không** dùng `_pyc_jira` + `_dim_date`, lý do ở ghi chú bên dưới). Hàng: `ttsx_product`. Cột: `year_value` rồi `month_year`. Cột giá trị theo đúng thứ tự bản gốc: `pyc_release_on_time`, `pyc_release`, `pyc_done_acceptance_thanh_cong`, `pct_mm_pyc_release` (%), `pct_pyc_release` (%), `pct_pyc_release_on_time` (%), `mm_pyc_released`, `mm_pyc_acceptance_thanh_cong`, `mm_pyc_thuc_te_handover_date_ttsp`, `mm_pyc_thuc_te_ban_giao_acceptance`. Sắp xếp cột theo `month_sort_key` tăng dần. Đặt tiêu đề "QUẢN TRỊ HIỆU QUẢ OS". |

**Ghi chú dựng lại D0 — ba vấn đề của bản sinh tự động và cách xử lý**

1. **10 measure của chart này chưa hề tồn tại trong Lightdash.** Trong `layout/charts/00_pyc_os/12_pivot_table_quan_tri_hieu_qua_os.json` cả 10 trường đều mang `status: measure_needs_sql`, tức `dbtgen` không sinh được metric nào. Tìm trong explore `PYC Jira` sẽ không ra — không phải sai tên.
2. **Trùng slug.** Power BI có hai cặp measure khác nhau nhưng slug hoá về cùng tên, khiến `measures_todo.yml` bị ghi đè và chỉ còn bản `%`:

   | Measure Power BI | Slug bị đụng | Tên dùng trong `_pyc_os_events` |
   |---|---|---|
   | `PYC RELEASE` (đếm) | `pyc_release` | `pyc_release` |
   | `%PYC RELEASE` (tỷ lệ) | `pyc_release` | `pct_pyc_release` |
   | `PYC RELEASE ĐÚNG HẠN` (đếm) | `pyc_release_on_time` | `pyc_release_on_time` |
   | `%PYC RELEASE ĐÚNG HẠN` (tỷ lệ) | `pyc_release_on_time` | `pct_pyc_release_on_time` |
   | `%MM PYC RELEASE` | `mm_pyc_release` | `pct_mm_pyc_release` |

3. **`production_management_1_dim_date` không dùng được làm trục thời gian.** `Dim_Date` là bảng DAX `CALENDAR()`; VertiPaq không lưu dữ liệu của cột sinh bởi `ADDCOLUMNS`, nên trong `Dim_Date.tsv` (2 222 dòng) các cột `Date`, `Year`, `Month`, `MonthName`, `Quarter`, `MonthYear`, `QuarterYear`, `WeekStart`, `WeekEnd` đều **rỗng** — chỉ `Tuần`, `DayInWeek`, `SortKey`, `QuarterSortKey`, `WeekSortKey` có giá trị. Model extract cũng **không có quan hệ nào** giữa `PYC Jira` và `Dim_Date` (`relationships` = 0 với `Dim_Date`), nên `_pyc_jira` cũng không có join sang `_dim_date`.

   Ngoài ra 10 measure dùng `USERELATIONSHIP` với **ba cột ngày khác nhau** (`Ngày_Release_10806`, `Ngày_nghiệm_thu_thành_công_10847`, `Ngày_bàn_giao_10849`) — một join active duy nhất của Lightdash không phục vụ được cả ba.

   Cách xử lý: model viết tay `production_management_1_pyc_os_events` fan-out mỗi PYC thành 1 dòng cho mỗi mốc ngày có giá trị (`date_role` ∈ release / acceptance / handover), tự sinh `year_value` / `month_year` / `quarter_year` từ chính `event_date` theo đúng định dạng của Power BI (`MMM YYYY`, `Qn YYYY`). Mỗi metric lọc theo `date_role` nên một query duy nhất vẫn ra đủ 10 cột đúng ngữ nghĩa DAX gốc. File: `dbt/models/production_management_1_pyc_os_events.sql` + `__schema.yml`. Tên file mới nên `run.py` không ghi đè. **Grain là (PYC × mốc ngày)** — đừng thêm metric đếm/cộng không lọc `date_role` vào model này.

   Model này cũng dùng lại được cho **D10-10** và **D10-11**.

4. **Ba cột % có thể vượt 100%** — đây là hành vi của bản gốc, không phải lỗi dựng lại: tử số đếm theo *ngày release*, mẫu số đếm theo *ngày nghiệm thu thành công*, hai mốc rơi vào tháng khác nhau. Ví dụ trên dữ liệu thật: `Automation Test` / Dec 2025 → 7 PYC release, 2 PYC nghiệm thu ⇒ `%PYC RELEASE` = 350%. Nếu muốn con số đọc được thì phải đổi định nghĩa nghiệp vụ, cần người dùng chốt.

---

## D1 —  TTSX&CNTT OS - ĐỘ ỔN ĐỊNH KẾ HOẠCH

**Mục đích:** Trả lời một câu hỏi cụ thể và rất hay bị né: **kế hoạch giao cho đối tác có bị sửa nhiều không**. Nếu một phiếu bị dời ngày commit 3–4 lần thì con số "đúng hạn" ở D0 không còn nhiều ý nghĩa.

Dữ liệu lấy từ `pyc_snapshot` — bảng lưu **mọi lần** ngày commit bị thay đổi, chứ không chỉ giá trị hiện tại.

**Bộ lọc trang:** trạng thái phiếu, dự án, sản phẩm TTSX, người phụ trách, năm, tháng, quý, khối, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(số PYC có điều chỉnh)* · Big value | `production_management_1_pyc_snapshot` · ⚠️`count_pyc_co_dieu_chinh_commit` | Bao nhiêu phiếu đã bị dời ngày cam kết ít nhất một lần. | Tạo Big value từ `production_management_1_pyc_snapshot`, metric `count_pyc_co_dieu_chinh_commit`. Đặt tiêu đề "Số PYC có điều chỉnh Commit". |
| 2 | *(số lần điều chỉnh trung bình)* · Big value | `production_management_1_pyc_snapshot` · ⚠️`avg_count_lan_dieu_chinh_pyc` | Trung bình mỗi phiếu bị dời bao nhiêu lần. Trên 2 là dấu hiệu kế hoạch đầu vào không chắc. | Tạo Big value từ `production_management_1_pyc_snapshot`, metric `avg_count_lan_dieu_chinh_pyc`. Đặt tiêu đề "Avg số lần điều chỉnh / PYC". |
| 3 | *(% PYC điều chỉnh)* · Big value | `production_management_1_pyc_snapshot` · ⚠️`pyc_dieu_chinh_commit` | Tỷ lệ phiếu bị dời trên tổng số phiếu. | Tạo Big value từ `production_management_1_pyc_snapshot`, metric `pyc_dieu_chinh_commit`, định dạng phần trăm. Đặt tiêu đề "% PYC điều chỉnh Commit". |
| 4 | *(% PYC điều chỉnh nhiều lần)* · Big value | `production_management_1_pyc_snapshot` · ⚠️`pyc_dieu_chinh_nhieu_lan` | Tỷ lệ phiếu bị dời **từ 2 lần trở lên** — nhóm đáng lo nhất. | Tạo Big value từ `production_management_1_pyc_snapshot`, metric `pyc_dieu_chinh_nhieu_lan`, định dạng phần trăm. Đặt tiêu đề "% PYC điều chỉnh nhiều lần". |
| 5 | **XU HƯỚNG ĐIỀU CHỈNH MM COMMIT** · Mixed (bar + line) | `production_management_1_pyc_snapshot` + `_dim_date`<br>Trục X: năm → quý → tháng<br>Cột: ✅`count_lan_dieu_chinh_hien_tai`, ⚠️`mm_commit_hien_tai`, ⚠️`mm_snapshot_commit`<br>Đường (trục phải): ⚠️`pyc_dieu_chinh_commit` | Theo thời gian, số lần điều chỉnh và khối lượng man-month bị dời có đang giảm không. Cột `mm_snapshot_commit` là cam kết **ban đầu**, `mm_commit_hien_tai` là cam kết **hiện tại** — khoảng cách giữa hai cột chính là phần đã trượt. | Tạo Mixed chart từ `production_management_1_pyc_snapshot` join `production_management_1_dim_date`: trục X là `year_value` → `quarter` → `month_name`. Cột trục trái: metric `count_lan_dieu_chinh_hien_tai`, `mm_commit_hien_tai`, `mm_snapshot_commit`. Đường trục phải: metric `pyc_dieu_chinh_commit` định dạng phần trăm. Đặt tiêu đề "XU HƯỚNG ĐIỀU CHỈNH MM COMMIT". |
| 6 | **PHÂN BỔ SỐ LẦN ĐIỀU CHỈNH PYC** · Pie | `production_management_1_pyc_snapshot` · ⚠️`slcs_phat_sinh_1_month` | Cơ cấu phiếu theo số lần bị dời (1 lần, 2 lần, 3 lần…). **Lưu ý**: bản gốc thiếu dimension lát cắt, cần bổ sung dimension "số lần điều chỉnh" khi dựng lại, nếu không pie chỉ có một miếng. | Tạo Pie chart từ `production_management_1_pyc_snapshot` với giá trị là metric `slcs_phat_sinh_1_month`. **Cần bổ sung** dimension lát cắt theo số lần điều chỉnh (ví dụ tạo dimension bậc thang từ `count_lan_dieu_chinh_hien_tai`: 1 lần / 2 lần / từ 3 lần). Đặt tiêu đề "PHÂN BỔ SỐ LẦN ĐIỀU CHỈNH PYC". |
| 7 | **CHI TIẾT ĐIỀU CHỈNH** · Table | `production_management_1_pyc_snapshot`<br>Cột: `pm`, ⚠️`mm_commit_hien_tai`, ⚠️`commit_lan_dau`, ✅`date_commit_hien_tai`, ✅`count_lan_dieu_chinh_hien_tai`, ⚠️`count_date_dich_chuyen_commit`, `issue_key`, `issue_current_status_name`, `commit_history_date`, ⚠️`mm_snapshot_commit`, `history_edit_count`, `project`, `issue_summary` | Danh sách từng phiếu bị dời: ai là PM, cam kết lần đầu ngày nào, hiện tại ngày nào, dời tổng cộng bao nhiêu ngày. Đây là bảng để đi hỏi trách nhiệm. | Tạo Table từ `production_management_1_pyc_snapshot` với các cột: `issue_key`, `issue_summary`, `project`, `pm`, `issue_current_status_name`, `commit_lan_dau`, `date_commit_hien_tai`, `count_lan_dieu_chinh_hien_tai`, `count_date_dich_chuyen_commit`, `mm_snapshot_commit`, `mm_commit_hien_tai`, `commit_history_date`, `history_edit_count`. Đặt tiêu đề "CHI TIẾT ĐIỀU CHỈNH". |

---

## D2 — Nội bộ TTSX&CNTT - NGUỒN LỰC OS

**Mục đích:** Trang OS đầy đủ nhất, bản dùng nội bộ. Khác với D10 (bản chính thức) ở chỗ **lấy số từ `pyc_snapshot`** — tức là tính theo cam kết tại thời điểm chụp, không tính theo giá trị hiện tại. Nhờ vậy con số không bị "đẹp lên" khi ai đó sửa ngày commit về sau.

Bố cục là 8 chart xếp thành lưới, mỗi chart trả lời một câu:
tiến độ có kịp không → man-month có đủ không → chất lượng có ổn không → tiền có về không.

**Bộ lọc trang:** PM, sản phẩm TTSX, dự án, đối tác, năm, tháng, quý, khối, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **PYC OS** · Bar | `production_management_1_pyc_snapshot` + `_dim_date`<br>Trục X: `month_year` / `quarter_year`<br>Giá trị: ⚠️`count_pyc_snap`<br>Nhóm màu: `issue_current_status_name`<br>Lọc: bỏ trạng thái `canceled` | Số phiếu yêu cầu theo tháng, chia màu theo trạng thái. Nhìn thấy ngay tồn đọng: nếu màu "In Progress" ngày càng dày lên nghĩa là phiếu vào nhiều hơn ra. | Tạo Bar chart từ `production_management_1_pyc_snapshot` join `production_management_1_dim_date`: trục X `month_year` (sắp xếp tăng dần), giá trị metric `count_pyc_snap`, nhóm màu theo dimension `issue_current_status_name`, bật Stack. Lọc loại bỏ trạng thái `canceled`. Đặt tiêu đề "PYC OS". |
| 2 | **KẾ HOẠCH VÀ SỬ DỤNG MAN-MONTH** · Mixed (bar + line) | `production_management_1_pyc_jira` + `_os_mm_plan` + `_dim_date`<br>Cột: ⚠️`mm_ttsx_du_kien_date_du_kien`, ⚠️`mm_ttsp_du_kien_date_du_kien`<br>Đường (trục phải): ✅`sum_mm_os_plan` | Ba con số man-month đặt cạnh nhau: **TTSX dự kiến**, **TTSP dự kiến**, và **kế hoạch OS được duyệt**. Lệch nhau nghĩa là hai đầu chưa thống nhất về khối lượng. | Tạo Mixed chart từ `production_management_1_pyc_jira` và `production_management_1_os_mm_plan` join `production_management_1_dim_date`: trục X `quarter_year` → `month_year` (tăng dần). Cột trục trái: metric `mm_ttsx_du_kien_date_du_kien` và `mm_ttsp_du_kien_date_du_kien`. Đường trục phải: metric `sum_mm_os_plan` (nhãn "MM TTr - PLAN"). Đặt tiêu đề "KẾ HOẠCH VÀ SỬ DỤNG MAN-MONTH". |
| 3 | **TIẾN ĐỘ XỬ LÝ PYC ĐÚNG HẠN** · Mixed (bar + line) | `production_management_1_pyc_snapshot` + `_dim_date`<br>Cột: ⚠️`pyc_on_time_snap`, ⚠️`pyc_end_snap`<br>Đường (trục phải): ⚠️`pyc_done_on_time_snap` | Cột là **số phiếu kết thúc** và **số phiếu đúng hạn**, đường là **tỷ lệ đúng hạn**. Phải nhìn cả hai: tháng ít phiếu mà tỷ lệ 100% không nói lên nhiều điều. | Tạo Mixed chart từ `production_management_1_pyc_snapshot` join `production_management_1_dim_date`: trục X `month_year` + `quarter_year` (tăng dần). Cột trục trái: metric `pyc_end_snap` và `pyc_on_time_snap`. Đường trục phải: metric `pyc_done_on_time_snap` định dạng phần trăm 2 chữ số. Đặt tiêu đề "TIẾN ĐỘ XỬ LÝ PYC ĐÚNG HẠN". |
| 4 | **QUẢN LÝ HIỆU SUẤT** · Mixed (bar + line) | `production_management_1_pyc_jira` + `_pyc_snapshot` + `_dim_date`<br>Cột: ⚠️`mm_burned_ttsx_commit_date` (thực đốt), ⚠️`mm_ttsx_commit_commit_date` (cam kết)<br>Đường (trục phải): ⚠️`delta_thuc_passed_commit` | **Đốt bao nhiêu so với cam kết bấy nhiêu.** Đường delta dương nghĩa là làm nhiều hơn cam kết — có thể do phát sinh, có thể do ước lượng thấp. | Tạo Mixed chart từ `production_management_1_pyc_jira` và `production_management_1_pyc_snapshot` join `production_management_1_dim_date`: trục X `quarter_year` → `month_year`. Cột trục trái: metric `mm_burned_ttsx_commit_date` và `mm_ttsx_commit_commit_date`. Đường trục phải: metric `delta_thuc_passed_commit` định dạng phần trăm 1 chữ số. Đặt tiêu đề "QUẢN LÝ HIỆU SUẤT". |
| 5 | **CHẤT LƯỢNG BUG RATE** · Mixed (bar + line) | `production_management_1_pyc_jira` + `_dim_date`<br>Giá trị: ⚠️`bug_rate` | Số bug trên mỗi đơn vị công OS, theo tháng. Đây là thước đo chất lượng hàng đối tác giao. | Tạo Mixed chart từ `production_management_1_pyc_jira` join `production_management_1_dim_date`: trục X `month_year` (tăng dần) + `quarter_year`, giá trị là metric `bug_rate`. Đặt tiêu đề "CHẤT LƯỢNG BUG RATE". |
| 6 | **BUG SAU NGHIỆM THU** · Bar | `production_management_1_post_acceptance_bug` + `_dim_date`<br>Trục X: `month_year`, `year_value`<br>Giá trị: ⚠️`post_acceptance_bug`<br>Nhóm màu: `severity` | Bug phát hiện **sau khi đã nghiệm thu** — nghĩa là lọt qua khâu kiểm tra. Chia màu theo mức nghiêm trọng để biết đây là lỗi vặt hay lỗi nặng. | Tạo Bar chart từ `production_management_1_post_acceptance_bug` join `production_management_1_dim_date`: trục X `month_year` (tăng dần), giá trị metric `post_acceptance_bug`, nhóm màu theo dimension `severity`, bật Stack. Đặt tiêu đề "BUG SAU NGHIỆM THU". |
| 7 | **QUẢN LÝ CR** · Mixed (bar + line) | `production_management_1_pyc_jira` + `_dim_date`<br>Cột: ⚠️`mm_burned_ttsx_acceptance`, ⚠️`mm_commit_ttsx_acceptance`<br>Đường (trục phải): ⚠️`cr` (%CR) | CR = change request. Đường %CR cho biết bao nhiêu phần khối lượng phát sinh so với hợp đồng ban đầu. Cao liên tục nghĩa là khâu làm rõ yêu cầu đầu vào đang yếu. | Tạo Mixed chart từ `production_management_1_pyc_jira` join `production_management_1_dim_date`: trục X `quarter_year` → `month_year`. Cột trục trái: metric `mm_commit_ttsx_acceptance` và `mm_burned_ttsx_acceptance`. Đường trục phải: metric `cr` định dạng phần trăm. Đặt tiêu đề "QUẢN LÝ CR". |
| 8 | **TỈ LỆ RELEASE** · Mixed (bar + line) | `production_management_1_pyc_jira` + `_dim_date`<br>Cột: ⚠️`mm_ttsx_released_commit_date`, ⚠️`mm_burned_ttsx_acceptance`<br>Đường (trục phải): ⚠️`hieu_released` | Trong khối lượng đã đốt, bao nhiêu phần đã thực sự release. Phần chênh là công đã bỏ ra nhưng chưa ra được sản phẩm. | Tạo Mixed chart từ `production_management_1_pyc_jira` join `production_management_1_dim_date`: trục X `quarter_year` + `month_year`. Cột trục trái: metric `mm_burned_ttsx_acceptance` và `mm_ttsx_released_commit_date`. Đường trục phải: metric `hieu_released` định dạng phần trăm. Đặt tiêu đề "TỈ LỆ RELEASE". |
| 9 | **THEO DÕI MM TTSX THEO DỰ KIẾN - COMMIT - ACTUAL - THANH TOÁN** · Mixed (bar + line) | `production_management_1_pyc_jira` + `_os_mm_plan` + `_dim_date`<br>Cột: ⚠️`mm_ttsx_du_kien_date_du_kien`, ⚠️`mm_ttsx_commit_commit_date_last`, ⚠️`mm_burned_ttsx_acceptance`, ⚠️`mm_thanh_toan_ttsx`<br>Đường (trục phải): ✅`sum_mm_os_plan` | **Chart quan trọng nhất trang này.** Bốn cột là bốn mốc của cùng một khối lượng công: dự kiến → cam kết → thực đốt → đã thanh toán. Cột sau luôn phải nhỏ hơn hoặc bằng cột trước; chỗ nào tụt mạnh là chỗ đang rò rỉ. | Tạo Mixed chart từ `production_management_1_pyc_jira` và `production_management_1_os_mm_plan` join `production_management_1_dim_date`: trục X `quarter_year` → `month_year`. Bốn cột trục trái theo thứ tự: `mm_ttsx_du_kien_date_du_kien`, `mm_ttsx_commit_commit_date_last`, `mm_burned_ttsx_acceptance`, `mm_thanh_toan_ttsx`. Đường trục phải: `sum_mm_os_plan`. Đặt tiêu đề "THEO DÕI MM TTSX THEO DỰ KIẾN - COMMIT - ACTUAL - THANH TOÁN". |

---

## D10 — TTSX&CNTT - NGUỒN LỰC OS

**Mục đích:** Bản **chính thức** của trang OS, dùng để báo cáo lên trên. Cùng bố cục với D2 nhưng số liệu lấy từ `pyc_jira` (giá trị hiện tại) thay vì `pyc_snapshot` (giá trị tại thời điểm chụp). Có thêm 2 chart cuối về tỷ lệ release trong tháng.

**Bộ lọc trang:** người phụ trách, sản phẩm TTSX, dự án, đối tác, năm, tháng, quý, khối, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **PYC OS** · Bar | `production_management_1_pyc_jira` + `_dim_date`<br>Giá trị ⚠️`count_pyc` · Nhóm màu `issue_current_status_name`<br>Lọc: bỏ `canceled` | 🔁 Như D2-1 nhưng đếm trên bảng PYC hiện tại. | Tạo Bar chart từ `production_management_1_pyc_jira` join `production_management_1_dim_date`: trục X `month_year` (tăng dần), giá trị metric `count_pyc`, nhóm màu `issue_current_status_name`, bật Stack, loại bỏ trạng thái `canceled`. Đặt tiêu đề "PYC OS". |
| 2 | **KẾ HOẠCH VÀ SỬ DỤNG MAN-MONTH** · Mixed | `_os_mm_plan` + `_pyc_jira` + `_dim_date`<br>Cột: ✅`sum_mm_os_plan`, ⚠️`mm_ttsp_du_kien_commit_date`, ⚠️`mm_ttsx_commit_commit_date_last`<br>Đường (trục phải): ⚠️`delta_ttsx_ttsp` | Khác D2-2 ở chỗ có thêm đường **DELTA (TTSX - TTSP)** — chênh lệch giữa khối lượng hai trung tâm ước tính. Delta lớn nghĩa là hai bên chưa thống nhất phạm vi công việc. | Tạo Mixed chart từ `production_management_1_os_mm_plan` và `production_management_1_pyc_jira` join `production_management_1_dim_date`: trục X `quarter_year` → `month_year`. Cột trục trái: `sum_mm_os_plan`, `mm_ttsx_commit_commit_date_last`, `mm_ttsp_du_kien_commit_date`. Đường trục phải: `delta_ttsx_ttsp` định dạng phần trăm 1 chữ số. Đặt tiêu đề "KẾ HOẠCH VÀ SỬ DỤNG MAN-MONTH". |
| 3 | **TIẾN ĐỘ XỬ LÝ PYC ĐÚNG HẠN** · Mixed | `_pyc_jira` + `_dim_date`<br>Cột: ⚠️`pyc_end`, ⚠️`pyc_on_time`<br>Đường (trục phải): ⚠️`pyc_done_on_time` | 🔁 Như D2-3 nhưng dùng metric bản hiện tại (không có hậu tố `_snap`). | Tạo Mixed chart từ `production_management_1_pyc_jira` join `production_management_1_dim_date`: trục X `month_year` (tăng dần) + `quarter_year`. Cột trục trái: `pyc_end` và `pyc_on_time`. Đường trục phải: `pyc_done_on_time` định dạng phần trăm. Đặt tiêu đề "TIẾN ĐỘ XỬ LÝ PYC ĐÚNG HẠN". |
| 4 | **CHẤT LƯỢNG BUG RATE** · Mixed | `_pyc_jira` + `_dim_date` · ⚠️`bug_rate` | 🔁 Giống D2-5. | 🔁 Dùng lại chart "CHẤT LƯỢNG BUG RATE" của D2. |
| 5 | **BUG SAU NGHIỆM THU** · Bar | `_post_acceptance_bug` + `_dim_date` · ⚠️`post_acceptance_bug` · Nhóm màu `severity` | 🔁 Giống D2-6. | 🔁 Dùng lại chart "BUG SAU NGHIỆM THU" của D2. |
| 6 | **QUẢN LÝ HIỆU SUẤT** · Mixed | `_pyc_jira` + `_dim_date`<br>Cột: ⚠️`mm_burned_ttsx_commit_date`, ⚠️`mm_ttsx_commit_commit_date_last`<br>Đường: ⚠️`delta_thuc_passed_commit` | 🔁 Như D2-4, lấy cam kết bản mới nhất (`_last`) thay vì bản snapshot. | Tạo Mixed chart từ `production_management_1_pyc_jira` join `production_management_1_dim_date`: trục X `quarter_year` → `month_year`. Cột trục trái: `mm_ttsx_commit_commit_date_last` và `mm_burned_ttsx_commit_date`. Đường trục phải: `delta_thuc_passed_commit` định dạng phần trăm. Đặt tiêu đề "QUẢN LÝ HIỆU SUẤT". |
| 7 | **TỈ LỆ RELEASE** · Mixed | `_pyc_jira` + `_dim_date` · như D2-8 | 🔁 Giống D2-8. | 🔁 Dùng lại chart "TỈ LỆ RELEASE" của D2. |
| 8 | **QUẢN LÝ CR** · Mixed | `_pyc_jira` + `_dim_date` · như D2-7 | 🔁 Giống D2-7. | 🔁 Dùng lại chart "QUẢN LÝ CR" của D2. |
| 9 | **THEO DÕI MM TTSX THEO DỰ KIẾN - COMMIT - ACTUAL - THANH TOÁN** · Mixed | `_pyc_jira` + `_dim_date`<br>Cột: ⚠️`mm_ttsx_commit_commit_date_last`, ⚠️`mm_burned_ttsx_acceptance`, ⚠️`mm_thanh_toan_ttsx`<br>Đường (trục phải): ⚠️`mm_ttsx_du_kien_date_du_kien` | 🔁 Như D2-9, khác ở chỗ "MM dự kiến" được đưa lên trục phải thành đường thay vì cột. | Tạo Mixed chart từ `production_management_1_pyc_jira` join `production_management_1_dim_date`: trục X `quarter_year` → `month_year`. Cột trục trái: `mm_ttsx_commit_commit_date_last`, `mm_burned_ttsx_acceptance`, `mm_thanh_toan_ttsx`. Đường trục phải: `mm_ttsx_du_kien_date_du_kien`. Đặt tiêu đề "THEO DÕI MM TTSX THEO DỰ KIẾN - COMMIT - ACTUAL - THANH TOÁN". |
| 10 | **TỈ LỆ MM PYC RELEASE TRONG THÁNG** · Mixed | `_pyc_jira` + `_dim_date`<br>Hàng phụ: `ttsx_product`<br>Cột: ⚠️`mm_pyc_released`, ⚠️`mm_pyc_acceptance_thanh_cong`, ⚠️`pyc_release`<br>Đường (trục phải): ⚠️`mm_pyc_release` (%) | Trong tháng, khối lượng man-month đã release chiếm bao nhiêu phần khối lượng đã nghiệm thu — tách theo từng sản phẩm TTSX. | Tạo Mixed chart từ `production_management_1_pyc_jira` join `production_management_1_dim_date`: trục X `month_year`, nhóm theo `ttsx_product`. Cột trục trái: `mm_pyc_acceptance_thanh_cong`, `mm_pyc_released`, `pyc_release`. Đường trục phải: `mm_pyc_release` định dạng phần trăm. Đặt tiêu đề "TỈ LỆ MM PYC RELEASE TRONG THÁNG". |
| 11 | **TỈ LỆ XỬ LÝ PYC RELEASE TRONG THÁNG** · Mixed | `_pyc_jira` + `_dim_date`<br>Hàng phụ: `ttsx_product`<br>Cột: ⚠️`pyc_release_on_time`, ⚠️`pyc_done_acceptance_thanh_cong`, ⚠️`pyc_release`<br>Đường (trục phải): ⚠️`pyc_release_on_time` (%), ⚠️`pyc_release` (%) | Bản đếm **số phiếu** của chart 10 (chart 10 đếm man-month). Hai đường trục phải cho biết % release và % release đúng hạn. | Tạo Mixed chart từ `production_management_1_pyc_jira` join `production_management_1_dim_date`: trục X `month_year`, nhóm theo `ttsx_product`. Cột trục trái: `pyc_release`, `pyc_release_on_time`, `pyc_done_acceptance_thanh_cong`. Hai đường trục phải: `pyc_release` và `pyc_release_on_time` ở dạng phần trăm. Đặt tiêu đề "TỈ LỆ XỬ LÝ PYC RELEASE TRONG THÁNG". |

---

## D11 — TTSX&CNTT - ĐƠN VỊ OS

**Mục đích:** Nhìn OS từ góc **tiền và đối tác** thay vì góc sản phẩm. Trả lời: tháng này thanh toán bao nhiêu, cho đối tác nào, theo hợp đồng nào, thuộc diện nhân sự nào.

**Bộ lọc trang:** người phụ trách, sản phẩm TTSX, dự án, đối tác, năm, tháng, quý, khối, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **THEO DÕI THANH TOÁN THEO DIỆN NHÂN SỰ** · Bar | `production_management_1_pyc_jira` + `_dim_date`<br>Trục X: năm → quý → tháng<br>Giá trị: ⚠️`mm_thanh_toan_ttsx`<br>Nhóm màu: `type_os` | Man-month thanh toán theo thời gian, chia màu theo **nhóm đối tượng OS** (loại hình nhân sự thuê ngoài). Thấy được cơ cấu chi đang nghiêng về diện nào. | Tạo Bar chart từ `production_management_1_pyc_jira` join `production_management_1_dim_date`: trục X `year_value` → `quarter_year` → `month_year` (tăng dần), giá trị metric `mm_thanh_toan_ttsx`, nhóm màu theo dimension `type_os`, bật Stack. Đặt tiêu đề "THEO DÕI THANH TOÁN THEO DIỆN NHÂN SỰ". |
| 2 | **CHI TIẾT THANH TOÁN THEO ĐƠN VỊ OS** · Table (pivot) | `production_management_1_pyc_jira` + `_dim_date`<br>Hàng: `type_os`, `year_value`, `quarter`, `month_name`<br>Cột: `partner`<br>Giá trị: ⚠️`mm_thanh_toan_ttsx`<br>Lọc: bỏ `canceled` | Ma trận **đối tác × thời gian**. Dùng để đối chiếu khi làm hồ sơ thanh toán cuối kỳ. | Tạo Table pivot từ `production_management_1_pyc_jira` join `production_management_1_dim_date`. Hàng: `type_os`, `year_value`, `quarter`, `month_name`. Cột: `partner`. Giá trị: metric `mm_thanh_toan_ttsx`. Lọc bỏ trạng thái `canceled`. Đặt tiêu đề "CHI TIẾT THANH TOÁN THEO ĐƠN VỊ OS". |
| 3 | **THEO DÕI THANH TOÁN OS THEO HỢP ĐỒNG** · Bar | `production_management_1_pyc_jira` + `_dim_date`<br>Trục X: `partner_by_contract` + năm/quý/tháng<br>Giá trị: ⚠️`mm_thanh_toan_ttsx`<br>Nhóm màu: `type_os` | Như chart 1 nhưng gom theo **đối tác theo hợp đồng** — một đối tác có thể có nhiều hợp đồng, cột này phục vụ theo dõi hạn mức từng hợp đồng. | Tạo Bar chart từ `production_management_1_pyc_jira` join `production_management_1_dim_date`: trục X `partner_by_contract` (tăng dần) kết hợp `year_value`/`quarter`/`month_name`, giá trị metric `mm_thanh_toan_ttsx`, nhóm màu `type_os`, bật Stack. Lọc bỏ `canceled`. Đặt tiêu đề "THEO DÕI THANH TOÁN OS THEO HỢP ĐỒNG". |

---

## D12 — Release -  Chi tiết

**Mục đích:** Một bảng lớn nối **man-month** với **bản release cụ thể** và **bug production phát sinh**. Đây là chỗ trả lời "bản release tháng này ngốn bao nhiêu công, và có gây ra lỗi gì không".

**Bộ lọc trang:** năm, tháng, quý, tên project, khối, mức ưu tiên bug, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **QUẢN TRỊ SẢN XUẤT CÔNG TY - RELEASE VERSION** · Table (pivot) | Hàng: `project_name_last`, `division`<br>Cột: `month_year`, `year_value`<br>Giá trị: ✅`sum_man_month` (MM), ⚠️`mm_ulnl_release`, ✅`mm_worklog_release`, ✅`mm_worklog_release_nang_cap`, ✅`mm_worklog_release_bao_tri`, ✅`count_bug_prd`, ⚠️`production_bug_rate`, ⚠️`mm_fix_bug_release` (%), ⚠️`mm_release_tinh_nang` (%) | Bảng gộp ba câu chuyện: **công bỏ ra** (MM, worklog), **công dùng để làm gì** (nâng cấp / bảo trì / fix bug / tính năng mới), và **hậu quả** (số bug production, tỷ lệ bug). Hai cột % cuối là chỉ số đáng chú ý nhất: nếu %mm fix bug release cao thì đội đang dành phần lớn thời gian dọn nợ kỹ thuật thay vì làm tính năng. | Tạo Table pivot trên Lightdash gộp `production_management_1_project_name`, `_mamday_hr`, `_fix_versions`, `_mm_release_versions`, `_bug_production`, `_mmrelease` join `production_management_1_dim_date`. Hàng: `division` (sắp xếp tăng dần), `project_name_last`. Cột: `year_value`, `month_year`. Cột giá trị: `sum_man_month`, `mm_ulnl_release`, `mm_worklog_release`, `mm_worklog_release_nang_cap`, `mm_worklog_release_bao_tri`, `count_bug_prd`, `production_bug_rate`, `mm_fix_bug_release` (định dạng %), `mm_release_tinh_nang` (định dạng %). Đặt tiêu đề "QUẢN TRỊ SẢN XUẤT CÔNG TY - RELEASE VERSION". |

---

## Mảng B — Trước khi đọc, hiểu cấu trúc dữ liệu KPIKQI

Toàn bộ mảng B xoay quanh **một bảng duy nhất**: `production_management_1_kpikqi_new`. Mỗi dòng của bảng là một **kết quả đo**, gồm:

| Cụm cột | Ý nghĩa |
|---|---|
| `customer_2`, `customer_jira`, `customer_group`, `customer_type` | Khách hàng nào, thuộc nhóm nào (VVIP / VIP / Viettel / SOCP / Khách hàng ngoài) |
| `spdv`, `spdv_fixed` | Sản phẩm dịch vụ nào (EDR, NDR, NSM, SIEM, SOAR, KIAN, SOC Platform, Cloudrity, Threat Intelligence…) |
| `metric_name` | Tên chỉ tiêu, ví dụ "Tỷ lệ agent online có log" |
| `target_fin`, `target_td`, `condition`, `target_condition` | Ngưỡng phải đạt và điều kiện so sánh (≥ hay ≤) |
| `result`, `result_fact`, `result_prev_1_period`, `result_prev_2_periods` | Kết quả kỳ này và 2 kỳ trước |
| `evaluation`, `evaluation_td` | Kết luận Đạt / Không đạt |
| `kpi_trend`, `so_with_period_n_1_td` | Xu hướng so với kỳ trước |
| `criteria_period_type`, `report_date_2`, `period_result` | Chu kỳ đo (Week / Month / Quarter) và ngày báo cáo |

Ba metric đếm được dùng đi dùng lại khắp mảng B:

| Metric | Đếm cái gì |
|---|---|
| ✅`tong_count_dv_kh` | Số cặp **dịch vụ × khách hàng** — dùng khi muốn đếm "bao nhiêu khách đang dùng dịch vụ này và kết quả ra sao" |
| ✅`tong_count_sp_fixed` | Số cặp **sản phẩm × chỉ tiêu** thuộc loại KPI "Sản phẩm" |
| ⚠️`tong_customer` / ⚠️`tong_khach_passed` / ⚠️`tong_khach_khong_passed` | Số khách hàng, số khách đạt **toàn bộ** chỉ tiêu, số khách trượt ít nhất một chỉ tiêu |

Nắm ba metric này là hiểu được 90% chart trong mảng B.

---

## D5 — KPIKQI Tập đoàn

**Mục đích:** Bộ chỉ tiêu báo cáo lên **Tập đoàn** (khác với chỉ tiêu nội bộ). Các cột có hậu tố `_td` đều là phiên bản dùng cho báo cáo Tập đoàn, target và cách đánh giá có thể khác bản nội bộ.

**Bộ lọc trang:** chu kỳ báo cáo, tuần, tháng, quý, SPDV, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **KPIKQI** · Mixed (bar + line) | `production_management_1_kpikqi_new` + `_target_kpikqi_tap_doan`<br>Trục X: `customer_group`<br>Giá trị: ✅`tong_count_spdv_td`<br>Nhóm màu: `evaluation_td`<br>Lọc: `tag_td = 'Tập đoàn'` | Theo từng nhóm khách hàng, bao nhiêu chỉ tiêu Tập đoàn đạt / không đạt. Cột chồng nên nhìn được cả quy mô lẫn tỷ lệ. | Tạo Mixed chart (dạng cột chồng) từ `production_management_1_kpikqi_new` join `production_management_1_target_kpikqi_tap_doan`: trục X `customer_group`, giá trị metric `tong_count_spdv_td` (sắp xếp giảm dần), nhóm màu theo dimension `evaluation_td`, bật Stack. Lọc `tag_td = 'Tập đoàn'`. Đặt tiêu đề "KPIKQI". |
| 2 | **CHI TIẾT KẾT QUẢ** · Table | `production_management_1_kpikqi_new`<br>Cột: `spdv_fixed`, `metric_name`, `unit_of_measure_td`, `target_td`, `condition_td`, `result` (kỳ N), `result_prev_1_period` (kỳ N-1), `so_with_period_n_1_td`, `vs_target_td`, `kpi_trend_td`, `evaluation_td`, `report_date_2`, `criteria_period_type`<br>Định dạng màu: ⚠️`mau_sac_nen_so_voi_ky_n_1`, ⚠️`mau_sac_font_so_voi_ky_n_1` | Bảng chi tiết từng chỉ tiêu Tập đoàn: target bao nhiêu, kỳ này ra bao nhiêu, so với kỳ trước tăng hay giảm bao nhiêu %. Hai metric "màu sắc" chỉ để tô nền/chữ, không phải số liệu — trên Lightdash dùng **conditional formatting** thay vì tạo cột. | Tạo Table từ `production_management_1_kpikqi_new` với các cột: `report_date_2` (định dạng MM/dd/yyyy), `criteria_period_type`, `spdv_fixed`, `metric_name`, `unit_of_measure_td`, `target_td`, `condition_td` (sắp xếp tăng dần), `result` (2 chữ số thập phân), `result_prev_1_period`, `so_with_period_n_1_td` (định dạng %), `vs_target_td`, `kpi_trend_td`, `evaluation_td`. Áp dụng conditional formatting cho cột `so_with_period_n_1_td` theo logic của metric `mau_sac_nen_so_voi_ky_n_1`. Đặt tiêu đề "CHI TIẾT KẾT QUẢ". |

---

## D6 — KPIKQI Tập đoàn *(bảng thô)*

**Mục đích:** Bảng dữ liệu gốc phía sau D5, không format gì. Dùng để đối soát hoặc xuất Excel. Khi dựng lại trên Lightdash **có thể bỏ** — người dùng chỉ cần nhấn *Export* từ chart D5-2.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(bảng thô KPI Tập đoàn)* · Table | `production_management_1_kpikqi_new`<br>Cột: `spdv`, `metric_name`, `result`, `report_date_2`, `result_prev_1_period`, `result_prev_2_periods`, `tag_td`, `target_td`, `condition_td`, `evaluation_td`, `criteria_period_type`, `customer_group`, `unit_of_measure_td`<br>Lọc: `tag_td = 'Tập đoàn'` | Toàn bộ trường dữ liệu KPI Tập đoàn, không lược bớt. | Tạo Table từ `production_management_1_kpikqi_new` với các cột `report_date_2`, `criteria_period_type`, `customer_group`, `spdv`, `metric_name`, `unit_of_measure_td`, `target_td`, `condition_td`, `result`, `result_prev_1_period`, `result_prev_2_periods`, `evaluation_td`, `tag_td`. Lọc `tag_td = 'Tập đoàn'`. Đặt tiêu đề "Dữ liệu KPI Tập đoàn". |

---

## D7 — 1. SOCP-VIETTEL

**Mục đích:** Kết quả KPI cho hai nhóm khách nội bộ: **SOC Platform** và **Viettel**. Trang này có cấu trúc chuẩn mà các trang nhóm khách khác (D8 VVIP, D35 VIP, D41) đều lặp lại:

> tiêu đề kỳ báo cáo → card tổng hợp Đạt/Fail → bảng cảnh báo fail 2 kỳ liên tiếp → chart phân bố theo sản phẩm → bảng chi tiết.

**Bộ lọc trang:** năm, khách hàng, chu kỳ báo cáo, tuần, tháng, quý, SPDV.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(tiêu đề kỳ báo cáo)* · Big value | `production_management_1_kpikqi_new` · ⚠️`report_week_new` | Không phải KPI — chỉ là dòng chữ "Báo cáo tuần XX" để người xem biết đang nhìn kỳ nào. Trên Lightdash nên làm bằng **markdown tile** thay vì Big value. | Trên Lightdash, tạo một **Markdown tile** trên dashboard hiển thị kỳ báo cáo hiện tại, thay vì dùng chart. Nếu bắt buộc dùng chart thì tạo Big value từ `production_management_1_kpikqi_new` với metric `report_week_new`. |
| 2 | *(tổng hợp sản phẩm đạt / fail)* · Big value ×3 | `production_management_1_kpikqi_new`<br>⚠️`tong_count_sp_passed`, ⚠️`tong_count_sp_khong_passed`, ✅`tong_sp` | Ba số: tổng sản phẩm được đo, số đạt, số fail. Trong Power BI nằm chung một card; trên Lightdash phải tách thành 3 Big value hoặc 1 bảng 3 dòng. | Tạo 3 Big value riêng từ `production_management_1_kpikqi_new`: (1) metric `tong_sp` nhãn "TỔNG SẢN PHẨM", (2) metric `tong_count_sp_passed` nhãn "TỔNG SP ĐẠT", (3) metric `tong_count_sp_khong_passed` nhãn "TỔNG SP FAIL". Xếp cạnh nhau trên dashboard. |
| 3 | **CẢNH BÁO FAIL KPI 2 KỲ** · Table (pivot) | `production_management_1_kpikqi_new`<br>Hàng: `customer_group`, `spdv`, `metric_name`<br>Giá trị: ✅`max_condition`, ⚠️`result_fact`, ⚠️`target_dim`, ✅`max_kpi_trend`, ✅`max_alert_icon`, ⚠️`result_prev_1_period_fact`, ⚠️`result_prev_2_periods_fact`<br>Lọc: nhóm KH ∈ (SOCP, Viettel), cờ fail 2 kỳ | **Bảng quan trọng nhất trang này.** Chỉ liệt kê những chỉ tiêu đã trượt **hai kỳ liên tiếp** — tức là không phải sự cố nhất thời mà là vấn đề có hệ thống. Ba cột kết quả (kỳ này, N-1, N-2) đặt cạnh nhau để thấy rõ đang xấu dần hay đã chạm đáy. | Tạo Table pivot từ `production_management_1_kpikqi_new`. Hàng: `customer_group`, `spdv`, `metric_name`. Cột giá trị: `max_condition`, `target_dim`, `result_fact`, `result_prev_1_period_fact`, `result_prev_2_periods_fact`, `max_kpi_trend`, `max_alert_icon`. Lọc `customer_group IN ('SOCP','Viettel')` và chỉ giữ các dòng có cờ fail 2 kỳ liên tiếp (`flag_2ky_fail`). Đặt tiêu đề "CẢNH BÁO FAIL KPI 2 KỲ". |
| 4 | **KPIKQI SẢN PHẨM** · Mixed (bar chồng) | `production_management_1_kpikqi_new`<br>Trục X: `customer_group` + `spdv`<br>Giá trị: ✅`tong_count_sp_fixed`<br>Nhóm màu: `evaluation`<br>Lọc: nhóm KH ∈ (SOCP, Viettel), loại KPI = Sản phẩm | Mỗi cột là một sản phẩm trong một nhóm khách, chia màu Đạt/Không đạt. Nhìn ra ngay sản phẩm nào đang kéo tụt nhóm. | Tạo Mixed chart dạng cột chồng từ `production_management_1_kpikqi_new`: trục X `customer_group` + `spdv`, giá trị metric `tong_count_sp_fixed`, nhóm màu theo `evaluation`, bật Stack. Lọc `customer_group IN ('SOCP','Viettel')` và loại KPI = 'Sản phẩm'. Đặt tiêu đề "KPIKQI SẢN PHẨM". |
| 5 | **CHI TIẾT KẾT QUẢ THEO SẢN PHẨM** · Table | `production_management_1_kpikqi_new`<br>Cột: `report_date_2`, `spdv`, `metric_name`, `result`, `evaluation`, `target_fin`, `condition`, `result_prev_1_period`, `result_prev_2_periods`, `customer_group`, `report_month_date` | Bảng chi tiết đầy đủ, không lọc theo cờ cảnh báo. Dùng khi cần tra một chỉ tiêu cụ thể. | Tạo Table từ `production_management_1_kpikqi_new` với các cột `report_date_2`, `report_month_date`, `customer_group`, `spdv`, `metric_name`, `target_fin`, `condition`, `result`, `result_prev_1_period`, `result_prev_2_periods`, `evaluation`. Đặt tiêu đề "CHI TIẾT KẾT QUẢ THEO SẢN PHẨM". |

---

## D8 — 2. VVIP

**Mục đích:** Trang KPI cho nhóm khách **VVIP** — nhóm được ưu tiên cao nhất. Bố cục: một hàng chart cột cho **từng sản phẩm bảo mật**, cộng thêm card tổng hợp và hai bảng theo dõi số khách đạt qua các tuần.

Bảy chart từ 1 đến 9 (trừ 2) có **cấu trúc hoàn toàn giống nhau**, chỉ khác bộ lọc `spdv`. Vì vậy phần prompt dưới đây viết chung một lần, còn bảng liệt kê từng sản phẩm.

**Bộ lọc trang:** khách hàng, năm, chu kỳ báo cáo, kỳ kết quả, đánh giá SOC, chỉ tiêu SOC, SPDV.

### Nhóm chart "một sản phẩm một cột" (chart 1, 3, 4, 5, 6, 7, 8, 9)

**Khuôn chung:** Bar chart từ `production_management_1_kpikqi_new`, trục X là `customer_type` + `metric_name`, giá trị là ✅`tong_count_dv_kh`, nhóm màu là `evaluation`, lọc theo đúng một `spdv`.

**Ý nghĩa chung:** Với sản phẩm đó, mỗi chỉ tiêu có bao nhiêu cặp *dịch vụ × khách hàng* đạt và bao nhiêu không đạt, tách theo loại khách (VVIP / VIP). Cột nào đỏ nhiều là chỉ tiêu đang có vấn đề trên diện rộng.

| # | Chart | Bộ lọc riêng | Ghi chú |
|---|---|---|---|
| 1 | **CLOUDRITY** · Bar | `spdv = 'Cloudrity'` | |
| 3 | **KIAN** · Bar | `spdv = 'KIAN'` | |
| 4 | **SOAR** · Bar | `spdv = 'SOAR'`, `metric_name = 'Tỷ lệ playbook thực hiện thành công'` | Chỉ theo dõi đúng một chỉ tiêu |
| 5 | **NSM** · Bar | `spdv = 'NSM'` | |
| 6 | **SIEM** · Bar | `spdv = 'SIEM'`, `metric_name` ∈ (Tỷ lệ agent online, Tỷ lệ agent online có log) | |
| 7 | **NDR** · Bar | `spdv = 'NDR'` | |
| 8 | **EDR** · Bar | `spdv = 'EDR'`, `customer_type = 'VVIP'`, `metric_name` ∈ (Độ khả dụng của Portal quản trị, Tỷ lệ giao dịch thành công của toàn bộ hệ thống) | |
| 9 | **SOC Platform** · Bar | `spdv = 'SOC Platform'` | |

**Prompt chung (thay `<SPDV>` bằng tên sản phẩm và bổ sung bộ lọc riêng ở bảng trên):**

> Tạo Bar chart trên Lightdash từ model `production_management_1_kpikqi_new`: trục X là dimension `customer_type` kết hợp `metric_name` (cùng sắp xếp tăng dần), giá trị là metric `tong_count_dv_kh`, nhóm màu theo dimension `evaluation`, bật Stack. Lọc `spdv = '<SPDV>'`. Đặt tiêu đề "<SPDV>".

### Các chart còn lại

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 2 | **CHỈ SỐ THEO KHÁCH HÀNG** · Mixed (bar chồng) | `production_management_1_kpikqi_new`<br>Trục X: `spdv` + `customer_2` + `period_result`<br>Giá trị: ✅`tong_count_dv_kh`<br>Nhóm màu: `evaluation`<br>Lọc: nhóm KH = Khách hàng ngoài, `spdv` ∈ 7 sản phẩm bảo mật, `tick_vvip_2026 = 'VVIP'` | Đảo góc nhìn: thay vì "một sản phẩm nhìn nhiều khách", đây là "một khách nhìn nhiều sản phẩm". Dùng khi chuẩn bị họp với một khách hàng cụ thể. | Tạo Mixed chart dạng cột chồng từ `production_management_1_kpikqi_new`: trục X `spdv` → `customer_2` → `period_result` (đều tăng dần), giá trị metric `tong_count_dv_kh`, nhóm màu `evaluation`, bật Stack. Lọc `customer_group = 'KHÁCH HÀNG NGOÀI'`, `spdv IN ('EDR','NDR','SOAR','NSM','SIEM','SOC Platform','KIAN')`, và cờ VVIP. Đặt tiêu đề "CHỈ SỐ THEO KHÁCH HÀNG". |
| 10 | *(tiêu đề kỳ báo cáo)* · Big value | ⚠️`report_week_new` | 🔁 Như D7-1, nên thay bằng markdown tile. | 🔁 Dùng markdown tile hiển thị kỳ báo cáo. |
| 11 | **KHÁCH HÀNG** · Big value ×3 | ⚠️`tong_customer`, ⚠️`tong_khach_passed`, ⚠️`tong_khach_khong_passed`<br>Lọc: kỳ báo cáo mới nhất | Tổng số khách VVIP, số khách đạt **toàn bộ** chỉ tiêu, số khách trượt ít nhất một chỉ tiêu. Đây là con số báo cáo lên lãnh đạo. | Tạo 3 Big value từ `production_management_1_kpikqi_new`, lọc kỳ báo cáo mới nhất: (1) metric `tong_customer` nhãn "Tổng khách hàng", (2) `tong_khach_passed` nhãn "Tổng khách Đạt", (3) `tong_khach_khong_passed` nhãn "Tổng khách Không Đạt". Nhóm lại thành một cụm tiêu đề "KHÁCH HÀNG". |
| 12 | **SỐ LƯỢNG KHÁCH HÀNG ĐẠT TOÀN BỘ KPI THEO TUẦN** · Table (pivot) | Hàng: `spdv`<br>Giá trị: ⚠️`tong_customer`, ⚠️`tong_khach_passed`, ⚠️`tong_khach_passed_period_n_1`, ⚠️`tong_khach_passed_period_n_2`<br>Lọc: nhóm KH = Khách hàng ngoài | Ba tuần liền kề đặt cạnh nhau cho từng sản phẩm. Đây là bảng để trả lời "tuần này có tốt hơn tuần trước không" mà không cần vẽ biểu đồ. | Tạo Table pivot từ `production_management_1_kpikqi_new`: hàng `spdv` (sắp xếp tăng dần), cột giá trị `tong_customer` (nhãn "Tổng KH"), `tong_khach_passed` ("Tổng KH Đạt Tuần N"), `tong_khach_passed_period_n_1` ("Tuần N-1"), `tong_khach_passed_period_n_2` ("Tuần N-2"). Lọc `customer_group = 'KHÁCH HÀNG NGOÀI'`. Đặt tiêu đề "SỐ LƯỢNG KHÁCH HÀNG ĐẠT TOÀN BỘ KPI THEO TUẦN". |

---

## D9 — KPIKQI-SOC

**Mục đích:** KPI riêng cho **dịch vụ giám sát an ninh (SOC)** — loại dịch vụ vận hành, không phải sản phẩm phần mềm. Vì thế dùng bảng riêng `production_management_1_soc_results` với các chỉ tiêu đặc thù ngành: MTTD, MTTR, cảnh báo bỏ lọt, tỷ lệ xử lý đúng hạn.

**Bộ lọc trang:** khách hàng, năm, chu kỳ, kỳ báo cáo SOC, chỉ tiêu SOC, đánh giá SOC.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **CHỈ SỐ THEO KHÁCH HÀNG** · Mixed (bar chồng) | `production_management_1_soc_results`<br>Trục X: `metric_name` · Giá trị: ✅`count_metric_kh` · Nhóm màu: `evaluation` | Với mỗi chỉ tiêu SOC, bao nhiêu cặp *chỉ tiêu × khách hàng* đạt và không đạt. Chỉ tiêu nào nhiều đỏ nhất là điểm yếu chung của dịch vụ. | Tạo Mixed chart dạng cột chồng từ `production_management_1_soc_results`: trục X `metric_name`, giá trị metric `count_metric_kh`, nhóm màu `evaluation`, bật Stack. Đặt tiêu đề "CHỈ SỐ THEO KHÁCH HÀNG". |
| 2 | **MTTD** · Bar | `_soc_results`<br>Trục X `customer` · Giá trị ✅`sum_result` · Nhóm màu `evaluation`<br>Lọc: `metric_name = 'MTTD'` | **MTTD** (Mean Time To Detect) — trung bình bao lâu phát hiện được một cuộc tấn công. Cột càng thấp càng tốt. Màu cho biết khách nào đang vượt ngưỡng cam kết. | Tạo Bar chart từ `production_management_1_soc_results`: trục X `customer`, giá trị metric `sum_result`, nhóm màu `evaluation`. Lọc `metric_name = 'MTTD'`. Đặt tiêu đề "MTTD". |
| 3 | **Số lượng cảnh báo Nghiêm trọng bỏ lọt** · Bar | Như chart 2, lọc `metric_name = 'Số lượng cảnh báo Nghiêm trọng bỏ lọt'` | Số cảnh báo mức **nghiêm trọng** mà hệ thống **không phát hiện được**. Đây là chỉ số đau nhất của dịch vụ SOC — bất kỳ giá trị nào lớn hơn 0 đều cần giải trình. | Tạo Bar chart từ `production_management_1_soc_results`: trục X `customer`, giá trị `sum_result`, nhóm màu `evaluation`. Lọc `metric_name = 'Số lượng cảnh báo Nghiêm trọng bỏ lọt'`. Đặt tiêu đề "Số lượng cảnh báo Nghiêm trọng bỏ lọt". |
| 4 | **Số lượng cảnh báo Trung bình bỏ lọt** · Bar | Như chart 3, mức **trung bình** | Phiên bản nhẹ hơn của chart 3. | Tương tự chart 3, đổi bộ lọc thành `metric_name = 'Số lượng cảnh báo Trung bình bỏ lọt'`. Đặt tiêu đề "Số lượng cảnh báo Trung bình bỏ lọt". |
| 5 | **Tỉ lệ báo cáo ATTT hoàn thành đúng hạn** · Bar | Như chart 2, lọc `metric_name = 'Tỉ lệ báo cáo ATTT hoàn thành đúng hạn'` | Cam kết về báo cáo định kỳ gửi khách. Không liên quan kỹ thuật nhưng ảnh hưởng trực tiếp đến đánh giá dịch vụ. | Tương tự chart 2, đổi bộ lọc thành `metric_name = 'Tỉ lệ báo cáo ATTT hoàn thành đúng hạn'`. |
| 6 | **Tỷ lệ case tier 3 xử lý đúng hạn** · Bar | Như chart 2, lọc `metric_name = 'Tỷ lệ case tier 3 xử lý đúng hạn'` | Tier 3 là các case khó nhất, phải chuyển lên chuyên gia. Tỷ lệ này thấp nghĩa là đang thiếu năng lực xử lý sâu. | Tương tự chart 2, đổi bộ lọc thành `metric_name = 'Tỷ lệ case tier 3 xử lý đúng hạn'`. |
| 7 | **Tỷ lệ alert xử lý đúng hạn** · Bar | Như chart 2, lọc `metric_name = 'Tỷ lệ alert xử lý đúng hạn'` | Cảnh báo thông thường có được xử lý trong SLA không. | Tương tự chart 2, đổi bộ lọc thành `metric_name = 'Tỷ lệ alert xử lý đúng hạn'`. |
| 8 | **MTTR** · Bar | Như chart 2, lọc `metric_name = 'MTTR'` | **MTTR** (Mean Time To Respond) — trung bình bao lâu xử lý xong sau khi phát hiện. Đi cặp với MTTD ở chart 2: phát hiện nhanh mà xử lý chậm thì vẫn thiệt hại. | Tương tự chart 2, đổi bộ lọc thành `metric_name = 'MTTR'`. Đặt tiêu đề "MTTR". |
| 9 | **KHÁCH HÀNG** · Big value ×3 | `_soc_results`<br>✅`tong_customer_soc`, ⚠️`tong_khach_passed_soc`, ⚠️`tong_khach_khong_passed_soc`<br>Lọc: kỳ mới nhất | Tổng khách dùng dịch vụ SOC, số đạt toàn bộ chỉ tiêu, số không đạt. | Tạo 3 Big value từ `production_management_1_soc_results`, lọc kỳ báo cáo mới nhất: `tong_customer_soc` ("Tổng khách hàng"), `tong_khach_passed_soc` ("Đạt"), `tong_khach_khong_passed_soc` ("Không Đạt"). Cụm tiêu đề "KHÁCH HÀNG". |
| 10 | **SỐ LƯỢNG KHÁCH HÀNG ĐẠT TOÀN BỘ KPI THEO TUẦN** · Table (pivot) | Hàng: `metric_name`<br>Giá trị: ✅`tong_customer_soc`, ⚠️`tong_khach_passed_soc`, ⚠️`tong_khach_khong_passed_soc`, ⚠️`can_chu_y_soc` | Với mỗi chỉ tiêu SOC: bao nhiêu khách đạt, bao nhiêu không, và bao nhiêu **cần chú ý** (sát ngưỡng). Cột "cần chú ý" là cảnh báo sớm. | Tạo Table pivot từ `production_management_1_soc_results`: hàng `metric_name`, cột giá trị `tong_customer_soc` ("Tổng KH"), `tong_khach_passed_soc` ("Tổng KH Đạt"), `tong_khach_khong_passed_soc` ("Không Đạt"), `can_chu_y_soc` ("Cần chú ý"). Đặt tiêu đề "SỐ LƯỢNG KHÁCH HÀNG ĐẠT TOÀN BỘ KPI THEO TUẦN". |

---

## D13 — 5. BÁO CÁO ĐỊNH KỲ

**Mục đích:** Không phải chart số liệu mà là **sổ ghi vấn đề**. Mỗi dòng là một sự việc đã xảy ra trong kỳ, kèm nguyên nhân, ảnh hưởng, giải pháp và bộ phận chịu trách nhiệm. Đây là phần định tính đi kèm các con số ở D7–D9.

**Bộ lọc trang:** năm, chu kỳ báo cáo, kỳ kết quả, SPDV, mức ưu tiên vấn đề.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(tiêu đề kỳ báo cáo)* · Big value | ⚠️`report_week_new` | 🔁 Như D7-1. | 🔁 Dùng markdown tile hiển thị kỳ báo cáo. |
| 2 | **BÁO CÁO ĐỊNH KỲ** · Table | `production_management_1_issue_ttqt`<br>Cột: `date_value`, `spdv`, `issue_summary` (Vấn đề), `issue_description` (Mô tả), `impact` (Ảnh hưởng), `cause` (Nguyên nhân), `solution_action` (Giải pháp), `owner_department` (BP phụ trách), `classification` (Phân loại) | Bảng theo dõi vấn đề — đọc từ trái sang phải là một câu chuyện hoàn chỉnh: xảy ra chuyện gì, ảnh hưởng thế nào, vì sao, khắc phục ra sao, ai chịu trách nhiệm. | Tạo Table từ `production_management_1_issue_ttqt` với các cột theo thứ tự: `date_value` (định dạng dd/MM/yyyy), `spdv`, `classification`, `issue_summary`, `issue_description`, `impact`, `cause`, `solution_action` (sắp xếp tăng dần), `owner_department`. Đặt tiêu đề "BÁO CÁO ĐỊNH KỲ". |

---

## D19 — Xu hướng KPIKQI

**Mục đích:** Một bảng ngắn nhưng rất hữu ích: **khách nào đang tốt lên, khách nào đang xấu đi**, theo từng sản phẩm. Đây là góc nhìn động, khác với các trang khác chỉ chụp một thời điểm.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(xu hướng KPI theo SPDV)* · Table | `production_management_1_kpikqi_new`<br>Cột: `report_date_2`, `spdv`, ⚠️`quantity_kh_suy_giam`, ⚠️`quantity_kh_cai_thien`, ⚠️`danh_sach_kh_suy_giam`, ⚠️`danh_sach_kh_cai_thien`<br>Lọc: xu hướng ∈ (Cải thiện, Suy giảm), nhóm KH = Khách hàng ngoài | Với mỗi sản phẩm: đếm và **liệt kê tên** khách hàng đang cải thiện / suy giảm. Hai cột danh sách là chuỗi ghép tên — trên Lightdash nên dùng `STRING_AGG` trong SQL của model. | Tạo Table từ `production_management_1_kpikqi_new` với các cột `report_date_2`, `spdv`, `quantity_kh_cai_thien`, `danh_sach_kh_cai_thien`, `quantity_kh_suy_giam`, `danh_sach_kh_suy_giam`. Lọc `kpi_trend IN ('Cải thiện','Suy giảm')` và `customer_group = 'KHÁCH HÀNG NGOÀI'`. Hai metric danh sách cần viết bằng `STRING_AGG(customer_2, ', ')` trong model dbt. Đặt tiêu đề "Xu hướng KPIKQI theo sản phẩm". |

---

## D20 — Tooltip Nguyên nhân lỗi

**Mục đích:** Tooltip page — bung ra khi rê chuột vào một chỉ tiêu bị fail, cho biết **lỗi đó do đâu**. Lightdash không có tooltip page nên làm thành saved chart để drill-through.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(nguyên nhân lỗi)* · Table | `production_management_1_cause` + `_cause_project`<br>Cột: `cause_checkbox` (Nguyên nhân), `customer`, `issue_summary` (tên chỉ tiêu), `date_value` (năm/quý/tháng/ngày) | Với một chỉ tiêu bị fail của một khách cụ thể: nguyên nhân đã được ghi nhận là gì, vào ngày nào. | Tạo Table từ `production_management_1_cause` join `production_management_1_cause_project` với các cột `date_value`, `customer`, `issue_summary` (nhãn "Metric Name"), `cause_checkbox` (nhãn "Nguyên nhân"). Dùng làm chart chi tiết drill-through từ các chart KPIKQI. Đặt tiêu đề "Nguyên nhân lỗi". |

---

## D35 — 3. VIP

**Mục đích:** Bản dành cho nhóm khách **VIP** của trang D8 (VVIP). Cấu trúc y hệt, chỉ khác nhóm khách hàng và có thêm một bảng cảnh báo fail 2 kỳ chi tiết đến từng khách.

**Bộ lọc trang:** năm, khách hàng, chu kỳ, kỳ kết quả, đánh giá SOC, chỉ tiêu SOC, SPDV.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(tiêu đề kỳ báo cáo)* · Big value | ⚠️`report_week_new` | 🔁 Như D7-1. | 🔁 Dùng markdown tile. |
| 2 | **KHÁCH HÀNG** · Big value ×3 | ⚠️`tong_customer`, ⚠️`tong_khach_passed`, ⚠️`tong_khach_khong_passed` | 🔁 Như D8-11 nhưng cho nhóm VIP. | 🔁 Dùng lại cụm 3 Big value của D8-11, đổi bộ lọc `customer_type = 'VIP'`. |
| 3 | **CẢNH BÁO FAIL KPI 2 KỲ** · Table (pivot) | `_kpikqi_new`<br>Hàng: `spdv`, `customer_jira`<br>Giá trị: ✅`max_metric_name`, ✅`max_condition`, ⚠️`target_dim`, ⚠️`result_fact`, ⚠️`result_prev_1_period_fact`, ⚠️`result_prev_2_periods_fact`, ✅`max_kpi_trend`, ✅`max_alert_icon`<br>Lọc: `spdv` ∈ (NDR, NSM, SIEM, SOAR, EDR), cờ fail 2 kỳ | Khác D7-3 ở chỗ **hàng là khách hàng cụ thể** chứ không phải nhóm khách. Dùng để biết chính xác phải gọi cho ai. | Tạo Table pivot từ `production_management_1_kpikqi_new`: hàng `spdv` và `customer_jira`, cột giá trị `max_metric_name`, `max_condition`, `target_dim`, `result_fact`, `result_prev_1_period_fact`, `result_prev_2_periods_fact`, `max_kpi_trend`, `max_alert_icon`. Lọc `spdv IN ('NDR','NSM','SIEM','SOAR','EDR')` và cờ fail 2 kỳ liên tiếp. Đặt tiêu đề "CẢNH BÁO FAIL KPI 2 KỲ". |
| 4 | **CHỈ SỐ THEO SẢN PHẨM** · Mixed (bar chồng) | `_kpikqi_new`<br>Trục X: `spdv` + `customer_type`<br>Giá trị: ✅`tong_count_dv_kh` · Nhóm màu: `evaluation`<br>Tooltip: ⚠️`tong_khach_passed`<br>Lọc: nhóm KH = Khách hàng ngoài, `spdv` ∈ 5 sản phẩm | Phân bố Đạt/Không đạt theo sản phẩm, tách theo loại khách. | Tạo Mixed chart cột chồng từ `production_management_1_kpikqi_new`: trục X `spdv` + `customer_type`, giá trị `tong_count_dv_kh`, nhóm màu `evaluation`, bật Stack, thêm `tong_khach_passed` vào tooltip. Lọc `customer_group = 'KHÁCH HÀNG NGOÀI'`, `spdv IN ('EDR','NDR','NSM','SIEM','SOAR')`. Đặt tiêu đề "CHỈ SỐ THEO SẢN PHẨM". |
| 5–10 | **SOC Platform / NSM / SOAR / EDR / NDR / SIEM** · Bar ×6 | 🔁 Cùng khuôn với nhóm chart "một sản phẩm một cột" của D8 | Với mỗi sản phẩm, phân bố Đạt/Không đạt theo chỉ tiêu và loại khách. | 🔁 Dùng **prompt chung** ở mục D8, lần lượt thay `<SPDV>` bằng `SOC Platform`, `NSM`, `SOAR`, `EDR`, `NDR`, `SIEM`. |

---

## D41 — KPIKQI

**Mục đích:** Bản rút gọn của D7, khác ở bộ lọc sản phẩm (tập trung vào các dịch vụ Audit / Pentest / SOC theo vùng). Cấu trúc 5 chart giống hệt D7.

**Bộ lọc trang:** năm, khách hàng, chu kỳ, tuần, tháng, quý, SPDV.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(tiêu đề kỳ báo cáo)* · Big value | ⚠️`report_week_new` | 🔁 Như D7-1. | 🔁 Dùng markdown tile. |
| 2 | *(tổng hợp sản phẩm đạt / fail)* · Big value ×3 | ⚠️`tong_count_sp_passed`, ⚠️`tong_count_sp_khong_passed`, ✅`tong_sp` | 🔁 Như D7-2. | 🔁 Dùng lại cụm 3 Big value của D7-2. |
| 3 | **KPIKQI SẢN PHẨM** · Mixed (bar chồng) | `_kpikqi_new`<br>Trục X `customer_group` + `spdv` · Giá trị ✅`tong_count_sp_fixed` · Nhóm màu `evaluation`<br>Lọc: `spdv` ∈ (Audit ANHT, Pentest-TTDVATTT, Pentest-VPMN, SOC, SOC - VPMB, SOC - VPMN) | 🔁 Như D7-4 nhưng cho nhóm dịch vụ Audit/Pentest/SOC theo vùng miền. | Tạo Mixed chart cột chồng từ `production_management_1_kpikqi_new`: trục X `customer_group` + `spdv` (tăng dần), giá trị `tong_count_sp_fixed`, nhóm màu `evaluation`, bật Stack. Lọc `spdv IN ('Audit ANHT','Pentest-TTDVATTT','Pentest-VPMN','SOC','SOC - VPMB','SOC - VPMN')`. Đặt tiêu đề "KPIKQI SẢN PHẨM". |
| 4 | **CẢNH BÁO FAIL KPI 2 KỲ** · Table (pivot) | 🔁 Giống hệt D7-3 | 🔁 Như D7-3. | 🔁 Dùng lại chart "CẢNH BÁO FAIL KPI 2 KỲ" của D7. |
| 5 | **CHI TIẾT KẾT QUẢ THEO SẢN PHẨM** · Table | 🔁 Giống hệt D7-5 | 🔁 Như D7-5. | 🔁 Dùng lại chart "CHI TIẾT KẾT QUẢ THEO SẢN PHẨM" của D7. |

---

## D42 — 2. VVIP-old

**Mục đích:** **Bản cũ của D8**, chưa được xóa. 11 trong 13 chart trùng với D8. Tuy nhiên có **2 chart hữu ích mà D8 không có**, nên đừng xóa trang này mà không chuyển hai chart đó sang.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1–6, 13 | **SOC Platform / KIAN / SOAR / NSM / SIEM / NDR / EDR** · Bar ×7 | 🔁 Cùng khuôn với D8 | 🔁 Xem mục D8. | 🔁 Dùng **prompt chung** ở mục D8. |
| 7 | *(tiêu đề kỳ báo cáo)* · Big value | 🔁 Như D7-1 | | 🔁 Dùng markdown tile. |
| 8 | **KHÁCH HÀNG** · Big value ×3 | 🔁 Giống D8-11 | | 🔁 Dùng lại cụm Big value của D8-11. |
| 9 | **CHỈ SỐ SẢN PHẨM THEO KHÁCH HÀNG** · Mixed | 🔁 Giống D8-2 | | 🔁 Dùng lại chart D8-2. |
| 10 | **SỐ LƯỢNG KHÁCH HÀNG ĐẠT TOÀN BỘ KPI THEO TUẦN** · Table (pivot) | 🔁 Giống D8-12 | | 🔁 Dùng lại chart D8-12. |
| **11** | **SỐ LƯỢNG KHÁCH HÀNG ĐẠT TOÀN BỘ KPI THEO THÁNG** · Table (pivot) | Hàng: `spdv`<br>Giá trị: ⚠️`tong_customer`, ⚠️`tong_kh_passed_month_n`, ⚠️`tong_khach_passed_period_n_1`, ⚠️`tong_khach_passed_period_n_2` | ⭐ **Chart D8 không có.** Bản theo **tháng** của D8-12. Nhìn theo tháng lọc bớt nhiễu tuần, hợp với báo cáo cấp cao hơn. | Tạo Table pivot từ `production_management_1_kpikqi_new`: hàng `spdv`, cột giá trị `tong_customer` ("Tổng KH"), `tong_kh_passed_month_n` ("Tháng N"), `tong_khach_passed_period_n_1` ("Tháng N-1"), `tong_khach_passed_period_n_2` ("Tháng N-2"). Lọc `customer_group = 'KHÁCH HÀNG NGOÀI'`. Đặt tiêu đề "SỐ LƯỢNG KHÁCH HÀNG ĐẠT TOÀN BỘ KPI THEO THÁNG". |
| **12** | **PHÂN LOẠI NGUYÊN NHÂN LỖI** · Table (pivot) | `production_management_1_cause` + `_cause_project`<br>Hàng: `spdv` · Cột: `cause_checkbox`<br>Giá trị: ✅`count_distinct_id`<br>Lọc: `spdv` ∈ 6 sản phẩm bảo mật | ⭐ **Chart D8 không có.** Ma trận **sản phẩm × nguyên nhân lỗi**. Trả lời câu hỏi tổng hợp: các chỉ tiêu bị fail chủ yếu do hạ tầng, do agent, do cấu hình hay do khách? Rất đáng giữ. | Tạo Table pivot từ `production_management_1_cause` join `production_management_1_cause_project`: hàng `spdv`, cột `cause_checkbox`, giá trị metric `count_distinct_id` (nhãn "SỐ LỖI"). Lọc `spdv IN ('EDR','NDR','NSM','SIEM','SOAR','SOC Platform')`. Đặt tiêu đề "PHÂN LOẠI NGUYÊN NHÂN LỖI". |

---

## D43 — 4. DỊCH VỤ

**Mục đích:** KPI ở **cấp dịch vụ** — không gắn với khách hàng cụ thể mà đo chất lượng chung của dịch vụ. Dùng bảng `kpikqi_all` thay vì `kpikqi_new`.

**Bộ lọc trang:** năm, khách hàng, SPDV, quý, chu kỳ, tháng, tuần.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(tiêu đề kỳ báo cáo)* · Big value | ⚠️`report_week_new` | 🔁 Như D7-1. | 🔁 Dùng markdown tile. |
| 2 | *(tổng hợp chỉ số dịch vụ)* · Big value ×3 | `_kpikqi_all`<br>⚠️`tong_count_dv_fixed`, ⚠️`tong_count_dv_passed_all`, ⚠️`tong_count_dv_khong_passed_all` | Tổng số chỉ số dịch vụ đang đo, số đạt, số không đạt. | Tạo 3 Big value từ `production_management_1_kpikqi_all`: `tong_count_dv_fixed` ("TỔNG SỐ CHỈ SỐ DỊCH VỤ"), `tong_count_dv_passed_all` ("TỔNG SỐ CHỈ SỐ ĐẠT"), `tong_count_dv_khong_passed_all` ("TỔNG SỐ CHỈ SỐ KHÔNG ĐẠT"). |
| 3 | **KPIKQI DỊCH VỤ** · Mixed (bar chồng) | `_kpikqi_all`<br>Trục X `spdv` · Giá trị ⚠️`tong_count_dv_fixed` · Nhóm màu `evaluation`<br>Lọc: `spdv` ∈ 11 dịch vụ | Với mỗi dịch vụ, bao nhiêu chỉ số đạt / không đạt. | Tạo Mixed chart cột chồng từ `production_management_1_kpikqi_all`: trục X `spdv`, giá trị metric `tong_count_dv_fixed` (sắp xếp giảm dần), nhóm màu `evaluation`, bật Stack. Lọc `spdv IN ('EDR','KIAN','M-Suite','NDR','NSM','SE','SIEM','SOAR','TI Sản phẩm','VCS-KIAN','SOC Platform tenant')`. Đặt tiêu đề "KPIKQI DỊCH VỤ". |
| 4 | **CẢNH BÁO FAIL 2 KỲ** · Table (pivot) | `_kpikqi_new`<br>Hàng: `spdv`, `metric_name`<br>Giá trị: ⚠️`result_fact`, ⚠️`target_dim`, ✅`min_alert_icon`, ⚠️`result_prev_1_period_fact`, ⚠️`result_prev_2_periods_fact`<br>Lọc: `spdv` ∈ (EDR, KIAN, NDR, NSM, SIEM, SOAR, mSuite) | 🔁 Cùng ý tưởng D7-3, áp cho nhóm dịch vụ. | Tạo Table pivot từ `production_management_1_kpikqi_new`: hàng `spdv` và `metric_name`, cột giá trị `target_dim`, `result_fact` ("Kết quả hiện tại"), `result_prev_1_period_fact`, `result_prev_2_periods_fact`, `min_alert_icon` ("Cảnh báo"). Lọc `spdv IN ('EDR','KIAN','NDR','NSM','SIEM','SOAR','mSuite')` và cờ fail 2 kỳ. Đặt tiêu đề "CẢNH BÁO FAIL 2 KỲ". |
| 5 | **CHI TIẾT KẾT QUẢ THEO DỊCH VỤ** · Table | `_kpikqi_all`<br>Cột: `report_date`, `spdv`, `metric_name`, `target`, `unit_of_measure`, `result`, `evaluation`, `report_month_date`<br>Lọc: loại KPI = Dịch vụ | Bảng chi tiết từng chỉ số dịch vụ. | Tạo Table từ `production_management_1_kpikqi_all` với các cột `report_date` (MM/dd/yyyy), `report_month_date`, `spdv`, `metric_name`, `target`, `unit_of_measure`, `result` (2 chữ số thập phân), `evaluation`. Lọc loại KPI = 'Dịch vụ'. Đặt tiêu đề "CHI TIẾT KẾT QUẢ THEO DỊCH VỤ". |

---

## D47 — KPIKQI Sản phẩm

**Mục đích:** Bảng thô KPI **cấp sản phẩm**, tương ứng với D6 (bảng thô KPI Tập đoàn). Có thể bỏ khi dựng lại.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(bảng thô KPI sản phẩm)* · Table | `_kpikqi_new`<br>Cột: `spdv`, `metric_name`, `unit`, `target_fin`, `target_condition`, `result`, `evaluation`, `kpi_trend`, `customer_2`, `report_date_2`, `report_month_date`, `result_prev_1_period`, `result_prev_2_periods`<br>Lọc: loại KPI = Sản phẩm | Toàn bộ trường dữ liệu KPI sản phẩm. | Tạo Table từ `production_management_1_kpikqi_new` với các cột `report_date_2`, `report_month_date`, `customer_2`, `spdv`, `metric_name`, `unit`, `target_fin`, `target_condition`, `result`, `result_prev_1_period`, `result_prev_2_periods`, `kpi_trend`, `evaluation`. Lọc loại KPI = 'Sản phẩm'. Đặt tiêu đề "Dữ liệu KPI sản phẩm". |

---

## Nhóm trang bảng chi tiết KPIKQI — 19 trang cùng một khuôn

**Các trang:** D3 (Cloudrity), D14 (KH ngoài SOC), D15 (SOC VVIP), D16 (KIAN VVIP), D17 (EDR VVIP), D24 (VVIP kết quả chi tiết), D25 (VIP chi tiết), D26 (NDR VVIP), D27 (NSM VVIP), D28 (SIEM VVIP), D29 (SOAR VVIP), D30 (EDR VIP), D31 (NDR VIP), D32 (NSM VIP), D33 (SIEM VIP), D34 (SOCP-VIETTEL), D37 (KPIKQI Dịch vụ), D38 (KPIKQI KH ngoài), D48 (SP theo trung tâm).

**Tên trang đúng như trong `layout/pages.json`** (để tra ngược từ file sang tài liệu):
`CLOUDRITY VVIP KPIKQI KHÁCH HÀNG NGOÀI` (D3) ·
`KPIKQI KHÁCH HÀNG NGOÀI SOC` (D14) ·
`SOC VVIP KPIKQI KHÁCH HÀNG NGOÀI` (D15) ·
`KIAN VVIP KPIKQI KHÁCH HÀNG NGOÀI` (D16) ·
`EDR VVIP KPIKQI KHÁCH HÀNG NGOÀI` (D17) ·
`VIP - CHI TIẾT KPIKQI KHÁCH HÀNG NGOÀI` (D25) ·
`NDR VVIP KPIKQI KHÁCH HÀNG NGOÀI` (D26) ·
`NSM VVIP KPIKQI KHÁCH HÀNG NGOÀI` (D27) ·
`SIEM VVIP KPIKQI KHÁCH HÀNG NGOÀI` (D28) ·
`SOAR VVIP KPIKQI KHÁCH HÀNG NGOÀI` (D29) ·
`EDR VIP KPIKQI KHÁCH HÀNG NGOÀI` (D30) ·
`NDR VIP KPIKQI KHÁCH HÀNG NGOÀI` (D31) ·
`NSM VIP KPIKQI KHÁCH HÀNG NGOÀI` (D32) ·
`SIEM VIP KPIKQI KHÁCH HÀNG NGOÀI` (D33)

**Mục đích:** Mỗi trang là **đúng một bảng chi tiết**, cấu trúc cột giống hệt nhau, chỉ khác bộ lọc sản phẩm / nhóm khách. Trong Power BI phải tách trang vì không có cách nào cho người dùng tự đổi bộ lọc mà giữ được layout. **Trên Lightdash thì hoàn toàn không cần** — chỉ cần một saved chart cộng dashboard filter cho `spdv` và `customer_type`.

**Khuôn cột chung** (model `production_management_1_kpikqi_new`):

`report_date_2` · `criteria_period_type` · `customer_2` · `spdv` · `metric_name` · `target_fin` · `target_condition` (hoặc `condition`) · ⚠️`result_fact` · `evaluation` · `kpi_trend` · ⚠️`result_prev_1_period_fact` · ⚠️`result_prev_2_periods_fact`

**Ý nghĩa:** Với từng chỉ tiêu của từng khách hàng: target là bao nhiêu, điều kiện đạt là gì, kỳ này ra bao nhiêu, hai kỳ trước ra bao nhiêu, xu hướng đi lên hay xuống, kết luận đạt hay không.

**Prompt chung — tạo một lần, dùng cho cả 16 trang:**

> Tạo Table trên Lightdash từ model `production_management_1_kpikqi_new` với các cột theo thứ tự: `report_date_2`, `criteria_period_type`, `customer_2`, `spdv`, `metric_name`, `target_fin`, `target_condition`, `result_fact`, `result_prev_1_period_fact`, `result_prev_2_periods_fact`, `kpi_trend`, `evaluation`. Đặt tiêu đề "Chi tiết KPIKQI khách hàng". Sau đó thêm 3 dashboard filter cho `spdv`, `customer_type` và `customer_group` để người dùng tự chọn.

**Bảng đối chiếu bộ lọc từng trang** (dùng khi vẫn muốn giữ trang riêng):

| Trang | Bộ lọc riêng | Ghi chú |
|---|---|---|
| D3 CLOUDRITY VVIP | `spdv = 'Cloudrity'` | |
| D15 SOC VVIP | `customer_group = 'KHÁCH HÀNG NGOÀI'`, sản phẩm SOC | |
| D16 KIAN VVIP | `spdv = 'KIAN'`, KH ngoài | |
| D17 EDR VVIP | `spdv = 'EDR'`, KH ngoài | |
| D24 VVIP - KẾT QUẢ CHI TIẾT | `customer_type = 'VVIP'`, KH ngoài | Có thêm cột `period_result` |
| D25 VIP - CHI TIẾT | `customer_type = 'VIP'`, KH ngoài | |
| D26 NDR VVIP | `spdv = 'NDR'`, KH ngoài | |
| D27 NSM VVIP | `spdv = 'NSM'`, KH ngoài | |
| D28 SIEM VVIP | `spdv = 'SIEM'`, KH ngoài | |
| D29 SOAR VVIP | `spdv = 'SOAR'`, KH ngoài | |
| D30 EDR VIP | `spdv = 'EDR'`, `customer_type = 'VIP'` | |
| D31 NDR VIP | `spdv = 'NDR'`, `customer_type = 'VIP'` | |
| D32 NSM VIP | `spdv = 'NSM'`, `customer_type = 'VIP'` | |
| D33 SIEM VIP | `spdv = 'SIEM'`, `customer_type = 'VIP'` | |
| D34 1. SOCP-VIETTEL | KH ngoài | Trùng tên với D7 nhưng chỉ có 1 bảng |
| D38 KPIKQI KHÁCH HÀNG NGOÀI | `customer_group = 'KHÁCH HÀNG NGOÀI'` | Không lọc sản phẩm |

**Ba trang cùng nhóm nhưng dùng model khác:**

| Trang | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| D14 | *(chi tiết KPI SOC)* · Table | `production_management_1_soc_results`<br>Cột: `report_date`, `customer`, `metric_name`, `target`, `condition`, `result`, `evaluation`, `unit_of_measure` | Bảng chi tiết cho dịch vụ SOC, dùng bảng `soc_results` riêng chứ không phải `kpikqi_new`. | Tạo Table từ `production_management_1_soc_results` với các cột `report_date`, `customer`, `metric_name`, `unit_of_measure`, `target`, `condition`, `result`, `evaluation`. Đặt tiêu đề "Chi tiết KPIKQI khách hàng ngoài - SOC". |
| D37 | *(chi tiết KPI dịch vụ)* · Table | `production_management_1_kpikqi_all`<br>Cột: `report_date`, `spdv`, `metric_name`, `target`, `result`, `unit_of_measure`, `evaluation`, `condition`, `report_month_date`, `result_prev_1_period`, `kpi_trend`<br>Lọc: loại KPI = Dịch vụ | 🔁 Gần trùng D43-5. | 🔁 Dùng lại chart "CHI TIẾT KẾT QUẢ THEO DỊCH VỤ" (D43-5), bổ sung cột `kpi_trend` và `result_prev_1_period`. |
| D48 | *(sản phẩm theo trung tâm)* · Table | `production_management_1_project_vs_department`<br>Cột: `division`, `project_name` | Bảng tra cứu đơn giản: sản phẩm nào thuộc trung tâm nào. Nên chuyển thành **bảng chiều** trong dbt thay vì một trang dashboard. | Tạo Table từ `production_management_1_project_vs_department` với 2 cột `division` và `project_name`. Cân nhắc bỏ hẳn trang này và dùng bảng chiều trong model. Đặt tiêu đề "Sản phẩm theo trung tâm". |

---

## D4 — QTSX - Quý I

**Mục đích:** Trả lời câu hỏi mà lãnh đạo sản xuất hay hỏi nhất: **"công của đội đang đổ vào đâu?"** — làm tính năng mới, sửa lỗi nội bộ, hay sửa lỗi cho khách.

**Bộ lọc trang:** năm, tháng, quý, tên project, khối, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **QUẢN TRỊ SẢN XUẤT CÔNG TY** · Table (pivot) | `production_management_1_mmrelease`<br>Hàng: `project_name`<br>Giá trị: ✅`sum_mm_release` (tổng MM), ⚠️`mm_fix_bug_release`, ⚠️`mm_fix_bug_cho_kh`, ⚠️`mm_release_tinh_nang`, ⚠️`mm_release_tinh_nang_cho_kh`, ⚠️`mm_os`, cùng 4 phiên bản phần trăm tương ứng | Bốn cách tiêu công, mỗi cách có cả số tuyệt đối và tỷ lệ: **tính năng mới**, **tính năng cho khách**, **fix bug**, **fix bug cho khách**, cộng thêm phần thuê ngoài (MM OS). Nếu %mm fix bug vượt %mm tính năng thì đội đang chủ yếu dọn nợ chứ không tạo giá trị mới. | Tạo Table pivot trên Lightdash từ model `production_management_1_mmrelease`: hàng `project_name`, cột giá trị theo thứ tự `sum_mm_release`, `mm_release_tinh_nang`, `mm_release_tinh_nang_cho_kh`, `mm_fix_bug_release`, `mm_fix_bug_cho_kh`, `mm_os`, rồi 4 cột phần trăm `mm_release_tinh_nang`, `mm_release_tinh_nang_cho_kh`, `mm_fix_bug_release`, `mm_release_fix_bug_cho_kh` (định dạng % 2 chữ số). Đặt tiêu đề "QUẢN TRỊ SẢN XUẤT CÔNG TY". |

---

## D18 — Project *(manmonth fix version)*

**Mục đích:** Theo dõi công đổ vào **từng fix version** (bản vá / bản nâng cấp cụ thể trên Jira), so với tổng man-month được cấp. Trả lời: bản release này thực sự tiêu tốn bao nhiêu.

**Bộ lọc trang:** khối, tên project, năm, tháng, quý, tên fix version.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **MANMONTH FIX VERSION THEO RELEASE DATE** · Mixed (bar + line) | `_ulnl` + `_worklogs` + `_mamday_hr` + `_project_name`<br>Trục X: `project_name_last` + `quarter_year`<br>Cột: ⚠️`ulnl_fix_version`, ⚠️`worklog_fix_version`, ✅`sum_man_month`<br>Đường (trục phải): ⚠️`ulnl_done_release`, ⚠️`worklog_fix_version` (%) | Ba cột là ba cách đo cùng một khối lượng: **ULNL** (ước lượng), **worklog** (giờ log thực), **MM** (man-month được cấp). Hai đường là tỷ lệ phần công đó thực sự nằm trong bản đã release. | Tạo Mixed chart trên Lightdash gộp `production_management_1_ulnl`, `_worklogs`, `_mamday_hr`, `_project_name`: trục X `project_name_last` + `quarter_year`. Cột trục trái: `ulnl_fix_version`, `worklog_fix_version`, `sum_man_month` (nhãn "MM TỔNG"). Đường trục phải: `ulnl_done_release` và tỷ lệ `worklog_fix_version` ở dạng phần trăm. Đặt tiêu đề "MANMONTH FIX VERSION THEO RELEASE DATE". |
| 2 | **MANMONTH FIX VERSION THEO RELEASE DATE** · Table (pivot) | `_fix_versions`<br>Hàng: `fix_version_name`, `project_name` · Cột: `quarter_year`<br>Giá trị: ✅`sum_ulnl_mm_release`, ✅`sum_logworks`, ⚠️`ulnl_fixversions` (%), ⚠️`logwork_fixversions` (%), ⚠️`mm_tong_fixversions` | Bản bảng của chart 1, xuống tới từng fix version. Cột "ULNL đã trừ tái sử dụng" là điểm đáng chú ý: phần code dùng lại được trừ ra để không tính công hai lần. | Tạo Table pivot từ `production_management_1_fix_versions`: hàng `project_name` và `fix_version_name`, cột `quarter_year`, cột giá trị `sum_ulnl_mm_release` ("ULNL ĐÃ TRỪ TÁI SD"), `sum_logworks` ("LOGWORKS"), `mm_tong_fixversions`, `ulnl_fixversions` (%), `logwork_fixversions` (%). Đặt tiêu đề "MANMONTH FIX VERSION THEO RELEASE DATE". |
| 3 | *(bảng phẳng fix version)* · Table | `_fix_versions`<br>Cột: `quarter_year`, `project_name`, ✅`sum_ulnl_mm_release`, ✅`sum_logworks`, ⚠️`ulnl_fixversions`, ⚠️`logwork_fixversions`, ⚠️`mm_tong_fixversions` | 🔁 Bản không pivot của chart 2, để xuất Excel. | Tạo Table (không pivot) từ `production_management_1_fix_versions` với các cột `quarter_year`, `project_name`, `sum_ulnl_mm_release`, `sum_logworks`, `mm_tong_fixversions`, `ulnl_fixversions`, `logwork_fixversions`. |

---

## D21 — FIX VERSIONS

**Mục đích:** Bảng thô fix version, trùng nội dung với D18-3. **Nên bỏ** khi dựng lại.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(bảng thô fix version)* · Table | `_fix_versions`<br>Cột: `quarter_year`, `fix_version_name`, ✅`sum_logworks`, ✅`sum_ulnl_mm_release`, ⚠️`logwork_fixversions`, ⚠️`ulnl_fixversions`, ⚠️`mm_tong_fixversions` | 🔁 Gần trùng D18-3, chỉ khác là hiện `fix_version_name` thay vì `project_name`. | 🔁 Dùng lại chart D18-3, thêm cột `fix_version_name`. |

---

## D22 — Check Chi tiết

**Mục đích:** Trang **đối soát** — dùng khi số liệu ở D40/D45 lệch và cần lần xuống dữ liệu gốc. Đặc điểm nhận dạng: có cả `sum_worklog_logged_time` (giây), `worklog_time_hours` (giờ) và `sum_worklog_mm` (man-month) để kiểm tra công thức quy đổi.

**Bộ lọc trang:** tên project, khối, quý, năm, tháng, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **QUẢN TRỊ SẢN XUẤT CÔNG TY** · Table (pivot) | Hàng: `project_name_last` · Cột: `week_year`, `month_year`, `year_value`<br>Giá trị: ✅`average_release_frequency`, ✅`count_distinct_issue_key` (số story), ✅`average_days_to_done` (cycle time), ✅`sum_man_month`, ✅`sum_worklog_mm`, ✅`sum_ulnl_mm`, ✅`sum_worklog_logged_time` (giây), ✅`worklog_time_hours` (giờ), ✅`max_worklog_worklog_date`, ✅`sum_total_effort_excl_reuse` | Bảng kiểm tra chéo: cùng một khối lượng công được thể hiện bằng giây, giờ và man-month để phát hiện sai số quy đổi. Cột `max_worklog_worklog_date` cho biết dữ liệu worklog mới nhất đến ngày nào — rất hữu ích khi nghi ngờ số liệu chưa cập nhật. | Tạo Table pivot gộp `production_management_1_project_name`, `_dgcl_ttk`, `_cycle_time`, `_mamday_hr`, `_worklogs`, `_ulnl` join `production_management_1_dim_date`. Hàng: `project_name_last`. Cột: `year_value`, `month_year`, `week_year`. Cột giá trị: `average_release_frequency`, `count_distinct_issue_key`, `average_days_to_done`, `sum_man_month`, `sum_worklog_mm`, `sum_ulnl_mm`, `sum_total_effort_excl_reuse`, `sum_worklog_logged_time`, `worklog_time_hours`, `max_worklog_worklog_date`. Đặt tiêu đề "Đối soát số liệu sản xuất". |

---

## D23 — Project

**Mục đích:** Trang sức khỏe project ở mức chi tiết: công đổ vào fix version, tỷ lệ defect, bug production, và hai biểu đồ bong bóng so sánh nhiều chiều.

**Bộ lọc trang:** khối, tên project, năm, tháng, quý, tên fix version.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **MANMONTH FIX VERSION THEO RELEASE DATE** · Mixed | `_fix_versions`<br>Trục X: `quarter_year` + `fix_version_name`<br>Cột: ✅`sum_ulnl_mm_release`, ✅`sum_logworks`, ⚠️`mm_tong_fixversions`<br>Đường (trục phải): ⚠️`logwork_fixversions` (%), ⚠️`ulnl_fixversions` (%)<br>Lọc: quý 2025 | Cột là khối lượng tuyệt đối, đường là tỷ lệ. Nhìn cùng lúc "làm bao nhiêu" và "bao nhiêu phần thực sự vào bản release". | Tạo Mixed chart từ `production_management_1_fix_versions`: trục X `quarter_year` (tăng dần) + `fix_version_name`. Cột trục trái: `sum_ulnl_mm_release`, `sum_logworks`, `mm_tong_fixversions`. Đường trục phải: `logwork_fixversions` và `ulnl_fixversions` định dạng %. Lọc `quarter IN ('Q1 2025','Q2 2025','Q3 2025','Q4 2025')`. Đặt tiêu đề "MANMONTH FIX VERSION THEO RELEASE DATE". |
| 2 | **FIX VERSION THEO RELEASE DATE & PROJECT** · Mixed | `_fix_versions`<br>Trục X `quarter_year`, nhóm `project_name`<br>Đường (trục phải): ⚠️`ulnl_fixversions`, ⚠️`logwork_fixversions` | Bản chỉ có tỷ lệ của chart 1, tách theo project. | Tạo Mixed chart từ `production_management_1_fix_versions`: trục X `quarter_year`, nhóm theo `project_name`. Hai đường trục phải: `ulnl_fixversions` và `logwork_fixversions` định dạng %. Đặt tiêu đề "FIX VERSION THEO RELEASE DATE & PROJECT". |
| 3 | **%LOGWORK FIX VERSION THEO RELEASE DATE & PROJECT** · Line | `_fix_versions`<br>Trục X `quarter_year` · Nhóm màu `project_name` · Giá trị ⚠️`logwork_fixversions` | Xu hướng theo quý: mỗi project là một đường. Đường đi lên nghĩa là ngày càng nhiều công thực sự vào bản release. | Tạo Line chart từ `production_management_1_fix_versions`: trục X `quarter_year` (tăng dần), giá trị metric `logwork_fixversions` định dạng %, nhóm màu theo `project_name`. Đặt tiêu đề "%LOGWORK FIX VERSION THEO RELEASE DATE & PROJECT". |
| 4 | **%ULNL FIX VERSION THEO RELEASE DATE & PROJECT** · Line | Như chart 3, giá trị ⚠️`ulnl_fixversions` | Song sinh của chart 3, đo bằng ULNL thay vì worklog. | Tạo Line chart từ `production_management_1_fix_versions`: trục X `quarter_year`, giá trị `ulnl_fixversions` định dạng %, nhóm màu `project_name`. Đặt tiêu đề "%ULNL FIX VERSION THEO RELEASE DATE & PROJECT". |
| 5 | 🐍 **CÁC SẢN PHẨM THƯỜNG XUYÊN RELEASE** · Python visual | ⚠️`production_bug_rate`, ✅`average_days_to_done`, ✅`average_release_frequency`, ⚠️`rate_defect`, ⚠️`hieu_suat_load_nguoi`<br>Lọc: 8 project release đều | **Biểu đồ bong bóng 5 chiều** so sánh các sản phẩm ra bản đều đặn: tần suất release, cycle time, tỷ lệ bug production, tỷ lệ defect, hiệu suất load người. Mục đích là tìm sản phẩm "nhanh mà vẫn sạch". Lightdash không dựng lại được dạng này. | Lightdash không hỗ trợ Python visual. **Thay bằng Scatter chart**: trục X là metric `average_release_frequency`, trục Y là metric `production_bug_rate`, kích thước điểm là metric `hieu_suat_load_nguoi`, mỗi điểm là một `project_name_last`. Lọc 8 project: `Cloudrity Self Service`, `NSM - NDR`, `SOC PLATFORM`, `Threat Intelligence`, `V-AntiDDoS`, `VCS-CyCir`, `VCS-CyM`, `aJiant`. Thêm `average_days_to_done` và `rate_defect` vào tooltip. Đặt tiêu đề "CÁC SẢN PHẨM THƯỜNG XUYÊN RELEASE". |
| 6 | **TỶ LỆ DEFECT BY PROJECT** · Bar | `_defect` + `_dim_date`<br>Trục X: ngày / tháng / quý / tuần · Giá trị ⚠️`rate_defect` | Tỷ lệ defect theo thời gian, có thể drill từ quý xuống ngày. **Thiếu dimension project** dù tên chart nói "by project" — cần bổ sung khi dựng lại. | Tạo Bar chart từ `production_management_1_defect` join `production_management_1_dim_date`: trục X `quarter_year` → `month_year` → `week` → `date_value` (drill được), giá trị metric `rate_defect`. **Bổ sung** nhóm màu theo `project_name` để đúng với tên chart. Đặt tiêu đề "TỶ LỆ DEFECT BY PROJECT". |
| 7 | 🐍 **NGUỒN LỰC THEO PROJECT** · Python visual | Cùng 5 metric với chart 5, thêm dimension `project_name_last`<br>Lọc: 7 project khác | 🔁 Cùng dạng bong bóng với chart 5 nhưng cho nhóm project còn lại (BRG, TML, Server Endpoint, VCS-Hub, VCS-KIAN, VCS-InT, F2DR). | 🔁 Dùng cùng cấu hình Scatter chart như chart 5, đổi bộ lọc project thành `BRG`, `TML`, `Server Endpoint`, `VCS-Hub`, `VCS-KIAN`, `VCS-InT INSIDER THREAT`, `F2DR`. Đặt tiêu đề "NGUỒN LỰC THEO PROJECT". |
| 8 | **BUG PRODUCTION THEO PROJECT** · Mixed | `_bug_production` + `_project_name` + `_dim_date`<br>Trục X `project_name_last` · Giá trị ✅`count_bug_production` · Nhóm màu `quarter_year`<br>Lọc: 4 project | Số bug production theo project, chia màu theo quý để thấy xu hướng. Metric đã loại sẵn bug bị `Rejected`. | Tạo Mixed chart từ `production_management_1_bug_production` join `_project_name` và `_dim_date`: trục X `project_name_last`, giá trị metric `count_bug_production`, nhóm màu theo `quarter_year`. Lọc `project_name_last IN ('aJiant','NSM - NDR','VCS-CyCir','VCS-CyM')`. Đặt tiêu đề "BUG PRODUCTION THEO PROJECT". |

---

## D39 — CLDV *(bản đầy đủ)*

**Mục đích:** CLDV = **Chất lượng dịch vụ**. Đây là trang duy nhất trong cả báo cáo nhìn từ phía **khách hàng cảm nhận** thay vì từ phía sản xuất. Ba nhóm nội dung:

1. **Điểm hài lòng** — CSAT (mức hài lòng) và NPS (mức sẵn sàng giới thiệu).
2. **Sự cố** — số lượng, nguyên nhân, mức ưu tiên, trạng thái xử lý.
3. **Chỉ số vận hành ATTT** — MTTD, MTTR, cảnh báo bỏ lọt, độ phủ content và phiên bản agent.

**Bộ lọc trang:** mã VVIP (`production_management_1_vvip.code`), chu kỳ báo cáo, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(CSAT CLDV)* · Big value | `_fact_tong_hop_diem` · ✅`average_csat_product_quality` | Điểm hài lòng trung bình của khách về chất lượng sản phẩm dịch vụ. | Tạo Big value từ `production_management_1_fact_tong_hop_diem` với metric `average_csat_product_quality`. Đặt tiêu đề "CSAT CLDV". |
| 2 | **SỰ CỐ PHÁT SINH VÀ NGUYÊN NHÂN TÁC ĐỘNG** · Mixed (bar chồng) | `_n_5_vvip_incident_cause_id`<br>Trục X `customer` · Giá trị ✅`count_issue_id` · Nhóm màu `incident_cause` | Với mỗi khách VVIP: bao nhiêu sự cố, và chia theo nguyên nhân. Khách nào cột cao là khách đang chịu nhiều gián đoạn nhất. | Tạo Mixed chart cột chồng từ `production_management_1_n_5_vvip_incident_cause_id`: trục X `customer`, giá trị metric `count_issue_id`, nhóm màu theo dimension `incident_cause`, bật Stack. Đặt tiêu đề "SỰ CỐ PHÁT SINH VÀ NGUYÊN NHÂN TÁC ĐỘNG". |
| 3 | **TỈ LỆ UPDATE AGENT LATEST** · Mixed (bar chồng) | `_kpikqi_service_tkcg` + `_dim_date`<br>Trục X `product` + `quarter_year` · Giá trị ✅`average_latest_version_coverage_tkcg` · Nhóm màu `version`<br>Tooltip ✅`max_latest_version` | Bao nhiêu phần trăm agent đã cập nhật lên phiên bản mới nhất. Agent cũ là lỗ hổng bảo mật, nên chỉ số này thấp là rủi ro trực tiếp. | Tạo Mixed chart cột chồng từ `production_management_1_kpikqi_service_tkcg` join `production_management_1_dim_date`: trục X `product` + `quarter_year`, giá trị metric `average_latest_version_coverage_tkcg`, nhóm màu theo `version`, tooltip thêm `max_latest_version`. Đặt tiêu đề "TỈ LỆ UPDATE AGENT LATEST". |
| 4 | *(xu hướng CSAT)* · Line | `_fact_tong_hop_diem` + `_dim_date`<br>Trục X `quarter_year` · Giá trị ✅`average_csat_product_quality` | CSAT theo quý. Đường đi xuống là cảnh báo sớm về nguy cơ mất khách. | Tạo Line chart từ `production_management_1_fact_tong_hop_diem` join `production_management_1_dim_date`: trục X `quarter_year`, giá trị metric `average_csat_product_quality`. Đặt tiêu đề "Xu hướng CSAT CLDV". |
| 5 | **Tổng sự cố** · Big value | `_n_5_vvip_incident_cause_id` · ✅`count_issue_id` | Tổng số sự cố trong kỳ. | Tạo Big value từ `production_management_1_n_5_vvip_incident_cause_id` với metric `count_issue_id`. Đặt tiêu đề "Tổng sự cố". |
| 6 | **MTTD-MTTR-MISS TẤN CÔNG ATTT** · Mixed (bar + line) | `_mttd_mttr_miss_tan_cong`<br>Trục X `quarter` + `customer`<br>Cột: ✅`sum_mttr_h`, ✅`average_mttd_h`<br>Đường (trục phải): ✅`sum_missed_critical_alerts`, ✅`sum_missed_normal_alerts`, ✅`sum_total_incident_cases` | **Chart quan trọng nhất trang này.** Cột là thời gian phát hiện và xử lý (giờ), đường là số cảnh báo bị bỏ lọt và tổng số case. Đọc cùng nhau: phát hiện nhanh mà vẫn bỏ lọt nhiều nghĩa là hệ thống phát hiện có vùng mù. | Tạo Mixed chart từ `production_management_1_mttd_mttr_miss_tan_cong`: trục X `quarter` + `customer`. Cột trục trái: metric `average_mttd_h` (nhãn "(MTTD)(h)") và `sum_mttr_h` (nhãn "(MTTR)(h)"). Đường trục phải: `sum_missed_critical_alerts`, `sum_missed_normal_alerts`, `sum_total_incident_cases`. Đặt tiêu đề "MTTD-MTTR-MISS TẤN CÔNG ATTT". |
| 7 | **ĐỘ PHỦ CONTENT** · Mixed | `_content_coverage`<br>Trục X `product` · Đường (trục phải) ✅`sum_content_coverage_pct` | Tỷ lệ phủ của bộ luật phát hiện (content/rule) trên từng sản phẩm. Phủ thấp nghĩa là còn nhiều kiểu tấn công chưa có luật nhận diện. | Tạo Mixed chart từ `production_management_1_content_coverage`: trục X `product`, đường trục phải là metric `sum_content_coverage_pct` định dạng phần trăm. Đặt tiêu đề "ĐỘ PHỦ CONTENT". |
| 8 | **PHÂN LOẠI SỰ CỐ** · Bar | `_rpm_risk_problem` + `_dim_date`<br>Trục X `quarter_year` · Giá trị ✅`count_id` · Nhóm màu `issue_current_type_name` | Sự cố chia theo loại (risk / problem) qua các quý. | Tạo Bar chart từ `production_management_1_rpm_risk_problem` join `production_management_1_dim_date`: trục X `quarter_year`, giá trị metric `count_id`, nhóm màu theo `issue_current_type_name`. Đặt tiêu đề "PHÂN LOẠI SỰ CỐ". |
| 9 | **SỐ LƯỢNG SỰ CỐ THEO ƯU TIÊN** · Bar | `_n_5_vvip_prm_incident_management` + `_dim_date`<br>Trục X `quarter_year` · Giá trị ✅`count_distinct_id` · Nhóm màu `issue_priority` | Sự cố chia theo mức ưu tiên. Nếu tổng số giảm nhưng phần "Highest" tăng thì tình hình đang xấu đi chứ không tốt lên. | Tạo Bar chart từ `production_management_1_n_5_vvip_prm_incident_management` join `production_management_1_dim_date`: trục X `quarter_year`, giá trị metric `count_distinct_id`, nhóm màu theo `issue_priority`. Đặt tiêu đề "SỐ LƯỢNG SỰ CỐ THEO ƯU TIÊN". |
| 10 | **TỶ LỆ AGENT ONLINE CÓ LOG** · Bar | `_kpikqi_new` + `_dim_date`<br>Trục X `month_year` / `quarter_year` · Giá trị ⚠️`result_fact` · Nhóm màu `spdv`<br>Lọc: `metric_name` chứa "có log", chu kỳ ∈ (Month, Quarter) | Agent online nhưng **không gửi log** thì coi như không giám sát được. Chỉ số này đo phần "giám sát thật" so với "giám sát trên giấy". | Tạo Bar chart từ `production_management_1_kpikqi_new` join `production_management_1_dim_date`: trục X `month_year` + `quarter_year`, giá trị metric `result_fact`, nhóm màu theo `spdv`. Lọc `metric_name LIKE '%có log%'` và `criteria_period_type IN ('Month','Quarter')`. Đặt tiêu đề "TỶ LỆ AGENT ONLINE CÓ LOG". |
| 11 | **NPS** · Mixed (bar + line) | `_fact_tong_hop_diem` + `_n_5_vvipkpicx`<br>Trục X `quarter_year`<br>Cột: ✅`average_nps_score`<br>Đường (trục phải): ✅`median_nps` (KPI NPS) | Cột là điểm NPS thực tế, đường là ngưỡng KPI cam kết. Cột thấp hơn đường là chưa đạt. | Tạo Mixed chart từ `production_management_1_fact_tong_hop_diem` join `production_management_1_n_5_vvipkpicx` và `_dim_date`: trục X `quarter_year`. Cột trục trái: metric `average_nps_score`. Đường trục phải: metric `median_nps` (nhãn "KPI NPS"). Đặt tiêu đề "NPS". |
| 12 | **CSAT CHẤT LƯỢNG** · Mixed (bar + line) | `_fact_tong_hop_diem`<br>Trục X `quarter_year`<br>Cột: ✅`average_csat_product_quality`<br>Đường (trục phải): ✅`stddev_csat_avg` | Cột là điểm CSAT trung bình, đường là **độ lệch chuẩn**. Điểm trung bình cao nhưng độ lệch lớn nghĩa là có nhóm khách rất hài lòng và nhóm rất bất mãn — nguy hiểm hơn là tất cả đều trung bình. | Tạo Mixed chart từ `production_management_1_fact_tong_hop_diem` join `production_management_1_dim_date`: trục X `quarter_year`. Cột trục trái: metric `average_csat_product_quality`. Đường trục phải: metric `stddev_csat_avg`. Đặt tiêu đề "CSAT CHẤT LƯỢNG". |
| 13 | **CSAT THEO SPDV** · Mixed (bar + line) | `_fact_tong_hop_diem` + `_n_5_vvipkpicx`<br>Trục X `spdv`<br>Cột: ✅`average_csat_product_quality`<br>Đường (trục phải): ✅`median_csat` | CSAT từng sản phẩm so với ngưỡng KPI. Sản phẩm nào kéo tụt điểm chung. | Tạo Mixed chart từ `production_management_1_fact_tong_hop_diem` join `production_management_1_n_5_vvipkpicx`: trục X `spdv`. Cột trục trái: metric `average_csat_product_quality`. Đường trục phải: metric `median_csat`. Đặt tiêu đề "CSAT THEO SPDV". |
| 14 | **TRẠNG THÁI XỬ LÝ SỰ CỐ** · Bar | `_n_5_vvip_prm_incident_management` + `_dim_date`<br>Trục X `quarter_year` · Giá trị ✅`count_id` · Nhóm màu `issue_current_status_name` | Sự cố đang ở trạng thái nào. Phần "chưa đóng" ngày càng dày là dấu hiệu tồn đọng. | Tạo Bar chart từ `production_management_1_n_5_vvip_prm_incident_management` join `production_management_1_dim_date`: trục X `quarter_year`, giá trị metric `count_id`, nhóm màu theo `issue_current_status_name`, bật Stack. Đặt tiêu đề "TRẠNG THÁI XỬ LÝ SỰ CỐ". |

---

## D36 — CLDV *(bản rút gọn)*

**Mục đích:** Phiên bản 7 chart của D39, gần như là tập con. Khi dựng lại **nên bỏ trang này** và dùng D39, trừ chart 4 có nội dung riêng.

**Bộ lọc trang:** mã VVIP, khách hàng.

| # | Chart | Trạng thái | Ghi chú |
|---|---|---|---|
| 1 | *(CSAT CLDV)* · Big value | 🔁 Giống D39-1 | |
| 2 | *(xu hướng CSAT)* · Line | 🔁 Giống D39-4 | |
| 3 | 🐍 **PHÂN BỔ NGUỒN LỰC THEO PROJECT** · Python visual | 🔁 Cùng dạng D23-7 | Lọc 8 project: aJiant, Cloudrity Self Service, NSM - NDR, SOC PLATFORM, Threat Intelligence, V-AntiDDoS, VCS-CyCir, VCS-CyM. Dùng lại cấu hình Scatter chart của D23-5. |
| **4** | **ĐỘ PHỦ PHIÊN BẢN MỚI NHẤT** · Mixed | ⭐ Chỉ có ở trang này | Xem prompt bên dưới |
| 5 | **CSAT từng SPDV** · Mixed | 🔁 Gần giống D39-13 | Khác ở chỗ dùng `sum_csat_score` thay vì `average_csat_product_quality`, và đường KPI là `kpi_csat` thay vì `median_csat`. |
| 6 | **NPS** · Mixed | 🔁 Bản rút gọn D39-11 | Không có đường KPI. |
| 7 | **CSAT CHẤT LƯỢNG** · Mixed | 🔁 Giống D39-12 | |

**Prompt cho chart 4 (chart riêng của trang này):**

> Tạo Mixed chart trên Lightdash từ model `production_management_1_kpikqi_service_tkcg` join `production_management_1_dim_date`: trục X là `quarter_year` → `month_year` → `week_year` (drill được) kết hợp `product`, giá trị là metric `average_latest_version_coverage_tkcg`. Đặt tiêu đề "ĐỘ PHỦ PHIÊN BẢN MỚI NHẤT".

**Ý nghĩa chart 4:** Khác D39-3 ở chỗ nhìn theo **trục thời gian** (quý → tháng → tuần) thay vì theo phiên bản. Dùng để xem tốc độ khách hàng nâng cấp agent có đang nhanh lên không.

---

## D40 — TỔNG QUAN

**Mục đích:** Trang chủ điều hành của **toàn công ty**. Gom cả ba mảng vào một màn hình: KPI khách hàng (4 nhóm), sức khỏe sản xuất, chất lượng, và kế hoạch release.

**Bộ lọc trang:** năm, khối, tuần, tháng, quý, chu kỳ báo cáo, chỉ tiêu, tên project, SPDV, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **KPIKQI - SP KHÁCH HÀNG VVIP** · Big value ×3 | `_kpikqi_new`<br>⚠️`tong_customer`, ⚠️`tong_khach_passed`, ⚠️`tong_khach_khong_passed`<br>Lọc: `customer_type = 'VVIP'`, KH ngoài | Tổng / đạt / không đạt cho nhóm khách VVIP. | Tạo 3 Big value từ `production_management_1_kpikqi_new`, lọc `customer_type = 'VVIP'` và `customer_group = 'KHÁCH HÀNG NGOÀI'`: `tong_customer` ("TỔNG KH"), `tong_khach_passed` ("TỔNG ĐẠT"), `tong_khach_khong_passed` ("TỔNG KHÔNG ĐẠT"). Cụm tiêu đề "KPIKQI - SP KHÁCH HÀNG VVIP". |
| 2 | **KPIKQI - SP KHÁCH HÀNG VIP** · Big value ×3 | Như chart 1, lọc `customer_type = 'VIP'` | Bản VIP của chart 1. | 🔁 Như chart 1, đổi bộ lọc thành `customer_type = 'VIP'`. Cụm tiêu đề "KPIKQI - SP KHÁCH HÀNG VIP". |
| 3 | **KPIKQI - SP KHÁCH HÀNG VIETTEL** · Big value ×3 | Như chart 1, lọc `customer_group = 'Viettel'` | Bản Viettel (khách nội bộ tập đoàn). | 🔁 Như chart 1, đổi bộ lọc thành `customer_group = 'Viettel'`. Cụm tiêu đề "KPIKQI - SP KHÁCH HÀNG VIETTEL". |
| 4 | **KPIKQI - SP KHÁCH HÀNG SOC PLATFORM** · Big value ×3 | Như chart 1, lọc `customer_group = 'SOCP'` | Bản SOC Platform. | 🔁 Như chart 1, đổi bộ lọc thành `customer_group = 'SOCP'`. Cụm tiêu đề "KPIKQI - SP KHÁCH HÀNG SOC PLATFORM". |
| 5 | 🐍 **CÁC SẢN PHẨM THƯỜNG XUYÊN RELEASE** · Python visual | 🔁 Giống D23-5 | Bong bóng 5 chiều cho 8 project release đều. | 🔁 Dùng lại cấu hình Scatter chart của D23-5. |
| 6 | 🐍 **SỨC KHỎE TOÀN VCS** · Python visual | Như chart 5, thêm dimension `division` (Khối)<br>Lọc: `division = 'SOC'` | Bong bóng ở cấp **khối** thay vì cấp project. Mỗi điểm là một khối, giúp lãnh đạo so sánh đơn vị. | Thay Python visual bằng **Scatter chart**: trục X là metric `average_release_frequency`, trục Y là `production_bug_rate`, kích thước điểm là `hieu_suat_load_nguoi`, mỗi điểm là một `division`. Thêm `average_days_to_done` và `rate_defect` vào tooltip. Đặt tiêu đề "SỨC KHỎE TOÀN VCS". |
| 7 | **KPIKQI SẢN PHẨM** · Mixed (bar chồng) | `_kpikqi_new`<br>Trục X `customer_group` + `spdv` · Giá trị ✅`tong_count_sp_fixed` · Nhóm màu `evaluation` | 🔁 Như D7-4 nhưng cho toàn bộ nhóm khách, không lọc SOCP/Viettel. | Tạo Mixed chart cột chồng từ `production_management_1_kpikqi_new`: trục X `customer_group` + `spdv` (tăng dần), giá trị `tong_count_sp_fixed`, nhóm màu `evaluation`, bật Stack. Đặt tiêu đề "KPIKQI SẢN PHẨM". |
| 8 | **KPIKQI DỊCH VỤ** · Mixed (bar chồng) | `_kpikqi_all`<br>Trục X `spdv` · Giá trị ⚠️`tong_count_dv_fixed` · Nhóm màu `evaluation` | 🔁 Như D43-3. | 🔁 Dùng lại chart "KPIKQI DỊCH VỤ" của D43-3. |
| 9 | **TỔNG QUAN TRUNG TÂM** · Mixed (bar + line) | `_cycle_time` + `_done_charter` + `_project_vs_department` + `_project_name`<br>Trục X `project_name_last`<br>Cột: ✅`average_days_to_done`, ✅`count_distinct_issue_key`<br>Đường (trục phải): ✅`hoan_thanh_ticket` (%), ⚠️`bug_production_thanh_cong` (%) | Bốn chỉ số của cùng một project đặt cạnh nhau: **làm nhanh không** (cycle time), **làm nhiều không** (số story), **làm xong không** (% hoàn thành ticket), **làm sạch không** (% bug production xử lý thành công). | Tạo Mixed chart gộp `production_management_1_cycle_time`, `_done_charter`, `_project_vs_department`, `_project_name`: trục X `project_name_last`. Cột trục trái: metric `average_days_to_done` (sắp xếp giảm dần) và `count_distinct_issue_key`. Đường trục phải: `hoan_thanh_ticket` và `bug_production_thanh_cong`, cả hai định dạng phần trăm. Đặt tiêu đề "TỔNG QUAN TRUNG TÂM". |
| 10 | **PRODUCTION BUG** · Bar | `_bug_production` + `_division`<br>Trục X `issue_priority` · Giá trị ✅`count_bug_production` · Nhóm màu `division` | Bug production chia theo mức ưu tiên, mỗi màu là một khối. Nhìn ngay khối nào để lọt nhiều lỗi nặng. | Tạo Bar chart từ `production_management_1_bug_production` join `production_management_1_division`: trục X `issue_priority`, giá trị metric `count_bug_production` (sắp xếp giảm dần), nhóm màu theo `division`, bật Stack. Đặt tiêu đề "PRODUCTION BUG". |
| 11 | **BUG NGHIỆM THU TTSX** · Bar | `_bug_os` + `_dim_date`<br>Trục X `month_year` / `quarter_year` · Giá trị ✅`count_bug` · Nhóm màu `issue_current_status_name` | Bug phát hiện trong khâu nghiệm thu OS, theo tháng và trạng thái xử lý. Metric đã loại sẵn bug bị `Cancelled`. | Tạo Bar chart từ `production_management_1_bug_os` join `production_management_1_dim_date`: trục X `month_year` (tăng dần) + `quarter_year`, giá trị metric `count_bug`, nhóm màu theo `issue_current_status_name`, bật Stack. Đặt tiêu đề "BUG NGHIỆM THU TTSX". |
| 12 | **CHẤT LƯỢNG VÀ TẦN SUẤT RELEASE** · Mixed (bar + line) | `_project_vs_department` + `_kpikqi_customer` + `_division` + `_dim_date`<br>Trục X `division` + tuần/tháng<br>Cột: ⚠️`release_frequency_dgcl_ttk`<br>Đường (trục phải): ✅`count_distinct_spdv` | Ghép **tần suất release** với **số sản phẩm khối đó phụ trách**. Khối phụ trách nhiều sản phẩm mà tần suất release vẫn tốt là khối đang chạy hiệu quả. | Tạo Mixed chart gộp `production_management_1_project_vs_department`, `_kpikqi_customer`, `_division` join `_dim_date`: trục X `division` (tăng dần) + `week_year`/`month_year`. Cột trục trái: metric `release_frequency_dgcl_ttk`. Đường trục phải: metric `count_distinct_spdv` (nhãn "Số SP phụ trách"). Đặt tiêu đề "CHẤT LƯỢNG VÀ TẦN SUẤT RELEASE". |
| 13 | 🐍 **RELEASE PLAN BY VERSION** · Python visual | `_milestone_versions`<br>Cột: `version_name`, `version_des_change`, `version_start_date`, `version_release_date` | Biểu đồ **Gantt** kế hoạch release: mỗi version là một thanh từ ngày bắt đầu đến ngày release. Lightdash không có Gantt. | Lightdash không hỗ trợ Gantt. **Thay bằng Table** từ `production_management_1_milestone_versions` với các cột `version_name`, `version_des_change`, `version_start_date`, `version_release_date`, sắp xếp theo `version_start_date`, kèm cột tính "số ngày" = `version_release_date - version_start_date`. Nếu cần trực quan, dùng Bar chart ngang với trục X là số ngày và trục Y là `version_name`. Đặt tiêu đề "RELEASE PLAN BY VERSION". |

---

## D44 — QTSX TRUNG TÂM

**Mục đích:** Sức khỏe sản xuất nhìn theo **trung tâm / khối** thay vì theo project. Dùng khi giám đốc trung tâm cần biết đơn vị mình đang đứng ở đâu so với các đơn vị khác.

**Bộ lọc trang:** khối, tên project, tháng, khoảng ngày, quý, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **CYCLE TIME** · Mixed (bar + line) | `_cycle_time` + `_project_name`<br>Trục X `project_name_last`<br>Cột: ✅`average_days_to_done`<br>Đường (trục phải): ⚠️`target` | Cycle time từng project so với **ngưỡng mục tiêu theo quý**. Cột vượt đường là project cần can thiệp. | Tạo Mixed chart từ `production_management_1_cycle_time` join `production_management_1_project_name`: trục X `project_name_last`. Cột trục trái: metric `average_days_to_done` (sắp xếp giảm dần). Đường trục phải: metric `target` (nhãn "Target theo quý"). Đặt tiêu đề "CYCLE TIME". |
| 2 | **PYC OS** · Bar | `_pyc_jira` + `_dim_date`<br>Trục X tháng / quý / tuần · Giá trị ✅`count_issue_key` · Nhóm màu `issue_current_status_name`<br>Lọc: bỏ `canceled` | 🔁 Như D10-1 nhưng đếm bằng `count_issue_key` (metric sinh tự động) thay vì metric DAX. Đây là bản dễ dựng hơn — nên dùng bản này. | Tạo Bar chart từ `production_management_1_pyc_jira` join `production_management_1_dim_date`: trục X `month_year` (tăng dần) + `quarter_year` + `week`, giá trị metric `count_issue_key` (nhãn "Số phiếu"), nhóm màu `issue_current_status_name`, bật Stack, loại bỏ trạng thái `canceled`. Đặt tiêu đề "PYC OS". |
| 3 | **TỈ LỆ REWORK** · Mixed | `_project_name`<br>Trục X `project_name_last`<br>**Thiếu metric** | ⚠️ **Chart lỗi trong file gốc** — chỉ có trục X, không có giá trị nào. Khi dựng lại cần bổ sung metric tỷ lệ rework (phần công phải làm lại do lỗi). | Chart gốc bị thiếu metric. Khi dựng lại, tạo Mixed chart từ `production_management_1_project_name` join model chứa dữ liệu rework: trục X `project_name_last` (tăng dần), giá trị là **tỷ lệ rework** — cần định nghĩa metric mới, ví dụ `SUM(worklog_mm WHERE issue_type = 'Bug') / SUM(worklog_mm)`. Đặt tiêu đề "TỈ LỆ REWORK". |
| 4 | **TẦN SUẤT RELEASE THEO SẢN PHẨM** · Mixed (bar + line) | `_project_vs_department` + `_dgcl_ttk` + `_project_name` + `_division`<br>Trục X `project_name_last` · Nhóm màu `division`<br>Cột: ⚠️`release_frequency_dgcl_ttk`<br>Đường (trục phải): ✅`count_distinct_issue_key` | Cột là **số ngày giữa hai lần release**, đường là **số lần cập nhật**. Sản phẩm cột thấp + đường cao là sản phẩm ra hàng đều nhất. | Tạo Mixed chart gộp `production_management_1_project_vs_department`, `_dgcl_ttk`, `_project_name`, `_division`: trục X `project_name_last`, nhóm màu theo `division`. Cột trục trái: metric `release_frequency_dgcl_ttk` (sắp xếp giảm dần). Đường trục phải: metric `count_distinct_issue_key` (nhãn "Số update"). Đặt tiêu đề "TẦN SUẤT RELEASE THEO SẢN PHẨM". |
| 5 | **PRODUCTION BUG** · Bar | `_bug_production`<br>Trục X `issue_priority` · Giá trị ✅`count_bug_production`<br>Lọc: priority ∈ (Highest, Low, Lowest) | Bug production theo mức ưu tiên. **Lưu ý**: bộ lọc gốc bỏ mất mức `High` và `Medium` — nhiều khả năng là lọc sót, cần rà lại. | Tạo Bar chart từ `production_management_1_bug_production`: trục X `issue_priority`, giá trị metric `count_bug_production`, sắp xếp giảm dần. **Nên bỏ bộ lọc priority của bản Power BI** (bản gốc chỉ giữ Highest/Low/Lowest, thiếu High và Medium). Đặt tiêu đề "PRODUCTION BUG". |
| 6 | **PHÂN BỔ VÀ HIỆU QUẢ XỬ LÝ BUG PRODUCTION NĂM 2025** · Mixed (bar + line) | `_bug_production` + `_project_vs_department` + `_division`<br>Trục X `project_name` · Nhóm màu `division`<br>Cột: ✅`count_bug_production`<br>Đường (trục phải): ⚠️`bug_production_thanh_cong` (%) | Cột là **số bug**, đường là **tỷ lệ xử lý thành công**. Project nhiều bug nhưng tỷ lệ xử lý cao vẫn tốt hơn project ít bug mà xử lý ì ạch. | Tạo Mixed chart gộp `production_management_1_bug_production`, `_project_vs_department`, `_division`: trục X `project_name`, nhóm màu theo `division`. Cột trục trái: metric `count_bug_production` (sắp xếp giảm dần). Đường trục phải: metric `bug_production_thanh_cong` định dạng phần trăm. Đặt tiêu đề "PHÂN BỔ VÀ HIỆU QUẢ XỬ LÝ BUG PRODUCTION". |
| 7 | **BUG NGHIỆM THU TTSX THEO PROJECT** · Mixed (bar chồng) | `_bug_os` + `_project_name`<br>Trục X `project_name_last`<br>Giá trị: 5 metric theo trạng thái — ✅`count_acceptance_bug_thanh_cong`, ✅`count_acceptance_bug_that_bai`, ✅`count_bug_san_sang_acceptance`, ✅`count_bug_todo`, ✅`count_bug_in_progress` | Với mỗi project, bug nghiệm thu đang nằm ở trạng thái nào: chờ làm (TO DO), đang làm, sẵn sàng nghiệm thu, nghiệm thu thành công, nghiệm thu thất bại. Đây là **phễu xử lý bug** — chỗ nào phình to là chỗ tắc. | Tạo Mixed chart dạng cột chồng từ `production_management_1_bug_os` join `production_management_1_project_name`: trục X `project_name_last`, năm cột giá trị theo thứ tự phễu: `count_bug_todo` ("TO DO"), `count_bug_in_progress` ("IN PROGRESS"), `count_bug_san_sang_acceptance` ("SẴN SÀNG NGHIỆM THU"), `count_acceptance_bug_thanh_cong` ("NGHIỆM THU THÀNH CÔNG"), `count_acceptance_bug_that_bai` ("NGHIỆM THU THẤT BẠI"). Bật Stack. Đặt tiêu đề "BUG NGHIỆM THU TTSX THEO PROJECT". |

---

## D45 — Chi tiết

**Mục đích:** Bảng tổng hợp "một dòng một project" — tương đương D7 của báo cáo QTSX V2. Mọi chỉ số quan trọng của mảng C nằm trên cùng một hàng để tiện chấm điểm và xếp hạng.

**Bộ lọc trang:** năm, tháng, quý, tên project, khối, mức ưu tiên bug, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **QUẢN TRỊ SẢN XUẤT CÔNG TY** · Table (pivot) | Hàng: `division`, `project_name_last` · Cột: `month_year`, `year_value`<br>Giá trị (16 cột): ✅`average_release_frequency` (TS release), ✅`count_distinct_issue_key` (số story), ✅`average_days_to_done` (cycle time), ✅`sum_man_month` (MM), ✅`sum_worklog_mm`, ⚠️`worklog` (%), ✅`sum_ulnl_mm`, ⚠️`hieu_suat_load_nguoi` (%), ✅`count_bug_prd`, ⚠️`production_bug_rate` (%), ⚠️`rate_defect` (%), ✅`mm_worklog_release`, ⚠️`mm_dev_team`, ⚠️`mm_fix_bug_release` (%) | Bảng khám sức khỏe đầy đủ nhất của mảng C. Đọc theo cụm: **tiến độ** (TS release, cycle time), **khối lượng** (số story, MM, worklog, ULNL), **hiệu suất** (% worklog, % load người), **chất lượng** (bug production, defect), **cơ cấu công** (MM dev team, % fix bug). | Tạo Table pivot trên Lightdash gộp `production_management_1_project_name`, `_dgcl_ttk`, `_cycle_time`, `_mamday_hr`, `_worklogs`, `_ulnl`, `_ulnl_projects`, `_bug_production`, `_defect`, `_mm_release_versions`, `_mmrelease` join `production_management_1_dim_date`. Hàng: `division` (sắp xếp tăng dần), `project_name_last`. Cột: `year_value`, `month_year`. Cột giá trị: `average_release_frequency`, `count_distinct_issue_key`, `average_days_to_done`, `sum_man_month`, `sum_worklog_mm`, `worklog` (%), `sum_ulnl_mm`, `hieu_suat_load_nguoi` (%), `count_bug_prd`, `production_bug_rate`, `rate_defect`, `mm_worklog_release`, `mm_dev_team`, `mm_fix_bug_release` (%). Đặt tiêu đề "QUẢN TRỊ SẢN XUẤT CÔNG TY". |

---

## D46 — TTSX

**Mục đích:** Nhìn bug nghiệm thu và hiệu quả sản xuất theo **trung tâm**, dùng bảng chiều `project_vs_department` (ánh xạ sản phẩm ↔ trung tâm).

**Bộ lọc trang:** khối, tuần, tháng, khoảng ngày, chu kỳ báo cáo, khách hàng.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **PYC OS** · Bar | ❌ **Không map được bảng nguồn** | ⚠️ Chart này trong file gốc trỏ tới bảng đã bị xóa khỏi mô hình. Khi dựng lại nên **thay bằng chart D44-2** (cùng nội dung, có dữ liệu). | 🔁 Dùng lại chart "PYC OS" của D44-2. |
| 2 | **SỐ BUG TB/1 ĐƠN HÀNG** · Pie (donut) | `_project_vs_department`<br>Lát cắt `division` · Giá trị ⚠️`count_bug_order` | Trung bình mỗi đơn hàng phát sinh bao nhiêu bug, chia theo trung tâm. Chỉ số này quy chuẩn theo quy mô nên so sánh giữa các trung tâm được. | Tạo Pie chart (bật Donut) từ `production_management_1_project_vs_department`: lát cắt theo dimension `division`, giá trị là metric `count_bug_order`. Đặt tiêu đề "SỐ BUG TB/1 ĐƠN HÀNG". |
| 3 | **BUG NGHIỆM THU THEO THÁNG** · Bar | `_bug_os` + `_project_vs_department`<br>Trục X `project_name` · Giá trị ✅`count_bug` | Số bug nghiệm thu theo project. **Tên chart nói "theo tháng" nhưng trục X là project** — cần bổ sung trục thời gian khi dựng lại. | Tạo Bar chart từ `production_management_1_bug_os` join `production_management_1_project_vs_department`: trục X `project_name`, giá trị metric `count_bug`. **Bổ sung** dimension `month_year` vào trục X để khớp với tên chart. Đặt tiêu đề "BUG NGHIỆM THU THEO THÁNG". |
| 4 | **TẦN SUẤT RELEASE VÀ HIỆU QUẢ** · Mixed (bar + line) | `_project_vs_department` + `_done_charter`<br>Trục X `division`<br>Cột: ⚠️`release_frequency_dgcl_ttk`<br>Đường (trục phải): ✅`hoan_thanh_ticket` (%) | Cột là tần suất release, đường là % ticket hoàn thành. Trung tâm release đều mà ticket vẫn đóng kịp là trung tâm vận hành tốt. | Tạo Mixed chart từ `production_management_1_project_vs_department` join `production_management_1_done_charter`: trục X `division`. Cột trục trái: metric `release_frequency_dgcl_ttk`. Đường trục phải: metric `hoan_thanh_ticket` định dạng phần trăm. Đặt tiêu đề "TẦN SUẤT RELEASE VÀ HIỆU QUẢ". |
| 5 | **SỐ BUG THEO TRUNG TÂM** · Pie (donut) | `_bug_os` + `_project_vs_department`<br>Lát cắt `division` · Giá trị ✅`count_bug` | Cơ cấu bug nghiệm thu theo trung tâm (số tuyệt đối, khác chart 2 là số bình quân). | Tạo Pie chart (Donut) từ `production_management_1_bug_os` join `production_management_1_project_vs_department`: lát cắt `division`, giá trị metric `count_bug`. Đặt tiêu đề "SỐ BUG THEO TRUNG TÂM". |
| 6 | **PHÂN BỔ VÀ HIỆU QUẢ XỬ LÝ BUG PRODUCTION** · Mixed (bar + line) | `_bug_production` + `_project_vs_department` + `_done_charter` + `_dim_date`<br>Trục X `division` + tuần/tháng<br>Cột: ✅`count_bug_production`<br>Đường (trục phải): ⚠️`rate_acceptance_bug_thanh_cong`, ⚠️`bug_production_thanh_cong`, ✅`hoan_thanh_ticket` | Bản theo trung tâm của D44-6, có thêm hai đường: tỷ lệ bug nghiệm thu thành công và % hoàn thành ticket. Ba đường cùng lúc hơi dày — cân nhắc tách bớt khi dựng lại. | Tạo Mixed chart gộp `production_management_1_bug_production`, `_project_vs_department`, `_done_charter` join `_dim_date`: trục X `division` + `week_year`/`month_year`. Cột trục trái: metric `count_bug_production`. Đường trục phải: `bug_production_thanh_cong`, `rate_acceptance_bug_thanh_cong`, `hoan_thanh_ticket`, tất cả định dạng phần trăm. Đặt tiêu đề "PHÂN BỔ VÀ HIỆU QUẢ XỬ LÝ BUG PRODUCTION". |

---

## Gợi ý gom lại khi dựng trên Lightdash

Báo cáo gốc 49 trang có thể rút xuống khoảng **12–14 dashboard** mà không mất nội dung nào:

| Dashboard đề xuất | Gộp từ | Lý do |
|---|---|---|
| **1. Tổng quan điều hành** | D40 | Giữ nguyên, thay 2 Python visual bằng Scatter chart |
| **2. KPIKQI theo nhóm khách** | D7 + D8 + D35 + D41 + D42 | Bốn trang này cùng một bố cục, chỉ khác bộ lọc nhóm khách. Dựng **một dashboard** rồi thêm dashboard filter `customer_type` / `customer_group`. Nhớ mang theo 2 chart riêng của D42 (bảng theo tháng và phân loại nguyên nhân lỗi). |
| **3. Chi tiết KPIKQI** | D3 + D14 + D15–D17 + D24–D34 + D37 + D38 + D47 + D6 | **19 trang gộp thành 1.** Một saved table + 3 dashboard filter (`spdv`, `customer_type`, `customer_group`). Đây là chỗ tiết kiệm công sức lớn nhất. |
| **4. KPIKQI dịch vụ** | D43 | Giữ riêng vì dùng model `kpikqi_all` khác hẳn |
| **5. KPIKQI Tập đoàn** | D5 | Giữ riêng vì target và cách đánh giá khác bản nội bộ |
| **6. KPIKQI SOC** | D9 | Giữ riêng vì dùng model `soc_results` với chỉ tiêu đặc thù |
| **7. Xu hướng & nguyên nhân** | D19 + D20 + D13 | Ba trang bổ trợ nhau: ai đang xấu đi, vì sao, xử lý thế nào |
| **8. Nguồn lực OS** | D2 + D10 | Chọn **một** nguồn số liệu (`pyc_snapshot` hoặc `pyc_jira`) rồi bỏ trang còn lại. Nếu cần cả hai thì để thành 2 tab và ghi rõ khác biệt trên tiêu đề. |
| **9. Độ ổn định kế hoạch OS** | D1 | Giữ riêng — nội dung không trùng trang nào |
| **10. Thanh toán OS** | D11 + D0 | Cùng chủ đề tiền và đối tác |
| **11. Sức khỏe project** | D23 + D44 + D45 | Ba trang cùng nói về project/trung tâm, gộp thành 3 tab |
| **12. Man-month & release version** | D4 + D12 + D18 + D21 | Cùng xoay quanh fix version và cơ cấu man-month |
| **13. Chất lượng dịch vụ (CLDV)** | D39 (bỏ D36, giữ chart 4 của D36) | D36 là tập con của D39 |
| **14. Đối soát số liệu** | D22 + D46-1 | Trang kỹ thuật, chỉ mở khi nghi ngờ số liệu |

**Bỏ hẳn:** D6, D21, D47, D48 (bảng thô, thay bằng nút Export); D34, D37 (trùng trang khác); D46-1 (chart lỗi).

**Cần sửa trước khi dựng:**

| Chart | Vấn đề |
|---|---|
| D1-6 PHÂN BỔ SỐ LẦN ĐIỀU CHỈNH PYC | Thiếu dimension lát cắt, pie chỉ có một miếng |
| D23-6 TỶ LỆ DEFECT BY PROJECT | Thiếu dimension project dù tên chart nói "by project" |
| D44-3 TỈ LỆ REWORK | Thiếu hoàn toàn metric |
| D44-5 PRODUCTION BUG | Bộ lọc priority bỏ sót mức High và Medium |
| D46-1 PYC OS | Trỏ tới bảng đã bị xóa khỏi mô hình |
| D46-3 BUG NGHIỆM THU THEO THÁNG | Thiếu trục thời gian dù tên chart nói "theo tháng" |






