# QTSX V2 — Mô tả dashboard, chart và prompt tạo trên Lightdash

> Nguồn: `powerbi_output/qtsx_v2/`
> Quy mô: **14 trang · 133 chart · 57 bộ lọc · 34 bảng dữ liệu**
> Ký hiệu ⚠️ ✅ 🔁 🐍 xem ở [00_huong_dan_va_quy_uoc_prompt.md](00_huong_dan_va_quy_uoc_prompt.md)

## Báo cáo này nói về cái gì

Đây là bộ báo cáo **quản trị sản xuất phần mềm của VCS**, nhìn theo 4 trục sức khỏe:

1. **Cam kết KPI/KQI với khách hàng** — sản phẩm dịch vụ (SPDV) nào đạt, khách hàng nào đạt, đạt bao nhiêu %.
2. **Tiến độ** — tần suất release, cycle time, lead time.
3. **Chất lượng** — production bug, defect, bug rate.
4. **Hiệu suất & nguồn lực** — man-month bỏ ra so với man-month tạo ra giá trị (worklog, ULNL, busy rate).

Cộng thêm một nhánh riêng cho **OS (outsourcing)** — đo phiếu yêu cầu (PYC) giao cho đối tác ngoài.

## Bản đồ 14 dashboard

| # | Dashboard | Chart | Vai trò |
|---|---|---|---|
| [D0](#d0--tổng-quan) | Tổng quan | 32 | Trang chủ điều hành — tất cả KPI đầu ngón tay |
| [D1](#d1--kpikqi--value) | KPIKQI - VALUE | 14 | Bóc tách KPI/KQI theo khách hàng, SPDV, trung tâm |
| [D2](#d2--chi-tiết-kpikqi) | CHI TIẾT KPIKQI | 2 | Bảng chi tiết từng chỉ tiêu KPI sản phẩm / dịch vụ |
| [D3](#d3--tiến-độ--progress) | Tiến độ - Progress | 21 | Release / cycle time / lead time |
| [D4](#d4--chất-lượng--quality) | Chất lượng - Quality | 19 | Production bug / defect / bug |
| [D5](#d5--hiệu-suất--nguồn-lực) | Hiệu suất & Nguồn lực | 16 | EE, năng suất, busy rate, load người |
| [D6](#d6--chỉ-tiêu-os) | Chỉ tiêu OS | 12 | PYC đúng hạn, bug OS, hiệu quả OS |
| [D7](#d7--chi-tiết) | CHI TIẾT | 3 | Bảng tổng hợp mọi chỉ số theo project |
| [D8](#d8--chi-tiết-version-report) | Chi tiết version report | 2 | Defect theo version |
| [D9](#d9--tooltip-sk) | Tooltip SK | 1 | Bảng bung ra khi hover chart KPI sản phẩm |
| [D10](#d10--tooltip-dv) | Tooltip DV | 1 | Bảng bung ra khi hover chart KPI dịch vụ |
| [D11](#d11--pyc) | PYC | 1 | Danh sách phiếu yêu cầu OS |
| [D12](#d12--page-review) | Page review | 6 | Trang đối soát số liệu OS |
| [D13](#d13--page-1) | Page 1 | 3 | Trang nháp đối soát MM release |

## Model dbt hay dùng nhất

| Model | Nội dung | Xuất hiện ở |
|---|---|---|
| `qtsx_v2_kpikqi_nocodb` | Kết quả KPI/KQI theo khách hàng + SPDV (nguồn NocoDB) | D0, D1, D2, D9 |
| `qtsx_v2_kpikqi_all` | Kết quả KPI/KQI cấp dịch vụ, hợp nhất nhiều nguồn | D0, D1, D2, D10 |
| `qtsx_v2_dgcl_ttk` | Đánh giá chất lượng release của Trung tâm Thiết kế | D0, D3, D7 |
| `qtsx_v2_cycel_time` / `qtsx_v2_lead_time` | Số ngày từ start → done của user story | D0, D3, D7 |
| `qtsx_v2_bug_productions` / `qtsx_v2_defect` / `qtsx_v2_bug` | 3 tầng lỗi: lỗi trên production, defect nội bộ, bug chung | D0, D4, D7 |
| `qtsx_v2_worklog` / `qtsx_v2_issue_ulnl` / `qtsx_v2_man_month_qa` | Công thực tế, ước lượng nguồn lực, man-month kế hoạch | D5, D7 |
| `qtsx_v2_pyc_os`, `qtsx_v2_pyc_os_thnt`, `qtsx_v2_pyc_os_done_release` | Phiếu yêu cầu OS ở 3 trạng thái: tất cả / đã nghiệm thu / đã release | D6, D11, D12, D13 |
| `qtsx_v2_project`, `qtsx_v2_dim_date`, `qtsx_v2_dim_status` | Bảng chiều: dự án–trung tâm, thời gian, trạng thái | Mọi trang |

## Cảnh báo trước khi bắt tay

- **129 DAX measure chưa dịch tự động được** (ký hiệu ⚠️). Đợt 2026-08 đã viết tay xong nhóm `priority ≥ 2` — xem [Trạng thái metric](#trạng-thái-metric-cập-nhật-2026-08) ngay dưới để biết metric nào đã dùng được và nằm ở model nào. Riêng nhóm `*_text`, `*_display`, `*_growth_text` là các measure Power BI trả về **chuỗi đã format sẵn** (ví dụ `"12.5% ▲"`). Chúng **không** được dựng lại thành metric — trên Lightdash chỉ tạo metric số nền rồi format ở tầng chart.
- **`qtsx_v2_dim_date` trước đây rỗng.** `Dim_Date` là bảng DAX `CALENDAR()`; VertiPaq không lưu dữ liệu cột sinh bởi `ADDCOLUMNS` nên `Date`, `Year`, `MonthYear`, `QuarterYear`, `WeekDisplay`… trong `Dim_Date.tsv` (2 222 dòng) đều trống — chỉ `WeekLabel`, `YearMonth`, `IsWorkingDay` có giá trị. 35 lượt chart dùng `date_value` làm trục thời gian nên không dựng được. **Đã vá** bằng `powerbi_output/qtsx_v2/sql_overrides/qtsx_v2_dim_date.sql` (dựng lại toàn bộ lịch bằng SQL, đã đối chiếu khớp 2 222/2 222 dòng với dữ liệu export).
- **`qtsx_v2_kpikqi_all` mới phủ 73% dữ liệu.** Cũng là bảng DAX (`UNION` của 3 nguồn); 12 cột đầu rỗng trong export. `sql_overrides/qtsx_v2_kpikqi_all.sql` dựng lại được 5 935/8 122 dòng (đã đối chiếu, lệch 12 dòng = 0,20%). **Thiếu 2 187 dòng của Trung tâm `TT.SI`** vì nhánh `KPIKQI_TKCG_Report` còn phụ thuộc 2 tầng bảng tính toán nữa và 4 measure chưa dịch (`[Độ phủ]`, `[Độ trễ]`, `[ĐÁNH GIÁ CR]`…). Chart lọc theo TT.SI sẽ ra rỗng — chi tiết trong header file SQL.
- **9 chart lấy trục X từ auto date table** của Power BI (`status: auto_date_table` — bảng `LocalDateTable_*` sinh tự động trên `Dim_Date[Date]`), nằm ở trang *Hiệu suất & Nguồn lực* và *Tiến độ - Progress*. Sau khi `dim_date` đã được vá, trỏ thẳng vào `qtsx_v2_dim_date.date_value`.
- **Card không có tiêu đề** trong Power BI rất nhiều — chúng là các ô phụ nằm ngay dưới card chính để hiện "so với kỳ trước", "số project đạt/không đạt". Trong bảng dưới đây đã ghi rõ card đó thuộc về KPI nào.
- **Bộ lọc TopN theo `YearMonth`/`WeekYear`** nghĩa là "chỉ lấy kỳ gần nhất". Đây là bộ lọc xuất hiện nhiều nhất trong cả báo cáo.

## Trạng thái metric *(cập nhật 2026-08)*

Metric viết tay để ở `powerbi_output/qtsx_v2/metrics_overrides.yml` và các model viết tay
trong `dbt/models/`; `powerbi_tools/run.py` merge vào `*__schema.yml` nên chạy lại không mất.

**Đã làm xong toàn bộ measure `priority ≥ 1`** — tức mọi measure đang được ít nhất một
chart sử dụng (62 measure). Độ phủ: **119/187 lượt tham chiếu metric (64%)**; 41 tên còn
lại **toàn bộ** là chuỗi hiển thị / màu / tooltip cố ý không dựng (xem bảng cuối mục) hoặc
đã đổi tên sang metric số.

**Model trung gian viết tay — `qtsx_v2_mm_ratio_by_project_month` (34 metric).** Gộp
Bug / Defect / bug productions / cycel time / Lead time / ĐGCL TTK / Issue ULNL / worklog /
ULNL_worklog_by_version_release / bug OS / PYC OS / Project / Man_month_QA
về grain **(YearMonth × sản phẩm)**.
Lý do: mọi measure `multi_table_agg` đều chia cho `SUM(Man_month_QA[man month])`, mà
khai báo join thẳng giữa hai bảng chi tiết sẽ **fan-out** (mỗi dòng bug nhân với mỗi
dòng man-month cùng sản phẩm/tháng) làm mẫu số bị thổi lên. Gộp trước rồi mới chia thì
không còn khả năng nhân bản dòng. Grain này cũng đúng luôn cho các measure đếm
`COUNTROWS(FILTER(SUMMARIZE(<bảng>, [projectname], Dim_Date[YearMonth]), … > ngưỡng))`.

| Metric mới | Thay cho measure Power BI |
|---|---|
| `kpi_mm_per_bug`, `kpi_mm_per_defect` | `KPI MM per bug`, `KPI MM per Defect`, `Bug` |
| `hieu_su_dung_source_luc_ee` | `Hiệu quả sử dụng nguồn lực EE` |
| `nang_xuat_lao_dong`, `pct_worklog_by_version` | `Năng xuất lao động`, `% Worklog by version` |
| `bug_project_pass` / `_fail`, `defect_project_pass` / `_fail` | `Bug Project Pass/Fail show`, `Defect Project Pass/Fail show` |
| `average_days_to_done`, `cyceltime_pass` / `_fail` | `Cycel time AVG text`, `Cyceltime Pass/Fail` |
| `average_days_to_done_v2`, `leadtime_pass` / `_fail` | `Lead time AVG text`, `Leadtime pass/Fail` |
| `release_kpi`, `spdv_pass_release` / `_fail_release` | `Release KPI`, `SPDV Pass/Fail (Release)` |
| `bug_per_project`, `pass_projects_count`, `fail_projects_count` | `Bug per Project`, `Pass/Fail Projects` |
| `pct_bug`, `pct_defect`, `rate_bug_p` | `% bug`, `% defect`, `Tỉ lệ Bug P` |
| `hs_load_nguoi_by_versionrelease` | `HS_load_nguoi_by_versionrelease` |
| `total_log_hours`, `standard_capacity_hours`, `busy_rate` | `Total Log Hours`, `Standard Capacity Hours`, `Busy Rate` |
| `tong_he_so_bug`, `mm_thuc_te_os`, `ti_le_bug_mm` | `Tong_He_So_Bug`, `MM_Thuc_Te`, `Ti_Le_Bug_MM` |

Model này **không vô hình với pipeline**: `run.py` mỗi lần chạy đều kiểm `ref()` trỏ đúng
model, `source()` trỏ đúng bảng silver, và `sql` của metric chỉ dùng cột đã khai — sai chỗ
nào in ra `! model viết tay: …` và ghi vào `report.md` (mục *Model viết tay*).

**Metric mới trên model gốc** (khai trong `metrics_overrides.yml`):

| Model | Metric | Thay cho |
|---|---|---|
| `qtsx_v2_pyc_os` | `so_pass`, `pct_on_time`, `pct_release`, `pct_hieu_su_dung_os` | `Số Pass`, `% Đúng hạn` / `% Pass Rate` / `Pass Rate Display`, `% release`, `Hiệu quả sử dụng OS` |
| `qtsx_v2_kpikqi_all` | `spdv_pass_count_all`, `spdv_fail_count_all`, `spdv_fail_count_all_evaluation` | `SPDV Count all`, và phần số nền của `SPDV Status all` / `SPDV Status all color` |
| `qtsx_v2_pyc_os_thnt` | `pct_release_pyc` | `% release PYC` |

**Đổi tên** (measure gốc trả về chuỗi hoặc phụ thuộc slicer `Dim_Status`):

| Measure Power BI | Metric Lightdash | Ghi chú |
|---|---|---|
| `Customer Count Nocodb` | `customer_pass_count` + `customer_fail_count` | DAX so với `SELECTEDVALUE(Dim_Status[Status])`; Lightdash không có bảng slicer rời nên tách đôi |
| `SPDV Count Nocodb` | `spdv_pass_count` + `spdv_fail_count` | như trên |
| `SPDV Count all` | `spdv_pass_count_all` + `spdv_fail_count_all` | như trên, trên bảng `KPIKQI_ALL` |
| `Hiệu quả sử dụng OS` | `pct_hieu_su_dung_os` | measure gốc trả chuỗi `"n% / KPI > 50%"` |
| `Pass Rate Display` / `% Pass Rate` | `pct_on_time` | ba measure cùng công thức, gộp về một metric |

**Cố ý KHÔNG dựng lại — 38 measure nhóm hiển thị** (`text_measure`, `conditional`, `iterator`). Đây là chuỗi hiển thị
(`"▲ 12,3% so với tháng trước"`, `"3 SP Failed"`, `"0.85 / KPI < 1"`). Metric kiểu chuỗi
không lọc/sắp xếp/tổng hợp được và rất khó bảo trì. Cách dựng trên Lightdash:

| Phần trang trí | Làm bằng |
|---|---|
| `▲` / `▼` + % so kỳ trước | **Big value** → bật *Comparison*, chọn kỳ so sánh |
| `" / KPI < 30"` | ô *Subtitle* / *Comment* của Big value |
| `"n SP Passed"` | Big value trên metric đếm (`cyceltime_pass`…) + đặt *Label* |
| Tô màu theo ngưỡng | *Conditional formatting* của Table / Big value |
| `Customertype Tooltip`, `SPDV Tooltip nocodb` (`CONCATENATEX` nối tên) | Table nhỏ trong dashboard tile thay cho tooltip |

**Đổi tên — measure `%` giờ có tiền tố `pct_`.** Trước đây bộ sinh bỏ ký tự `%` khi đặt
tên nên `X` và `%X` ra cùng một slug, một trong hai bị ghi đè và biến mất khỏi
`measures_todo.yml`. Đã sửa ở `powerbi_tools/run.py` (hàm `measure_slug`). Báo cáo này có
**10 metric đổi tên**: `% bug` → `pct_bug`, `% defect` → `pct_defect`, `% Đúng hạn` →
`pct_on_time`, `% release` → `pct_release`, `% PYC Release` → `pct_pyc_release`,
`% release PYC` → `pct_release_pyc`, `% Worklog by version` → `pct_worklog_by_version`,
`% Pass Rate` → `pct_pass_rate`, `% Pass Rate Last Month` → `pct_pass_rate_last_month`,
`% Rework` → `pct_rework`. (QTSX V2 không có cặp đụng độ nào, nhưng vẫn đổi cho nhất quán
với `production_management_1`.)

**Còn lại chưa làm:** chỉ còn measure `priority = 0` — **không chart nào dùng**, nên không
cần cho việc dựng lại dashboard. Danh sách đầy đủ ở `layout/measures_todo.yml`.

`ti_le_bug_mm` (trước đây bị chặn) **đã xong**: quan hệ thật trong DataModel là
`'bug OS'[Project] = 'Project'[Project Name]` (active) chứ không phải qua `ttsx_production`,
nên gộp `bug OS` + `PYC OS` về (YearMonth × sản phẩm) là chia được.

---

## D0 — Tổng quan

**Mục đích:** Trang mở đầu dành cho lãnh đạo. Một màn hình trả lời: tuần/tháng này VCS có đang đạt cam kết với khách hàng không, sản xuất có nhanh không, chất lượng có tụt không.

**Bố cục gốc:** hàng trên là 2 pie KPI/KQI + các card tăng trưởng; hàng giữa là 3 cụm KPI tiến độ (release, cycle time, lead time); hàng dưới là 3 cụm KPI chất lượng (production bug, defect, bug); cuối trang là 2 bảng pivot chi tiết.

**Bộ lọc trang:** khoảng ngày (`qtsx_v2_dim_date.date_value`), quý (`quarter_year`), tháng (`year_month`), tuần (`week_year`), sản phẩm (`qtsx_v2_project.project_name`), trung tâm (`qtsx_v2_project.center`).

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **% hoàn thành KPI KQI theo SPDV** · Pie | `qtsx_v2_kpikqi_nocodb` + `qtsx_v2_dim_status`<br>Lát cắt: `status`<br>Giá trị: ⚠️`spdv_count_nocodb`<br>Tooltip: ⚠️`spdv_tooltip_nocodb`<br>Lọc: nhóm KH = `VVIP`, chu kỳ = `Week`, tuần mới nhất | Trong tuần vừa rồi, bao nhiêu **sản phẩm dịch vụ** đạt trọn bộ chỉ tiêu cam kết với khách VVIP, bao nhiêu trượt. Đây là con số ban lãnh đạo nhìn đầu tiên. | Tạo pie chart trên Lightdash từ model `qtsx_v2_kpikqi_nocodb` join `qtsx_v2_dim_status`. Lát cắt theo dimension `status`, giá trị là metric `spdv_count_nocodb`, sắp xếp giảm dần. Lọc `customer_group = 'VVIP'` và chu kỳ báo cáo `Week`, chỉ lấy tuần mới nhất. Đặt tiêu đề "% hoàn thành KPI KQI theo SPDV". |
| 2 | **% hoàn thành KPI KQI KH VVIP** · Pie | `qtsx_v2_kpikqi_nocodb` + `qtsx_v2_dim_status`<br>Lát cắt: `status`<br>Giá trị: ⚠️`customer_count_nocodb`<br>Tooltip: ⚠️`customertype_tooltip`<br>Lọc: nhóm KH = `VVIP`, chu kỳ = `Week`, tuần mới nhất | Giống chart 1 nhưng **đếm theo khách hàng** thay vì theo sản phẩm. Một khách hàng chỉ tính "đạt" khi mọi chỉ tiêu của họ đều đạt. | Tạo pie chart trên Lightdash từ model `qtsx_v2_kpikqi_nocodb` join `qtsx_v2_dim_status`. Lát cắt theo `status`, giá trị là metric `customer_count_nocodb`, sắp xếp giảm dần. Lọc `customer_group = 'VVIP'`, chu kỳ `Week`, tuần mới nhất. Đặt tiêu đề "% hoàn thành KPI KQI KH VVIP". |
| 3 | *(card phụ của chart 2)* · Big value | `qtsx_v2_kpikqi_nocodb`<br>⚠️`customer_pass_growth_detail` | Diễn giải bằng chữ mức tăng/giảm số khách hàng đạt so với tuần trước, ví dụ "tăng 2 KH so với tuần trước". | Tạo Big value trên Lightdash từ model `qtsx_v2_kpikqi_nocodb` với metric `customer_pass_growth_detail`, lọc `customer_group = 'VVIP'`, chu kỳ `Week`, tuần mới nhất. Không hiện tiêu đề, đặt ngay dưới pie "% hoàn thành KPI KQI KH VVIP". |
| 4 | *(card phụ của chart 2)* · Big value | `qtsx_v2_kpikqi_nocodb`<br>⚠️`customer_pass_growth_text` | Phần trăm thay đổi số khách hàng đạt so với kỳ trước, kèm mũi tên lên/xuống. | Tạo Big value trên Lightdash từ model `qtsx_v2_kpikqi_nocodb` với metric `customer_pass_growth_text`, lọc `customer_group = 'VVIP'`, chu kỳ `Week`, tuần mới nhất. Hiển thị dạng % có so sánh kỳ trước. |
| 5 | *(card phụ của chart 1)* · Big value | `qtsx_v2_kpikqi_nocodb`<br>⚠️`spdv_pass_growth_text` | Phần trăm thay đổi số SPDV đạt so với kỳ trước. | Tạo Big value trên Lightdash từ model `qtsx_v2_kpikqi_nocodb` với metric `spdv_pass_growth_text`, lọc `customer_group = 'VVIP'`, chu kỳ `Week`, tuần mới nhất. |
| 6 | *(card phụ của chart 1)* · Big value | `qtsx_v2_kpikqi_nocodb`<br>⚠️`spdv_pass_growth_detail` | Diễn giải bằng chữ mức tăng/giảm số SPDV đạt. | Tạo Big value trên Lightdash từ model `qtsx_v2_kpikqi_nocodb` với metric `spdv_pass_growth_detail`, lọc `customer_group = 'VVIP'`, chu kỳ `Week`, tuần mới nhất. |
| 7 | **Tần suất release** · Big value | `qtsx_v2_dgcl_ttk`<br>⚠️`release_kpi`<br>Lọc: tháng mới nhất | Trung bình bao nhiêu ngày ra một bản release. Số càng nhỏ càng tốt — đội sản xuất giao hàng càng đều tay. | Tạo Big value trên Lightdash từ model `qtsx_v2_dgcl_ttk` với metric `release_kpi`, lọc tháng mới nhất. Đặt tiêu đề "Tần suất release", đơn vị ngày. |
| 8 | **Cycle time US trung bình VCS** · Big value | `qtsx_v2_cycel_time`<br>⚠️`cycel_time_avg_text`<br>Lọc: tháng mới nhất | Trung bình một user story mất bao nhiêu ngày **kể từ lúc bắt đầu code** đến khi done. Đo tốc độ đội dev. | Tạo Big value trên Lightdash từ model `qtsx_v2_cycel_time` với metric `cycel_time_avg_text`, lọc tháng mới nhất. Đặt tiêu đề "Cycle time US trung bình VCS". |
| 9 | **Lead time US trung bình VCS** · Big value | `qtsx_v2_lead_time`<br>⚠️`lead_time_avg_text`<br>Lọc: tháng mới nhất | Trung bình một user story mất bao nhiêu ngày **kể từ lúc khách yêu cầu** đến khi done. Luôn ≥ cycle time; chênh lệch chính là thời gian chờ trong hàng đợi. | Tạo Big value trên Lightdash từ model `qtsx_v2_lead_time` với metric `lead_time_avg_text`, lọc tháng mới nhất. Đặt tiêu đề "Lead time US trung bình VCS". |
| 10 | *(card phụ của chart 7)* · Big value | `qtsx_v2_dgcl_ttk` · ⚠️`release_change_text` | Tần suất release tháng này so với tháng trước, nhanh lên hay chậm đi. | Tạo Big value từ `qtsx_v2_dgcl_ttk`, metric `release_change_text`, lọc tháng mới nhất. Đặt dưới card "Tần suất release". |
| 11 | *(card phụ của chart 8)* · Big value | `qtsx_v2_cycel_time` · ⚠️`cyceltime_change_text` | Cycle time tháng này so với tháng trước. | Tạo Big value từ `qtsx_v2_cycel_time`, metric `cyceltime_change_text`, lọc tháng mới nhất. |
| 12 | *(card phụ của chart 9)* · Big value | `qtsx_v2_lead_time` · ⚠️`lead_time_change_text` | Lead time tháng này so với tháng trước. | Tạo Big value từ `qtsx_v2_lead_time`, metric `lead_time_change_text`, lọc tháng mới nhất. |
| 13 | *(card phụ của chart 7)* · Big value | `qtsx_v2_dgcl_ttk` · ⚠️`spdv_pass_release` | Số sản phẩm **đạt** chuẩn tần suất release. | Tạo Big value từ `qtsx_v2_dgcl_ttk`, metric `spdv_pass_release`, lọc tháng mới nhất. Nhãn "Đạt". |
| 14 | *(card phụ của chart 7)* · Big value | `qtsx_v2_dgcl_ttk` · ⚠️`spdv_fail_release` | Số sản phẩm **không đạt** chuẩn tần suất release — danh sách cần nhắc nhở. | Tạo Big value từ `qtsx_v2_dgcl_ttk`, metric `spdv_fail_release`, lọc tháng mới nhất. Nhãn "Không đạt". |
| 15 | *(card phụ của chart 8)* · Big value | `qtsx_v2_cycel_time` · ⚠️`cyceltime_pass` | Số sản phẩm đạt ngưỡng cycle time. | Tạo Big value từ `qtsx_v2_cycel_time`, metric `cyceltime_pass`, lọc tháng mới nhất. |
| 16 | *(card phụ của chart 8)* · Big value | `qtsx_v2_cycel_time` · ⚠️`cyceltime_fail` | Số sản phẩm vượt ngưỡng cycle time. | Tạo Big value từ `qtsx_v2_cycel_time`, metric `cyceltime_fail`, lọc tháng mới nhất. |
| 17 | *(card phụ của chart 9)* · Big value | `qtsx_v2_lead_time` · ⚠️`leadtime_pass` | Số sản phẩm đạt ngưỡng lead time. | Tạo Big value từ `qtsx_v2_lead_time`, metric `leadtime_pass`, lọc tháng mới nhất. |
| 18 | *(card phụ của chart 9)* · Big value | `qtsx_v2_lead_time` · ⚠️`leadtime_fail` | Số sản phẩm vượt ngưỡng lead time. | Tạo Big value từ `qtsx_v2_lead_time`, metric `leadtime_fail`, lọc tháng mới nhất. |
| 19 | **Production bug VCS (High / Critical)** · Big value | `qtsx_v2_bug_productions`<br>⚠️`kpi_display_full`<br>Lọc: tháng mới nhất | Số lỗi mức cao/nghiêm trọng **lọt ra môi trường thật**, khách hàng nhìn thấy. Đây là chỉ số chất lượng đau nhất. | Tạo Big value từ `qtsx_v2_bug_productions`, metric `kpi_display_full`, lọc tháng mới nhất. Đặt tiêu đề "Production bug VCS (High / Critical)". |
| 20 | **Defect rate VCS** · Big value | `qtsx_v2_defect`<br>⚠️`kpi_display`<br>Lọc: tháng mới nhất | Tỷ lệ defect phát hiện trong quá trình kiểm thử nội bộ, tính trên khối lượng công (MM). Cao nghĩa là chất lượng code đầu vào kém. | Tạo Big value từ `qtsx_v2_defect`, metric `kpi_display`, lọc tháng mới nhất. Đặt tiêu đề "Defect rate VCS". |
| 21 | **Bug rate VCS** · Big value | `qtsx_v2_bug`<br>⚠️`kpi_bug_display`<br>Lọc: tháng mới nhất | Tỷ lệ bug trên khối lượng công. Khác defect ở chỗ tính cả bug ngoài chu trình kiểm thử. | Tạo Big value từ `qtsx_v2_bug`, metric `kpi_bug_display`, lọc tháng mới nhất. Đặt tiêu đề "Bug rate VCS". |
| 22 | *(card phụ của chart 19)* · Big value | `qtsx_v2_bug_productions` · ⚠️`kpi_delta_display` | Production bug tháng này chênh bao nhiêu so với tháng trước. | Tạo Big value từ `qtsx_v2_bug_productions`, metric `kpi_delta_display`, lọc tháng mới nhất. |
| 23 | *(card phụ của chart 20)* · Big value | `qtsx_v2_defect` · ⚠️`project_pass_mo_m_display` | Số project đạt chuẩn defect, so sánh tháng này với tháng trước (MoM). | Tạo Big value từ `qtsx_v2_defect`, metric `project_pass_mo_m_display`, lọc tháng mới nhất. |
| 24 | *(card phụ của chart 21)* · Big value | `qtsx_v2_bug` · ⚠️`bug_project_pass_mo_m_display` | Số project đạt chuẩn bug rate, so sánh MoM. | Tạo Big value từ `qtsx_v2_bug`, metric `bug_project_pass_mo_m_display`, lọc tháng mới nhất. |
| 25 | *(card phụ của chart 19)* · Big value | `qtsx_v2_bug_productions` · ⚠️`pass_projects_count` | Số project **không có** production bug high/critical. | Tạo Big value từ `qtsx_v2_bug_productions`, metric `pass_projects_count`, lọc tháng mới nhất. Nhãn "Đạt". |
| 26 | *(card phụ của chart 19)* · Big value | `qtsx_v2_bug_productions` · ⚠️`fail_projects_display` | Số project **có** production bug high/critical — danh sách phải giải trình. | Tạo Big value từ `qtsx_v2_bug_productions`, metric `fail_projects_display`, lọc tháng mới nhất. Nhãn "Không đạt". |
| 27 | *(card phụ của chart 20)* · Big value | `qtsx_v2_defect` · ⚠️`defect_project_pass_show` | Số project đạt ngưỡng defect rate. | Tạo Big value từ `qtsx_v2_defect`, metric `defect_project_pass_show`, lọc tháng mới nhất. |
| 28 | *(card phụ của chart 20)* · Big value | `qtsx_v2_defect` · ⚠️`defect_project_fail_show` | Số project vượt ngưỡng defect rate. | Tạo Big value từ `qtsx_v2_defect`, metric `defect_project_fail_show`, lọc tháng mới nhất. |
| 29 | *(card phụ của chart 21)* · Big value | `qtsx_v2_bug` · ⚠️`bug_project_pass_show` | Số project đạt ngưỡng bug rate. | Tạo Big value từ `qtsx_v2_bug`, metric `bug_project_pass_show`, lọc tháng mới nhất. |
| 30 | *(card phụ của chart 21)* · Big value | `qtsx_v2_bug` · ⚠️`bug_project_fail_show` | Số project vượt ngưỡng bug rate. | Tạo Big value từ `qtsx_v2_bug`, metric `bug_project_fail_show`, lọc tháng mới nhất. |
| 31 | **% hoàn thành KPI KQI KH VVIP** · Table (pivot) | `qtsx_v2_kpikqi_nocodb`<br>Hàng: `spdv` · Cột: `customer_name`<br>Giá trị: ⚠️`final_status`<br>Lọc: nhóm KH = `VVIP`, chu kỳ = `Week`, tuần mới nhất | Ma trận **sản phẩm × khách hàng**, mỗi ô là Đạt/Không đạt. Nhìn ngang biết một sản phẩm đang trượt ở khách nào, nhìn dọc biết một khách đang trượt ở sản phẩm nào. | Tạo Table trên Lightdash từ model `qtsx_v2_kpikqi_nocodb`, bật chế độ pivot: hàng là `spdv` (sắp xếp tăng dần), cột là `customer_name`, ô giá trị là metric `final_status`. Lọc `customer_group = 'VVIP'`, `criteria_period_type = 'Week'`, tuần mới nhất. Đặt tiêu đề "% hoàn thành KPI KQI KH VVIP". |
| 32 | **% hoàn thành KPI KQI KH VVIP** *(bản dịch vụ)* · Table (pivot) | `qtsx_v2_kpikqi_all`<br>Cột: `spdv`<br>Giá trị: ⚠️`spdv_status_all`, ⚠️`spdv_status_all_color`<br>Lọc: `spdv = SE`, chu kỳ `Week`, tuần mới nhất | Cùng ý tưởng chart 31 nhưng lấy từ bảng KPI cấp **dịch vụ** (`kpikqi_all`). Metric `..._color` chỉ để tô màu ô, không phải số liệu. | Tạo Table pivot từ model `qtsx_v2_kpikqi_all`: cột là `spdv`, giá trị là metric `spdv_status_all`. Dùng metric `spdv_status_all_color` làm quy tắc tô màu điều kiện, không hiển thị thành cột riêng. Lọc `criteria_period_type = 'Week'`, tuần mới nhất. |

---

## D1 — KPIKQI - VALUE

**Mục đích:** Bóc tách kết quả KPI/KQI ở D0 theo 3 chiều: **khách hàng**, **sản phẩm dịch vụ**, **trung tâm**. Dùng khi lãnh đạo hỏi "trượt ở đâu, do đơn vị nào".

**Bộ lọc trang:** khoảng ngày, quý, tháng, tuần (`week_display`), sản phẩm, trung tâm, chu kỳ báo cáo (`qtsx_v2_period_type.period`), nhóm khách hàng, SPDV.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **% hoàn thành KPI KQI KH** · Pie | `qtsx_v2_kpikqi_nocodb` + `qtsx_v2_dim_status`<br>Lát cắt `status` · Giá trị ⚠️`customer_count_nocodb` | Tỷ lệ khách hàng đạt / không đạt, nhưng **không khóa cứng VVIP** như D0 — nhóm khách hàng do người dùng chọn ở bộ lọc trang. | Tạo pie chart từ `qtsx_v2_kpikqi_nocodb` join `qtsx_v2_dim_status`. Lát cắt `status`, giá trị metric `customer_count_nocodb`, sắp xếp giảm dần, tooltip thêm `customertype_tooltip`. Đặt tiêu đề "% hoàn thành KPI KQI KH". |
| 2 | **% hoàn thành KPI KQI theo SPDV** · Pie | `qtsx_v2_kpikqi_nocodb` + `qtsx_v2_dim_status`<br>Lát cắt `status` · Giá trị ⚠️`spdv_count_nocodb` | Tỷ lệ SPDV đạt / không đạt, nhóm khách hàng do bộ lọc trang quyết định. | Tạo pie chart từ `qtsx_v2_kpikqi_nocodb` join `qtsx_v2_dim_status`. Lát cắt `status`, giá trị `spdv_count_nocodb`, tooltip `spdv_tooltip_nocodb`. Đặt tiêu đề "% hoàn thành KPI KQI theo SPDV". |
| 3 | *(card phụ của chart 1)* · Big value | `qtsx_v2_kpikqi_nocodb` · ⚠️`customer_pass_growth_detail` | Diễn giải mức tăng/giảm số khách đạt so với kỳ trước. | Tạo Big value từ `qtsx_v2_kpikqi_nocodb`, metric `customer_pass_growth_detail`. |
| 4 | *(card phụ của chart 1)* · Big value | `qtsx_v2_kpikqi_nocodb` · ⚠️`customer_pass_growth_text` | % thay đổi số khách đạt so với kỳ trước. | Tạo Big value từ `qtsx_v2_kpikqi_nocodb`, metric `customer_pass_growth_text`. |
| 5 | *(card phụ của chart 2)* · Big value | `qtsx_v2_kpikqi_nocodb` · ⚠️`spdv_pass_growth_detail` | Diễn giải mức tăng/giảm số SPDV đạt. | Tạo Big value từ `qtsx_v2_kpikqi_nocodb`, metric `spdv_pass_growth_detail`. |
| 6 | *(card phụ của chart 2)* · Big value | `qtsx_v2_kpikqi_nocodb` · ⚠️`spdv_pass_growth_text` | % thay đổi số SPDV đạt. | Tạo Big value từ `qtsx_v2_kpikqi_nocodb`, metric `spdv_pass_growth_text`. |
| 7 | **% hoàn thành KPI KQI theo góc KH VVIP** · Bar stacked 100% | `qtsx_v2_kpikqi_nocodb`<br>Trục X: `customer_name`<br>Giá trị: ✅`count_evaluation`<br>Nhóm màu: `evaluation` | Mỗi cột là một khách hàng, chia đôi Pass/Fail theo tỷ lệ. Nhìn phát hiện ngay khách nào đang "đỏ" nhiều nhất. | Tạo Bar chart trên Lightdash từ `qtsx_v2_kpikqi_nocodb`: trục X `customer_name`, giá trị metric `count_evaluation`, nhóm màu theo dimension `evaluation`. Bật Stack và Normalize to 100%. Sắp xếp giảm dần. Đặt tiêu đề "% hoàn thành KPI KQI theo góc KH VVIP". |
| 8 | **% hoàn thành KPI KQI KH VVIP** · Table (pivot) | `qtsx_v2_kpikqi_nocodb`<br>Hàng `customer_name` · Cột `spdv`<br>Giá trị ⚠️`final_status` | Ma trận khách hàng × sản phẩm (đảo trục so với D0-31), tiện khi muốn duyệt theo từng khách. | Tạo Table pivot từ `qtsx_v2_kpikqi_nocodb`: hàng `customer_name` sắp xếp tăng dần, cột `spdv`, giá trị metric `final_status`. Đặt tiêu đề "% hoàn thành KPI KQI KH VVIP". |
| 9 | **% hoàn thành KPI KQI theo SPDV** · Pie | 🔁 Giống chart 2 của trang này | Bản sao đặt ở vị trí khác trong layout để tiện đối chiếu với cụm chart trung tâm bên dưới. | 🔁 Dùng lại chart "% hoàn thành KPI KQI theo SPDV" (chart 2). Trên Lightdash chỉ cần kéo cùng một saved chart vào dashboard hai lần. |
| 10 | **% sản phẩm hoàn thành KPI KQI theo TT** · Horizontal bar stacked 100% | `qtsx_v2_kpikqi_nocodb`<br>Trục: `center` · Giá trị ✅`count_evaluation` · Nhóm màu `evaluation` | Xếp hạng **trung tâm** theo tỷ lệ Pass/Fail. Dùng để so sánh đơn vị với nhau. | Tạo Bar chart ngang từ `qtsx_v2_kpikqi_nocodb`: trục `center`, giá trị `count_evaluation`, nhóm màu `evaluation`, bật Stack 100% và Horizontal. Sắp xếp giảm dần. Đặt tiêu đề "% sản phẩm hoàn thành KPI KQI theo TT". |
| 11 | **% sản phẩm hoàn thành KPI KQI theo SP** · Bar stacked 100% | `qtsx_v2_kpikqi_nocodb`<br>Trục X `spdv` · Giá trị ✅`count_evaluation` · Nhóm màu `evaluation` | Xếp hạng **sản phẩm** theo tỷ lệ Pass/Fail. | Tạo Bar chart từ `qtsx_v2_kpikqi_nocodb`: trục X `spdv`, giá trị `count_evaluation`, nhóm màu `evaluation`, Stack 100%. Đặt tiêu đề "% sản phẩm hoàn thành KPI KQI theo SP". |
| 12 | **% hoàn thành KPI KQI theo DV** · Pie | `qtsx_v2_kpikqi_all` + `qtsx_v2_dim_status`<br>Lát cắt `status` · Giá trị ⚠️`spdv_count_all`<br>Lọc: `spdv` ∈ (M-Suite, SE, VCS-CyM) | Cùng câu hỏi như chart 2 nhưng dữ liệu lấy từ bảng KPI **dịch vụ** hợp nhất, chỉ tính 3 dịch vụ trọng điểm. | Tạo pie chart từ `qtsx_v2_kpikqi_all` join `qtsx_v2_dim_status`. Lát cắt `status`, giá trị metric `spdv_count_all`, tooltip `spdv_tooltip_all`. Lọc `spdv IN ('M-Suite','SE','VCS-CyM')`. Đặt tiêu đề "% hoàn thành KPI KQI theo DV". |
| 13 | **% sản phẩm hoàn thành KPI KQI theo TT** *(bản dịch vụ)* · Horizontal bar stacked 100% | `qtsx_v2_kpikqi_all`<br>Trục `center` · Giá trị ✅`count_evaluation_v2` · Nhóm màu `evaluation_v2`<br>Lọc: `spdv` ∈ (M-Suite, SE, VCS-CyM), tuần mới nhất | Xếp hạng trung tâm, nhưng đo bằng chỉ tiêu dịch vụ (`evaluation_v2`) thay vì chỉ tiêu sản phẩm. | Tạo Bar chart ngang từ `qtsx_v2_kpikqi_all`: trục `center`, giá trị `count_evaluation_v2`, nhóm màu `evaluation_v2`, Stack 100%. Lọc `spdv IN ('M-Suite','SE','VCS-CyM')`, tuần mới nhất. Đặt tiêu đề "% sản phẩm hoàn thành KPI KQI theo TT". |
| 14 | **% sản phẩm hoàn thành KPI KQI theo DV** · Bar stacked 100% | `qtsx_v2_kpikqi_all`<br>Trục X `spdv` · Giá trị ✅`count_evaluation_v2` · Nhóm màu `evaluation_v2`<br>Lọc: `spdv` ∈ (SE, VCS-CyM, M-Suite), tuần mới nhất | Xếp hạng dịch vụ theo tỷ lệ đạt chỉ tiêu. | Tạo Bar chart từ `qtsx_v2_kpikqi_all`: trục X `spdv`, giá trị `count_evaluation_v2`, nhóm màu `evaluation_v2`, Stack 100%. Lọc `spdv IN ('SE','VCS-CyM','M-Suite')`, tuần mới nhất. Đặt tiêu đề "% sản phẩm hoàn thành KPI KQI theo DV". |

---

## D2 — CHI TIẾT KPIKQI

**Mục đích:** Hai bảng tra cứu ở mức chi tiết nhất — từng chỉ tiêu KPI một, kèm target, kết quả, đánh giá và xu hướng. Đây là nơi người phụ trách vào để biết **con số cụ thể** thay vì tỷ lệ tổng hợp.

**Bộ lọc trang:** khoảng ngày, quý, tháng, tuần, khách hàng, sản phẩm, SPDV, chu kỳ, nhóm khách hàng, trung tâm, tên KPI (`kpiname`), tên chỉ tiêu (`metric_name`).

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **KPI SẢN PHẨM** · Table | `qtsx_v2_kpikqi_nocodb`<br>Cột: `end_time` (kỳ báo cáo), `metric_name`, `unit`, `spdv`, `target`, `condition`, `result`, `evaluation`, `trend_v2`, `result_prev_1_period`, `so_with_period_n_1_td`, `customer_name` | Bảng chi tiết KPI **cấp sản phẩm**. Đọc theo dòng: chỉ tiêu này target bao nhiêu, điều kiện đạt là gì (≥ hay ≤), kỳ này ra bao nhiêu, kỳ trước bao nhiêu, xu hướng đi lên hay xuống. | Tạo Table trên Lightdash từ model `qtsx_v2_kpikqi_nocodb` với các cột theo thứ tự: `end_time` (định dạng ngày), `metric_name`, `unit`, `spdv` (sắp xếp tăng dần), `target` (2 chữ số thập phân), `condition`, `result`, `evaluation`, `trend_v2`, `result_prev_1_period`, `so_with_period_n_1_td`, `customer_name`. Đặt tiêu đề "KPI SẢN PHẨM". |
| 2 | **KPI DỊCH VỤ** · Table | `qtsx_v2_kpikqi_all`<br>Cột: `metric_name`, `spdv`, `unit_of_measure`, `target`, `condition`, `result`, `evaluation`, `report_date`, `center`, `trend_v2`, `result_prev_1_period` | Bảng chi tiết KPI **cấp dịch vụ**, thêm cột trung tâm phụ trách để quy trách nhiệm. | Tạo Table từ model `qtsx_v2_kpikqi_all` với các cột: `metric_name`, `spdv` (sắp xếp tăng dần), `unit_of_measure`, `target`, `condition`, `result`, `evaluation`, `report_date` (định dạng ngày), `center`, `trend_v2`, `result_prev_1_period`. Đặt tiêu đề "KPI DỊCH VỤ". |

---

## D3 — Tiến độ - Progress

**Mục đích:** Trả lời "sản xuất có đang chạy nhanh không". Ba chỉ số xương sống: **tần suất release** (bao lâu ra hàng một lần), **cycle time** (code xong mất bao lâu), **lead time** (từ lúc nhận yêu cầu mất bao lâu). Mỗi chỉ số đều được nhìn theo 3 lát: xu hướng thời gian, so sánh trung tâm, so sánh sản phẩm.

**Bộ lọc trang:** khoảng ngày, trung tâm, tháng, sản phẩm.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **Progress & Commitment Index** · Big value | `qtsx_v2_dgcl_ttk` · ⚠️`release_kpi` · tháng mới nhất | Chỉ số tổng hợp về mức độ giữ đúng cam kết tiến độ. Thực chất lấy từ tần suất release. | Tạo Big value từ `qtsx_v2_dgcl_ttk`, metric `release_kpi`, lọc tháng mới nhất. Đặt tiêu đề "Progress & Commitment Index". |
| 2 | **Lead time trung bình VCS** · Big value | `qtsx_v2_lead_time` · ⚠️`lead_time_avg_text` · tháng mới nhất | 🔁 Giống D0-9. Số ngày trung bình từ khi nhận yêu cầu đến khi done. | Tạo Big value từ `qtsx_v2_lead_time`, metric `lead_time_avg_text`, lọc tháng mới nhất. Đặt tiêu đề "Lead time trung bình VCS". |
| 3 | **Cycle time trung bình VCS** · Big value | `qtsx_v2_cycel_time` · ⚠️`cycel_time_avg_text` · tháng mới nhất | 🔁 Giống D0-8. Số ngày trung bình từ khi bắt đầu làm đến khi done. | Tạo Big value từ `qtsx_v2_cycel_time`, metric `cycel_time_avg_text`, lọc tháng mới nhất. Đặt tiêu đề "Cycle time trung bình VCS". |
| 4 | *(card phụ của chart 1)* · Big value | `qtsx_v2_dgcl_ttk` · ⚠️`release_change_text` | Tần suất release so với tháng trước. | Tạo Big value từ `qtsx_v2_dgcl_ttk`, metric `release_change_text`, lọc tháng mới nhất. |
| 5 | *(card phụ của chart 3)* · Big value | `qtsx_v2_cycel_time` · ⚠️`cyceltime_change_text` | Cycle time so với tháng trước. | Tạo Big value từ `qtsx_v2_cycel_time`, metric `cyceltime_change_text`, lọc tháng mới nhất. |
| 6 | *(card phụ của chart 2)* · Big value | `qtsx_v2_lead_time` · ⚠️`lead_time_change_text` | Lead time so với tháng trước. | Tạo Big value từ `qtsx_v2_lead_time`, metric `lead_time_change_text`, lọc tháng mới nhất. |
| 7 | *(card phụ của chart 1)* · Big value | `qtsx_v2_dgcl_ttk` · ⚠️`spdv_pass_release` | Số sản phẩm đạt chuẩn tần suất release. | Tạo Big value từ `qtsx_v2_dgcl_ttk`, metric `spdv_pass_release`, lọc tháng mới nhất. |
| 8 | *(card phụ của chart 1)* · Big value | `qtsx_v2_dgcl_ttk` · ⚠️`spdv_fail_release` | Số sản phẩm không đạt chuẩn tần suất release. | Tạo Big value từ `qtsx_v2_dgcl_ttk`, metric `spdv_fail_release`, lọc tháng mới nhất. |
| 9 | *(card phụ của chart 3)* · Big value | `qtsx_v2_cycel_time` · ⚠️`cyceltime_pass` | Số sản phẩm đạt ngưỡng cycle time. | Tạo Big value từ `qtsx_v2_cycel_time`, metric `cyceltime_pass`, lọc tháng mới nhất. |
| 10 | *(card phụ của chart 3)* · Big value | `qtsx_v2_cycel_time` · ⚠️`cyceltime_fail` | Số sản phẩm vượt ngưỡng cycle time. | Tạo Big value từ `qtsx_v2_cycel_time`, metric `cyceltime_fail`, lọc tháng mới nhất. |
| 11 | *(card phụ của chart 2)* · Big value | `qtsx_v2_lead_time` · ⚠️`leadtime_pass` | Số sản phẩm đạt ngưỡng lead time. | Tạo Big value từ `qtsx_v2_lead_time`, metric `leadtime_pass`, lọc tháng mới nhất. |
| 12 | *(card phụ của chart 2)* · Big value | `qtsx_v2_lead_time` · ⚠️`leadtime_fail` | Số sản phẩm vượt ngưỡng lead time. | Tạo Big value từ `qtsx_v2_lead_time`, metric `leadtime_fail`, lọc tháng mới nhất. |
| 13 | **Tần suất release** · Line | `qtsx_v2_dgcl_ttk` + `qtsx_v2_dim_date`<br>Trục X: `date_value` gom theo tháng<br>Giá trị: ✅`average_release_frequency` | Đường xu hướng số ngày giữa hai lần release, theo từng tháng. Đường đi xuống là tín hiệu tốt (release dày hơn). | Tạo Line chart từ `qtsx_v2_dgcl_ttk` join `qtsx_v2_dim_date`: trục X là `date_value` gom theo tháng, giá trị là metric `average_release_frequency` (2 chữ số thập phân). Đặt tiêu đề "Tần suất release", nhãn trục Y "ngày". |
| 14 | **Tần suất release trung bình theo TT** · Line | Như chart 13, thêm nhóm màu `qtsx_v2_dgcl_ttk.center` | Cùng đường xu hướng nhưng tách theo trung tâm — biết đơn vị nào đang cải thiện, đơn vị nào chững lại. | Tạo Line chart từ `qtsx_v2_dgcl_ttk` join `qtsx_v2_dim_date`: trục X `date_value` theo tháng, giá trị `average_release_frequency`, nhóm màu theo dimension `center`. Đặt tiêu đề "Tần suất release trung bình theo TT". |
| 15 | **Tần suất release theo SP** · Bar | `qtsx_v2_dgcl_ttk`<br>Trục X: `project` · Giá trị: ✅`average_release_frequency`<br>Lọc: tháng mới nhất | Xếp hạng sản phẩm theo tần suất release trong tháng gần nhất. Cột cao = lâu mới ra bản mới. | Tạo Bar chart từ `qtsx_v2_dgcl_ttk`: trục X dimension `project`, giá trị metric `average_release_frequency`, sắp xếp giảm dần, lọc tháng mới nhất. Đặt tiêu đề "Tần suất release theo SP". |
| 16 | **Cycle time** · Line | `qtsx_v2_cycel_time` + `qtsx_v2_dim_date`<br>Trục X `date_value` theo tháng · Giá trị ✅`average_days_to_done` | Xu hướng cycle time theo tháng của toàn VCS. | Tạo Line chart từ `qtsx_v2_cycel_time` join `qtsx_v2_dim_date`: trục X `date_value` theo tháng, giá trị metric `average_days_to_done`. Lọc tháng mới nhất theo dashboard filter. Đặt tiêu đề "Cycle time". |
| 17 | **Cycle time trung bình theo TT** · Line | Như chart 16, nhóm màu `qtsx_v2_cycel_time.center` | Cycle time tách theo trung tâm. | Tạo Line chart từ `qtsx_v2_cycel_time` join `qtsx_v2_dim_date`: trục X `date_value` theo tháng, giá trị `average_days_to_done`, nhóm màu `center`. Đặt tiêu đề "Cycle time trung bình theo TT". |
| 18 | **Cycle time theo SP** · Bar | `qtsx_v2_cycel_time`<br>Trục X `projectname` · Giá trị ✅`average_days_to_done` | Xếp hạng sản phẩm theo cycle time. Sản phẩm cột cao là nơi user story bị treo lâu nhất. | Tạo Bar chart từ `qtsx_v2_cycel_time`: trục X `projectname`, giá trị `average_days_to_done`, sắp xếp giảm dần. Đặt tiêu đề "Cycle time theo SP". |
| 19 | **Lead time** · Line | `qtsx_v2_lead_time` + `qtsx_v2_dim_date`<br>Trục X `date_value` theo tháng · Giá trị ✅`average_days_to_done_v2`<br>Lọc: `projectname = aJiant` | Xu hướng lead time. **Lưu ý**: bản gốc đang bị lọc cứng một project (`aJiant`) — nhiều khả năng là lọc sót khi làm báo cáo, nên rà lại trước khi bê nguyên. | Tạo Line chart từ `qtsx_v2_lead_time` join `qtsx_v2_dim_date`: trục X `date_value` theo tháng, giá trị metric `average_days_to_done_v2` (2 chữ số thập phân). Cân nhắc bỏ bộ lọc `projectname = 'aJiant'` của bản Power BI. Đặt tiêu đề "Lead time". |
| 20 | **Lead time trung bình theo TT** · Line | Như chart 19, nhóm màu `qtsx_v2_lead_time.center`, không lọc project | Lead time tách theo trung tâm. | Tạo Line chart từ `qtsx_v2_lead_time` join `qtsx_v2_dim_date`: trục X `date_value` theo tháng, giá trị `average_days_to_done_v2`, nhóm màu `center`. Đặt tiêu đề "Lead time trung bình theo TT". |
| 21 | **Lead time theo SP** · Bar | `qtsx_v2_lead_time`<br>Trục X `projectname` · Giá trị ✅`average_days_to_done_v2`<br>Lọc: tháng mới nhất | Xếp hạng sản phẩm theo lead time. So với chart 18 sẽ thấy sản phẩm nào tốn thời gian **chờ** nhiều hơn thời gian **làm**. | Tạo Bar chart từ `qtsx_v2_lead_time`: trục X `projectname`, giá trị `average_days_to_done_v2`, sắp xếp giảm dần, lọc tháng mới nhất. Đặt tiêu đề "Lead time theo SP". |

---

## D4 — Chất lượng - Quality

**Mục đích:** Trả lời "sản phẩm có đang tốt không". Ba tầng lỗi, xếp theo mức độ nghiêm trọng giảm dần:

- **Production bug** — lỗi đã lọt ra môi trường thật, khách hàng gặp phải. Nghiêm trọng nhất.
- **Defect** — lỗi phát hiện trong quá trình kiểm thử nội bộ, chưa ra ngoài.
- **Bug** — lỗi ghi nhận chung trên Jira.

**Bộ lọc trang:** khoảng ngày, tháng, sản phẩm, trung tâm.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **Production bug VCS** · Big value | `qtsx_v2_bug_productions` · ⚠️`kpi_display_full` · tháng mới nhất | 🔁 Giống D0-19. Số lỗi high/critical lọt ra production. | Tạo Big value từ `qtsx_v2_bug_productions`, metric `kpi_display_full`, lọc tháng mới nhất. Đặt tiêu đề "Production bug VCS". |
| 2 | **Defect VCS** · Big value | `qtsx_v2_defect` · ⚠️`kpi_display` · tháng mới nhất | 🔁 Giống D0-20. Tỷ lệ defect trên khối lượng công. | Tạo Big value từ `qtsx_v2_defect`, metric `kpi_display`, lọc tháng mới nhất. Đặt tiêu đề "Defect VCS". |
| 3 | **Bug VCS** · Big value | `qtsx_v2_bug` · ⚠️`kpi_bug_display` · tháng mới nhất | 🔁 Giống D0-21. Tỷ lệ bug trên khối lượng công. | Tạo Big value từ `qtsx_v2_bug`, metric `kpi_bug_display`, lọc tháng mới nhất. Đặt tiêu đề "Bug VCS". |
| 4 | *(card phụ của chart 1)* · Big value | `qtsx_v2_bug_productions` · ⚠️`kpi_delta_display` | Production bug so với tháng trước. | Tạo Big value từ `qtsx_v2_bug_productions`, metric `kpi_delta_display`, lọc tháng mới nhất. |
| 5 | *(card phụ của chart 2)* · Big value | `qtsx_v2_defect` · ⚠️`project_pass_mo_m_display` | Số project đạt chuẩn defect, so sánh MoM. | Tạo Big value từ `qtsx_v2_defect`, metric `project_pass_mo_m_display`, lọc tháng mới nhất. |
| 6 | *(card phụ của chart 3)* · Big value | `qtsx_v2_bug` · ⚠️`bug_project_pass_mo_m_display` | Số project đạt chuẩn bug rate, so sánh MoM. | Tạo Big value từ `qtsx_v2_bug`, metric `bug_project_pass_mo_m_display`, lọc tháng mới nhất. |
| 7 | *(card phụ của chart 1)* · Big value | `qtsx_v2_bug_productions` · ⚠️`pass_projects_count` | Số project sạch production bug. | Tạo Big value từ `qtsx_v2_bug_productions`, metric `pass_projects_count`, lọc tháng mới nhất. |
| 8 | *(card phụ của chart 1)* · Big value | `qtsx_v2_bug_productions` · ⚠️`fail_projects_display` | Số project dính production bug. | Tạo Big value từ `qtsx_v2_bug_productions`, metric `fail_projects_display`, lọc tháng mới nhất. |
| 9 | *(card phụ của chart 2)* · Big value | `qtsx_v2_defect` · ⚠️`defect_project_pass_show` | Số project đạt ngưỡng defect. | Tạo Big value từ `qtsx_v2_defect`, metric `defect_project_pass_show`, lọc tháng mới nhất. |
| 10 | *(card phụ của chart 2)* · Big value | `qtsx_v2_defect` · ⚠️`defect_project_fail_show` | Số project vượt ngưỡng defect. | Tạo Big value từ `qtsx_v2_defect`, metric `defect_project_fail_show`, lọc tháng mới nhất. |
| 11 | *(card phụ của chart 3)* · Big value | `qtsx_v2_bug` · ⚠️`bug_project_pass_show` | Số project đạt ngưỡng bug rate. | Tạo Big value từ `qtsx_v2_bug`, metric `bug_project_pass_show`, lọc tháng mới nhất. |
| 12 | *(card phụ của chart 3)* · Big value | `qtsx_v2_bug` · ⚠️`bug_project_fail_show` | Số project vượt ngưỡng bug rate. | Tạo Big value từ `qtsx_v2_bug`, metric `bug_project_fail_show`, lọc tháng mới nhất. |
| 13 | **Production bug theo TT** · Bar | `qtsx_v2_bug_productions` + `qtsx_v2_project`<br>Trục X: `center`<br>Giá trị: ✅`bug_count` *(chỉ đếm priority High/Highest/Critical)*<br>Lọc gốc loại bỏ Low/Lowest/Medium | Trung tâm nào đang để lọt nhiều lỗi nặng ra production nhất. | Tạo Bar chart từ `qtsx_v2_bug_productions` join `qtsx_v2_project`: trục X `center`, giá trị metric `bug_count` (metric này đã lọc sẵn priority High/Highest/Critical trong định nghĩa), sắp xếp giảm dần. Đặt tiêu đề "Production bug theo TT". |
| 14 | **Production bug theo SP CAO** · Bar | `qtsx_v2_bug_productions` + `qtsx_v2_project` + `qtsx_v2_dim_date`<br>Trục X: `project_name`, chia nhỏ theo tháng<br>Giá trị: ✅`bug_count_v2` | Số production bug theo từng sản phẩm, tách theo tháng — nhìn thấy sản phẩm nào bị lỗi dồn vào tháng nào. | Tạo Bar chart từ `qtsx_v2_bug_productions` join `qtsx_v2_project` và `qtsx_v2_dim_date`: trục X `project_name` (sắp xếp tăng dần) kết hợp `date_value` gom theo tháng, giá trị metric `bug_count_v2`. Đặt tiêu đề "Production bug theo SP CAO". |
| 15 | **Production bug** · Bar | Như chart 14, không lọc priority | Bản đầy đủ của chart 14, tính mọi mức độ ưu tiên. | Tạo Bar chart từ `qtsx_v2_bug_productions` join `qtsx_v2_project` và `qtsx_v2_dim_date`: trục X `project_name` + tháng, giá trị `bug_count_v2`, không lọc theo priority. Đặt tiêu đề "Production bug". |
| 16 | **Defect theo TT** · Bar | `qtsx_v2_defect` + `qtsx_v2_project`<br>Trục X `center` · Giá trị ✅`count_issuekey`<br>Lọc: tháng mới nhất | Số defect nội bộ theo trung tâm. Cao nghĩa là chất lượng bàn giao sang QA kém, nhưng ít nhất chưa lọt ra khách. | Tạo Bar chart từ `qtsx_v2_defect` join `qtsx_v2_project`: trục X `center`, giá trị metric `count_issuekey`, sắp xếp giảm dần, lọc tháng mới nhất. Đặt tiêu đề "Defect theo TT". |
| 17 | **Defect theo SP** · Mixed (bar + line) | `qtsx_v2_defect` + `qtsx_v2_project` + `qtsx_v2_dim_date`<br>Trục X: `project_name` + tháng<br>Cột: ✅`count_distinct_issuekey`<br>Đường (trục phải): ⚠️`defect` (% defect) | Cột là **số lượng** defect, đường là **tỷ lệ** defect trên khối lượng công. Hai thứ này phải đọc cùng nhau: nhiều defect nhưng project lớn thì tỷ lệ vẫn ổn. | Tạo Mixed chart từ `qtsx_v2_defect` join `qtsx_v2_project` và `qtsx_v2_dim_date`: trục X `project_name` + `date_value` theo tháng. Cột (trục trái) là metric `count_distinct_issuekey`. Đường (trục phải) là metric `defect`, định dạng phần trăm 2 chữ số. Đặt tiêu đề "Defect theo SP". |
| 18 | **Bug theo TT** · Bar | `qtsx_v2_bug` + `qtsx_v2_project`<br>Trục X `center` · Giá trị ✅`count_distinct_issuekey`<br>Lọc: tháng mới nhất | Số bug theo trung tâm. | Tạo Bar chart từ `qtsx_v2_bug` join `qtsx_v2_project`: trục X `center`, giá trị `count_distinct_issuekey`, sắp xếp giảm dần, lọc tháng mới nhất. Đặt tiêu đề "Bug theo TT". |
| 19 | **Bug theo SP** · Mixed (bar + line) | `qtsx_v2_bug` + `qtsx_v2_project` + `qtsx_v2_dim_date`<br>Cột: ✅`count_distinct_issuekey`<br>Đường (trục phải): ⚠️`bug` (% bug) | Song sinh của chart 17 nhưng cho bug thay vì defect. | Tạo Mixed chart từ `qtsx_v2_bug` join `qtsx_v2_project` và `qtsx_v2_dim_date`: trục X `project_name` + tháng. Cột là `count_distinct_issuekey`, đường trục phải là metric `bug` định dạng phần trăm. Đặt tiêu đề "Bug theo SP". |

---

## D5 — Hiệu suất & Nguồn lực

**Mục đích:** Trả lời "bỏ người ra có tạo được giá trị không". Ba chỉ số chính:

- **EE (Hiệu quả sử dụng nguồn lực)** — công ước lượng (ULNL) so với công thực tế bỏ ra.
- **Năng suất lao động (Productivity)** — sản lượng trên mỗi man-month.
- **Busy rate** — tỷ lệ thời gian nhân sự thực sự log việc, so với quỹ thời gian có.

Ngoài ra có nhánh **HS Load người theo version release** — đo phần công đổ vào các version thực sự được release, tránh trường hợp làm nhiều nhưng không ra hàng.

**Bộ lọc trang:** khoảng ngày, tháng, sản phẩm, trung tâm.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **Hiệu quả sử dụng nguồn lực EE** · Big value | `qtsx_v2_issue_ulnl` · ⚠️`hieu_display` · tháng mới nhất | Tỷ lệ công ước lượng / công thực tế. Gần 100% là ước lượng sát; dưới nhiều nghĩa là làm tốn hơn dự kiến. | Tạo Big value từ `qtsx_v2_issue_ulnl`, metric `hieu_display`, lọc tháng mới nhất. Đặt tiêu đề "Hiệu quả sử dụng nguồn lực EE", định dạng phần trăm. |
| 2 | **Năng suất lao động Productivity** · Big value | `qtsx_v2_worklog` · ⚠️`nang_xuat_display` · tháng mới nhất | Sản lượng trên mỗi man-month. | Tạo Big value từ `qtsx_v2_worklog`, metric `nang_xuat_display`, lọc tháng mới nhất. Đặt tiêu đề "Năng suất lao động Productivity". |
| 3 | **Busy rate** · Big value | `qtsx_v2_worklog` · ⚠️`busy_rate_display` · tháng mới nhất | Tỷ lệ giờ thực sự được log việc trên tổng quỹ giờ. Thấp có thể do quên log, hoặc do nhân sự thực sự rảnh. | Tạo Big value từ `qtsx_v2_worklog`, metric `busy_rate_display`, lọc tháng mới nhất. Đặt tiêu đề "Busy rate", định dạng phần trăm. |
| 4 | *(card phụ của chart 1)* · Big value | `qtsx_v2_issue_ulnl` · ⚠️`hieu_cung_period` | EE so với cùng kỳ. | Tạo Big value từ `qtsx_v2_issue_ulnl`, metric `hieu_cung_period`, lọc tháng mới nhất. |
| 5 | *(card phụ của chart 2)* · Big value | `qtsx_v2_worklog` · ⚠️`nang_xuat_so_cung_period` | Năng suất so với cùng kỳ. | Tạo Big value từ `qtsx_v2_worklog`, metric `nang_xuat_so_cung_period`, lọc tháng mới nhất. |
| 6 | *(card phụ của chart 3)* · Big value | `qtsx_v2_worklog` · ⚠️`busy_rate_so_cung_period` | Busy rate so với cùng kỳ. | Tạo Big value từ `qtsx_v2_worklog`, metric `busy_rate_so_cung_period`, lọc tháng mới nhất. |
| 7 | **Hiệu quả sử dụng nguồn lực** · Line | `qtsx_v2_issue_ulnl` + `qtsx_v2_dim_date`<br>Trục X `date_value` theo tháng · Giá trị ⚠️`hieu_su_dung_source_luc_ee` | Xu hướng EE theo tháng. | Tạo Line chart từ `qtsx_v2_issue_ulnl` join `qtsx_v2_dim_date`: trục X `date_value` gom theo tháng, giá trị metric `hieu_su_dung_source_luc_ee`, định dạng phần trăm. Đặt tiêu đề "Hiệu quả sử dụng nguồn lực". |
| 8 | **Hiệu quả sử dụng nguồn lực** *(theo TT)* · Bar | `qtsx_v2_issue_ulnl` + `qtsx_v2_project`<br>Trục X `center` · Giá trị ⚠️`hieu_su_dung_source_luc_ee` | So sánh EE giữa các trung tâm. | Tạo Bar chart từ `qtsx_v2_issue_ulnl` join `qtsx_v2_project`: trục X `center`, giá trị `hieu_su_dung_source_luc_ee` định dạng phần trăm, sắp xếp giảm dần, lọc tháng mới nhất. Đặt tiêu đề "Hiệu quả sử dụng nguồn lực theo TT". |
| 9 | **Hiệu quả sử dụng nguồn lực** *(theo SP)* · Bar | `qtsx_v2_issue_ulnl` + `qtsx_v2_project` + `qtsx_v2_dim_date`<br>Trục X `project_name` + tháng · Giá trị ⚠️`hieu_su_dung_source_luc_ee` | EE từng sản phẩm, tách theo tháng. | Tạo Bar chart từ `qtsx_v2_issue_ulnl` join `qtsx_v2_project` và `qtsx_v2_dim_date`: trục X `project_name` (tăng dần) + `date_value` theo tháng, giá trị `hieu_su_dung_source_luc_ee` định dạng phần trăm. Đặt tiêu đề "Hiệu quả sử dụng nguồn lực theo SP". |
| 10 | **HS Load người theo version release** · Line | `qtsx_v2_ulnl_worklog_by_version_release` + `qtsx_v2_dim_date`<br>Trục X `date_value` theo tháng · Giá trị ⚠️`worklog_by_version` | Tỷ lệ công đổ vào các version **thực sự được release**. Thấp nghĩa là nhiều công đang chảy vào việc không ra sản phẩm. | Tạo Line chart từ `qtsx_v2_ulnl_worklog_by_version_release` join `qtsx_v2_dim_date`: trục X `date_value` theo tháng, giá trị metric `worklog_by_version`, định dạng phần trăm 2 chữ số. Đặt tiêu đề "HS Load người theo version release". |
| 11 | **HS Load người theo version release TT** · Bar | `qtsx_v2_ulnl_worklog_by_version_release` + `qtsx_v2_project`<br>Trục X `center` · Giá trị ⚠️`worklog_by_version` | So sánh giữa các trung tâm. | Tạo Bar chart từ `qtsx_v2_ulnl_worklog_by_version_release` join `qtsx_v2_project`: trục X `center`, giá trị `worklog_by_version` định dạng phần trăm, sắp xếp giảm dần, lọc tháng mới nhất. Đặt tiêu đề "HS Load người theo version release TT". |
| 12 | **HS Load người theo version release SPDV** · Bar | Như chart 11 nhưng trục X là `project_name` + tháng | So sánh giữa các sản phẩm. | Tạo Bar chart từ `qtsx_v2_ulnl_worklog_by_version_release` join `qtsx_v2_project` và `qtsx_v2_dim_date`: trục X `project_name` + tháng, giá trị `worklog_by_version` định dạng phần trăm. Đặt tiêu đề "HS Load người theo version release SPDV". |
| 13 | **Busy rate** · Line | `qtsx_v2_worklog` + `qtsx_v2_dim_date`<br>Trục X `date_value` theo tháng · Giá trị ⚠️`busy_rate` | Xu hướng busy rate theo tháng. | Tạo Line chart từ `qtsx_v2_worklog` join `qtsx_v2_dim_date`: trục X `date_value` theo tháng, giá trị metric `busy_rate`. Đặt tiêu đề "Busy rate". |
| 14 | **Busy rate trung bình theo TT** · Bar | `qtsx_v2_worklog` + `qtsx_v2_project`<br>Trục X `center` · Giá trị ⚠️`nang_xuat_lao_dong` | Tiêu đề ghi "Busy rate" nhưng metric thực tế là **năng suất lao động** — cần thống nhất lại tên khi dựng bản mới. | Tạo Bar chart từ `qtsx_v2_worklog` join `qtsx_v2_project`: trục X `center`, giá trị metric `nang_xuat_lao_dong` định dạng phần trăm, sắp xếp giảm dần, lọc tháng mới nhất. Đặt tiêu đề "Năng suất lao động theo TT" (bản Power BI ghi nhầm là "Busy rate"). |
| 15 | **Busy rate theo SPDV** · Bar | Như chart 14 nhưng trục X `project_name` + tháng | Cùng lưu ý về tên như chart 14. | Tạo Bar chart từ `qtsx_v2_worklog` join `qtsx_v2_project` và `qtsx_v2_dim_date`: trục X `project_name` + tháng, giá trị `nang_xuat_lao_dong` định dạng phần trăm. Đặt tiêu đề "Năng suất lao động theo SPDV". |
| 16 | **HS Load người theo version release SPDV** *(bản chi tiết)* · Bar | `qtsx_v2_ulnl_worklog_by_version_release` + `qtsx_v2_man_month_qa` + `qtsx_v2_project`<br>Cột: ✅`sum_mm_worklog` (Worklog), ✅`sum_man_month` (MM kế hoạch)<br>Kèm ⚠️`worklog_by_version` | Đặt cạnh nhau **công thực tế đã log** và **man-month được cấp**. Khoảng cách giữa hai cột chính là phần nguồn lực chưa quy đổi được thành sản phẩm. | Tạo Bar chart nhóm (clustered) từ `qtsx_v2_ulnl_worklog_by_version_release` join `qtsx_v2_man_month_qa` và `qtsx_v2_project`: trục X `project_name` + tháng, hai cột giá trị là metric `sum_mm_worklog` và `sum_man_month`. Thêm metric `worklog_by_version` vào tooltip. Đặt tiêu đề "HS Load người theo version release SPDV". |

---

## D6 — Chỉ tiêu OS

**Mục đích:** Nhánh riêng cho **outsourcing**. Đo phiếu yêu cầu (PYC) giao cho đối tác ngoài: có đúng hạn không, chất lượng bàn giao thế nào, man-month cam kết so với man-month thực tế ra sao, và bao nhiêu phần trong đó thực sự được release.

**Bộ lọc trang:** khoảng ngày, tháng, đối tác (`qtsx_v2_pyc_os.partner`), trung tâm, sản phẩm.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | **Tỷ lệ PYC OS đúng hạn** · Big value | `qtsx_v2_pyc_os` · ✅`pass_rate_display`<br>*(= số PYC đánh giá Pass / tổng PYC)* · tháng mới nhất | Phần trăm phiếu yêu cầu OS được đối tác giao đúng hạn. Chỉ số cốt lõi để đánh giá đối tác. | Tạo Big value từ `qtsx_v2_pyc_os`, metric `pass_rate_display`, lọc tháng mới nhất, định dạng phần trăm. Đặt tiêu đề "Tỷ lệ PYC OS đúng hạn". |
| 2 | **Bug OS** · Big value | `qtsx_v2_bug_os` · ⚠️`hien_thi_ti_le_kpi` · tháng mới nhất | Tỷ lệ bug trên khối lượng công OS. Đo chất lượng hàng đối tác giao. | Tạo Big value từ `qtsx_v2_bug_os`, metric `hien_thi_ti_le_kpi`, lọc tháng mới nhất. Đặt tiêu đề "Bug OS". |
| 3 | **Hiệu quả sử dụng OS** · Big value | `qtsx_v2_pyc_os` · ⚠️`hieu_su_dung_os` · tháng mới nhất | Man-month OS thực tế dùng so với cam kết. | Tạo Big value từ `qtsx_v2_pyc_os`, metric `hieu_su_dung_os`, lọc tháng mới nhất. Đặt tiêu đề "Hiệu quả sử dụng OS". |
| 4 | *(card phụ của chart 1)* · Big value | `qtsx_v2_pyc_os` · ⚠️`pass_rate_growth_display` | Tỷ lệ đúng hạn so với kỳ trước. | Tạo Big value từ `qtsx_v2_pyc_os`, metric `pass_rate_growth_display`, lọc tháng mới nhất. |
| 5 | *(card phụ của chart 2)* · Big value | `qtsx_v2_bug_os` · ⚠️`bug_os_rate_growth_display` | Bug rate OS so với kỳ trước. | Tạo Big value từ `qtsx_v2_bug_os`, metric `bug_os_rate_growth_display`, lọc tháng mới nhất. |
| 6 | *(card phụ của chart 3)* · Big value | `qtsx_v2_pyc_os` · ⚠️`pyc_growth_display` | Số lượng PYC so với kỳ trước. | Tạo Big value từ `qtsx_v2_pyc_os`, metric `pyc_growth_display`, lọc tháng mới nhất. |
| 7 | **PYC đúng hạn trung bình theo TT** · Mixed (bar + line) | `qtsx_v2_pyc_os` + `qtsx_v2_dim_date`<br>Cột: ✅`count_issue_key` (số PYC), ✅`count_rows_filtered` (số Pass)<br>Đường (trục phải): ⚠️`on_time` (% đúng hạn)<br>Trục X: năm + tháng | Hai cột cho biết **quy mô** (tổng phiếu, số phiếu đạt), đường cho biết **chất lượng** (% đúng hạn). Tháng ít phiếu mà tỷ lệ cao chưa chắc đáng mừng. | Tạo Mixed chart từ `qtsx_v2_pyc_os` join `qtsx_v2_dim_date`: trục X `date_value` gom theo năm–tháng, sắp xếp tăng dần. Cột trục trái là metric `count_issue_key` và `count_rows_filtered`. Đường trục phải là metric `on_time` định dạng phần trăm. Đặt tiêu đề "PYC đúng hạn trung bình theo TT". |
| 8 | **CHẤT LƯỢNG BUG RATE** · Bar | `qtsx_v2_bug_os` + `qtsx_v2_dim_date`<br>Trục X: năm + tháng · Giá trị ⚠️`ti_le_bug_mm` | Số bug trên mỗi man-month OS, theo tháng. Xu hướng đi lên là cảnh báo chất lượng đối tác đang xuống. | Tạo Bar chart từ `qtsx_v2_bug_os` join `qtsx_v2_dim_date`: trục X `date_value` theo năm–tháng (tăng dần), giá trị metric `ti_le_bug_mm`. Đặt tiêu đề "CHẤT LƯỢNG BUG RATE". |
| 9 | **QUẢN LÝ HIỆU SUẤT** · Mixed (bar + line) | `qtsx_v2_pyc_os` + `qtsx_v2_dim_date`<br>Cột: ✅`sum_mm_ttsx_du_kien` (MM cam kết), ✅`sum_mm_pyc_thuc_te` (MM thực tế)<br>Đường (trục phải): ✅`hieu_suat` = thực tế / cam kết | So sánh **man-month cam kết** với **man-month thực dùng** theo tháng, kèm tỷ lệ hiệu suất. Thực tế vượt cam kết nhiều = ước lượng sai hoặc phát sinh ngoài phạm vi. | Tạo Mixed chart từ `qtsx_v2_pyc_os` join `qtsx_v2_dim_date`: trục X `date_value` theo năm–tháng. Hai cột trục trái là metric `sum_mm_ttsx_du_kien` và `sum_mm_pyc_thuc_te`. Đường trục phải là metric `hieu_suat` định dạng phần trăm. Đặt tiêu đề "QUẢN LÝ HIỆU SUẤT". |
| 10 | **TỈ LỆ RELEASE** · Mixed (bar + line) | `qtsx_v2_pyc_os_done_release` + `qtsx_v2_pyc_os_thnt` + `qtsx_v2_dim_date`<br>Cột: ✅`sum_mm_pyc_thuc_te` của bảng *done release* (MM đã release) và của bảng *THNT* (MM đã nghiệm thu)<br>Đường (trục phải): ⚠️`release_pyc` | Trong khối lượng đã **nghiệm thu thành công**, bao nhiêu phần thực sự đã **release** ra ngoài. Chênh lệch là phần đang nằm chờ. | Tạo Mixed chart từ `qtsx_v2_pyc_os_done_release` và `qtsx_v2_pyc_os_thnt` join `qtsx_v2_dim_date`: trục X `date_value` theo năm–tháng. Cột trục trái là `qtsx_v2_pyc_os_done_release.sum_mm_pyc_thuc_te` (nhãn "MM released") và `qtsx_v2_pyc_os_thnt.sum_mm_pyc_thuc_te` (nhãn "MM đã nghiệm thu thành công"). Đường trục phải là metric `release_pyc` định dạng phần trăm. Đặt tiêu đề "TỈ LỆ RELEASE". |
| 11 | **SỐ LƯỢNG BUG** · Mixed (bar + line) | `qtsx_v2_bug_os` + `qtsx_v2_project` + `qtsx_v2_dim_date`<br>Cột: ⚠️`ti_le_bug_mm`<br>Đường (trục phải): ✅`count_issuekey`<br>Trục X: `project_name` + tháng | Ghép **tỷ lệ bug/MM** với **số bug tuyệt đối** theo từng sản phẩm. Sản phẩm nhỏ dễ có tỷ lệ cao dù số bug ít. | Tạo Mixed chart từ `qtsx_v2_bug_os` join `qtsx_v2_project` và `qtsx_v2_dim_date`: trục X `project_name` (tăng dần) + tháng. Cột là metric `ti_le_bug_mm`, đường trục phải là metric `count_issuekey`. Đặt tiêu đề "SỐ LƯỢNG BUG". |
| 12 | **Cycle time release** · Bar | `qtsx_v2_pyc_os_done_release`<br>Trục X `project_2` · Giá trị ✅`average_so_ngay_tu_nghiem_thu_den_release`<br>Lọc: trạng thái = `Released` | Sau khi nghiệm thu xong, mất bao nhiêu ngày nữa mới release được. Đây là độ trễ của khâu cuối, thường bị bỏ quên. | Tạo Bar chart từ `qtsx_v2_pyc_os_done_release`: trục X `project_2`, giá trị metric `average_so_ngay_tu_nghiem_thu_den_release` (2 chữ số thập phân), sắp xếp giảm dần. Lọc `issue_current_status_name = 'Released'`. Thêm `sum_so_ngay_tu_nghiem_thu_den_release` vào tooltip. Đặt tiêu đề "Cycle time release". |

---

## D7 — CHI TIẾT

**Mục đích:** Ba bảng tổng hợp "một dòng một project". Đây là trang dành cho người cần **con số thô** để đối chiếu, không phải để nhìn xu hướng. Mọi chỉ số của D3–D6 đều xuất hiện ở đây dưới dạng cột.

**Bộ lọc trang:** năm, quý, tháng, trung tâm, sản phẩm.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(bảng tổng hợp sức khỏe project)* · Table (pivot) | Hàng: `qtsx_v2_project.project_name`, `project_group`, `center`<br>Cột giá trị (18 cột): ⚠️`kpi_mm_per_bug` (% BUG), ✅`bug_count` (Bug P), ✅`average_release_frequency` (TS Release), ✅`average_days_to_done` (Cycle Time), ✅`sum_ulnl_mm` (ULNL), ✅`sum_man_month` (MM), ⚠️`hieu_su_dung_source_luc_ee` (HS Load Người), ✅`sum_mm_worklog` (Worklog MM), ⚠️`nang_xuat_lao_dong` (% Worklog), ⚠️`kpi_mm_per_defect` (% Defect), ✅`done_charter` (% Done charter), ⚠️`hs_load_nguoi_by_versionrelease`, ⚠️`worklog_by_version`, ✅`rework_version` (% Rework), ✅`average_days_to_done_v2` (Leadtime), ⚠️`rate_bug_p` | Bảng "khám sức khỏe" đầy đủ cho từng sản phẩm: chất lượng, tiến độ, nguồn lực nằm cạnh nhau. Dùng để chấm điểm và xếp hạng project cuối tháng. | Tạo Table pivot trên Lightdash gộp các model `qtsx_v2_project`, `qtsx_v2_bug`, `qtsx_v2_bug_productions`, `qtsx_v2_dgcl_ttk`, `qtsx_v2_cycel_time`, `qtsx_v2_lead_time`, `qtsx_v2_issue_ulnl`, `qtsx_v2_man_month_qa`, `qtsx_v2_worklog`, `qtsx_v2_defect`, `qtsx_v2_done_charter`, `qtsx_v2_ulnl_worklog_by_version_release`. Hàng: `center`, `project_group`, `project_name`. Cột giá trị theo thứ tự: `kpi_mm_per_bug`, `bug_count`, `average_release_frequency`, `average_days_to_done`, `average_days_to_done_v2`, `sum_ulnl_mm`, `sum_man_month`, `sum_mm_worklog`, `hieu_su_dung_source_luc_ee`, `nang_xuat_lao_dong`, `kpi_mm_per_defect`, `done_charter`, `hs_load_nguoi_by_versionrelease`, `worklog_by_version`, `rework_version`, `rate_bug_p`. Đặt tiêu đề "Tổng hợp sức khỏe sản xuất theo project". |
| 2 | *(bảng tổng hợp OS theo project)* · Table (pivot) | Hàng: `project_name`, `project_group`, `center`<br>Cột: ✅`sum_mm_pyc_thuc_te` (MM PYC OS release), ✅`sum_mm_worklog` (MM Worklog release), ✅`sum_ulnl_mm` (MM ULNL release), ✅`sum_man_month` (MM), ✅`count_distinct_issue_key` (Số lần release ĐGTK), ✅`average_so_ngay_tu_nghiem_thu_den_release` (Cycle time), ✅`sum_mm_pyc_thuc_te` từ THNT (MM PYC OS1), ✅`sum_mm_backlog` (MM Tồn), ✅`average_so_ngay_create_ngay_ttsx_commit`, ⚠️`release_pyc` | Đối chiếu dòng chảy man-month của OS qua từng chặng: cam kết → nghiệm thu → release → tồn đọng. Cột "MM Tồn" cho biết bao nhiêu công đang mắc kẹt. | Tạo Table pivot gộp `qtsx_v2_project`, `qtsx_v2_pyc_os`, `qtsx_v2_pyc_os_done_release`, `qtsx_v2_pyc_os_thnt`, `qtsx_v2_ulnl_worklog_by_version_release`, `qtsx_v2_man_month_qa`, `qtsx_v2_dgcl_ttk`. Hàng: `center`, `project_group`, `project_name`. Cột giá trị: `qtsx_v2_pyc_os_done_release.sum_mm_pyc_thuc_te`, `qtsx_v2_ulnl_worklog_by_version_release.sum_mm_worklog`, `qtsx_v2_ulnl_worklog_by_version_release.sum_ulnl_mm`, `sum_man_month`, `count_distinct_issue_key`, `average_so_ngay_tu_nghiem_thu_den_release`, `qtsx_v2_pyc_os_thnt.sum_mm_pyc_thuc_te`, `sum_mm_backlog`, `average_so_ngay_create_ngay_ttsx_commit`, `release_pyc`. Đặt tiêu đề "Dòng chảy man-month OS theo project". |
| 3 | *(bảng defect theo version)* · Table (pivot) | Hàng: `project_name`, `qtsx_v2_version.vname`<br>Cột: ✅`count_distinct_issuekey` (Số Defect), ✅`sum_mm_worklog` (MM LOGWORK), ✅`average_days_to_done_v2` (Lead time) | Xuống tới mức **từng version**: bản này có bao nhiêu defect, ngốn bao nhiêu công, lead time bao lâu. Dùng khi cần truy nguyên một bản release có vấn đề. | Tạo Table pivot gộp `qtsx_v2_project`, `qtsx_v2_version`, `qtsx_v2_defect`, `qtsx_v2_worklog`, `qtsx_v2_lead_time`. Hàng: `project_name`, `vname`. Cột giá trị: `count_distinct_issuekey`, `sum_mm_worklog`, `average_days_to_done_v2`. Đặt tiêu đề "Defect và công theo version". |

---

## D8 — Chi tiết version report

**Mục đích:** Trang tra cứu defect gắn với version. Thường mở ra từ D7-3 khi thấy một version bất thường.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(defect theo version)* · Table (pivot) | Hàng: `qtsx_v2_project.project_name`, `qtsx_v2_version.vname`<br>Giá trị: ✅`count_distinct_issuekey` | Đếm defect theo từng version của từng sản phẩm. | Tạo Table pivot gộp `qtsx_v2_project`, `qtsx_v2_version`, `qtsx_v2_defect`: hàng `project_name` và `vname`, giá trị metric `count_distinct_issuekey`. Đặt tiêu đề "Số defect theo version". |
| 2 | **Defect** · Table | `qtsx_v2_defect`<br>Cột: `issuekey`, `created`, `summary` | Danh sách defect cụ thể — mã ticket, ngày tạo, tiêu đề. Đây là điểm cuối để nhảy sang Jira. | Tạo Table từ `qtsx_v2_defect` với các cột `issuekey`, `created`, `summary`. Đặt tiêu đề "Defect". |

---

## D9 — Tooltip SK

**Mục đích:** Trang này **không dùng độc lập**. Trong Power BI nó là tooltip page — bung ra khi người dùng rê chuột vào một ô KPI sản phẩm ở D0/D1. Lightdash không có tooltip page, nên cách làm tương đương là tạo một saved chart riêng và liên kết bằng *drill-through* / mở dashboard chi tiết.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(chi tiết KPI sản phẩm)* · Table | `qtsx_v2_kpikqi_nocodb`<br>Cột: `customer_name`, `end_time`, `kpiname`, `evaluation`, ✅`sum_result`, ✅`sum_target`, `spdv`, `condition`, `evaluation_value`, `criteria_period_type` | Với một khách hàng + sản phẩm cụ thể: từng chỉ tiêu target bao nhiêu, đạt bao nhiêu, kết luận đạt hay không. | Tạo Table từ `qtsx_v2_kpikqi_nocodb` với các cột `customer_name`, `end_time`, `kpiname`, `spdv`, `condition`, `sum_target`, `sum_result`, `evaluation`, `evaluation_value`, `criteria_period_type`. Dùng làm chart chi tiết mở ra từ các chart KPI sản phẩm. Đặt tiêu đề "Chi tiết KPI sản phẩm". |

---

## D10 — Tooltip DV

**Mục đích:** Tương tự D9 nhưng cho KPI **dịch vụ**.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(chi tiết KPI dịch vụ)* · Table | `qtsx_v2_kpikqi_all`<br>Cột: `metric_name`, `report_date`, `result`, `target`, `condition`, `evaluation_v2`, `unit_of_measure`, `spdv` | Chi tiết từng chỉ tiêu dịch vụ trong kỳ báo cáo đang chọn. | Tạo Table từ `qtsx_v2_kpikqi_all` với các cột `metric_name`, `spdv`, `report_date`, `target`, `condition`, `result`, `unit_of_measure`, `evaluation_v2`. Dùng làm chart chi tiết mở ra từ các chart KPI dịch vụ. Đặt tiêu đề "Chi tiết KPI dịch vụ". |

---

## D11 — PYC

**Mục đích:** Danh sách phiếu yêu cầu OS ở mức bản ghi, phục vụ tra cứu và đối soát với đối tác.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(danh sách PYC OS)* · Table | `qtsx_v2_pyc_os`<br>Cột: `issue_key`, `issue_summary`, `project_2`, ✅`sum_so_ngay_tu_nghiem_thu_den_release`, `acceptance_completion_date`, `release_date`, `issue_current_status_name` | Mỗi dòng một phiếu: mã, nội dung, dự án, ngày hoàn thành nghiệm thu, ngày release, số ngày kẹt giữa hai mốc, trạng thái hiện tại. | Tạo Table từ `qtsx_v2_pyc_os` với các cột `issue_key`, `issue_summary`, `project_2`, `acceptance_completion_date`, `release_date`, `sum_so_ngay_tu_nghiem_thu_den_release` (nhãn "Cycle time"), `issue_current_status_name`. Đặt tiêu đề "Danh sách PYC OS". |

---

## D12 — Page review

**Mục đích:** Trang đối soát nội bộ. Không dành cho lãnh đạo — dùng khi số ở D6 lệch so với hệ thống nguồn và cần lần ngược ra bản ghi gốc.

**Bộ lọc trang:** khoảng ngày, tháng, kết quả đánh giá PYC (`qtsx_v2_pyc_os.evaluation`).

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(PYC và mốc cam kết)* · Table | `qtsx_v2_pyc_os`<br>Cột: `issue_key`, `issue_summary`, `ttsx_commit_date`, `evaluation`, `acceptance_completion_date`, `issue_current_status_name` | So ngày TTSX cam kết với ngày hoàn thành nghiệm thu thực tế, để kiểm chứng đánh giá Pass/Fail có đúng không. | Tạo Table từ `qtsx_v2_pyc_os` với các cột `issue_key`, `issue_summary`, `ttsx_commit_date`, `acceptance_completion_date`, `evaluation`, `issue_current_status_name`. Đặt tiêu đề "Đối soát PYC theo mốc cam kết". |
| 2 | *(MM release theo project)* · Table (pivot) | Hàng `qtsx_v2_project.project_name`<br>Cột: ⚠️`release`, ✅`sum_mm_pyc_thuc_te`, ✅`mm_thuc_te_tinh_release` *(chỉ tính trạng thái Released / Bàn giao TT Sản phẩm / ĐG TTK)* | Kiểm tra con số "% release" ở D6-10 bằng cách bung ra man-month từng project. | Tạo Table pivot gộp `qtsx_v2_project`, `qtsx_v2_pyc_os`, `qtsx_v2_pyc_os_done_release`: hàng `project_name`, cột giá trị `release`, `sum_mm_pyc_thuc_te`, `mm_thuc_te_tinh_release`. Đặt tiêu đề "Đối soát MM release theo project". |
| 3 | *(số bug OS theo project)* · Table (pivot) | Hàng `project_name` · Giá trị ✅`count_issuekey` | Đếm bug OS từng project để đối chiếu với D6-11. | Tạo Table pivot gộp `qtsx_v2_project` và `qtsx_v2_bug_os`: hàng `project_name`, giá trị metric `count_issuekey`. Đặt tiêu đề "Số bug OS theo project". |
| 4 | *(danh sách bug OS)* · Table | `qtsx_v2_bug_os`<br>Cột: `created`, `projectname`, `issuekey`, `summary` | Bản ghi bug OS gốc. | Tạo Table từ `qtsx_v2_bug_os` với các cột `created`, `projectname`, `issuekey`, `summary`. Đặt tiêu đề "Danh sách bug OS". |
| 5 | *(MM theo đợt release)* · Table (pivot) | Hàng `qtsx_v2_pyc_os_thnt.project_2`<br>Giá trị ✅`sum_mm_release25`, ✅`sum_mm_release26` | So man-month của hai đợt release liền kề (release 25 và 26) cho từng project. | Tạo Table pivot từ `qtsx_v2_pyc_os_thnt`: hàng `project_2`, cột giá trị `sum_mm_release25` và `sum_mm_release26`. Đặt tiêu đề "MM theo đợt release". |
| 6 | *(danh sách PYC đã nghiệm thu)* · Table | `qtsx_v2_pyc_os_thnt`<br>Cột: `project_2`, `issue_key`, `issue_summary`, `acceptance_completion_date`, `release_date` | Bản ghi các phiếu đã nghiệm thu thành công, kèm hai mốc ngày để tính độ trễ release. | Tạo Table từ `qtsx_v2_pyc_os_thnt` với các cột `project_2`, `issue_key`, `issue_summary`, `acceptance_completion_date`, `release_date`. Đặt tiêu đề "PYC đã nghiệm thu thành công". |

---

## D13 — Page 1

**Mục đích:** Trang nháp còn sót lại trong file gốc, nội dung trùng với D7-2 và D12. Khi dựng lại trên Lightdash **nên bỏ trang này**, chỉ giữ nếu có người đang dùng thật.

**Bộ lọc trang:** quý, tuần, sản phẩm.

| # | Chart | Dữ liệu dùng | Ý nghĩa | Prompt tạo chart |
|---|---|---|---|---|
| 1 | *(tổng hợp OS theo project)* · Table (pivot) | 🔁 Trùng hoàn toàn với D7-2 | Không có gì khác D7-2 ngoài bộ lọc trang là quý/tuần thay vì tháng. | 🔁 Dùng lại chart "Dòng chảy man-month OS theo project" (D7-2), chỉ đổi dashboard filter sang quý và tuần. |
| 2 | *(danh sách PYC đã release)* · Table | `qtsx_v2_pyc_os_done_release`<br>Cột: `issue_key`, `issue_summary`, ✅`sum_mm_pyc_thuc_te` | Bản ghi các phiếu đã release, kèm man-month. | Tạo Table từ `qtsx_v2_pyc_os_done_release` với các cột `issue_key`, `issue_summary`, `sum_mm_pyc_thuc_te` (nhãn "MM"). Đặt tiêu đề "PYC đã release". |
| 3 | *(danh sách PYC đã nghiệm thu)* · Table | `qtsx_v2_pyc_os_thnt`<br>Cột: `issue_key`, `issue_summary`, ✅`sum_mm_pyc_thuc_te` | 🔁 Rút gọn của D12-6. | Tạo Table từ `qtsx_v2_pyc_os_thnt` với các cột `issue_key`, `issue_summary`, `sum_mm_pyc_thuc_te`. Đặt tiêu đề "PYC đã nghiệm thu". |

---

## Gợi ý gom lại khi dựng trên Lightdash

Báo cáo gốc có nhiều trùng lặp. Nếu muốn bản Lightdash gọn hơn, có thể gom như sau:

| Gộp gì | Lý do |
|---|---|
| D0 giữ nguyên, nhưng **28 card rời → 6 Big value + 3 bảng nhỏ** | Mỗi KPI hiện đang chiếm 4–5 card (giá trị, % thay đổi, số project đạt, số project không đạt). Trên Lightdash một bảng 4 dòng dễ đọc hơn nhiều card rời. |
| D3, D4, D5 dùng chung **một dashboard "Sức khỏe sản xuất"** với 3 tab | Ba trang này có cấu trúc giống hệt nhau (3 card KPI + 3 lát cắt: thời gian / trung tâm / sản phẩm). |
| D8, D13 **bỏ hẳn** | Nội dung đã nằm trong D7 và D12. |
| D9, D10 chuyển thành **saved chart dùng cho drill-through** | Lightdash không có tooltip page. |
| D1-9 **bỏ** (bản sao của D1-2) | Chỉ cần kéo cùng một saved chart vào dashboard hai lần nếu vẫn muốn hiện hai chỗ. |



