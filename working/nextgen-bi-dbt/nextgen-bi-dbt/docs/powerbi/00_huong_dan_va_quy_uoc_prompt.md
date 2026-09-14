# Hướng dẫn đọc tài liệu & quy ước viết prompt Lightdash

Tài liệu trong thư mục này mô tả lại **toàn bộ dashboard và chart** đã bóc tách từ file Power BI,
kèm **câu prompt sẵn dùng** để dựng lại chart đó trên Lightdash (chạy trên dbt).

## Danh sách tài liệu

| File | Nội dung | Quy mô |
|---|---|---|
| [01_qtsx_v2_mo_ta_dashboard_va_prompt.md](01_qtsx_v2_mo_ta_dashboard_va_prompt.md) | Báo cáo **QTSX V2** — quản trị sản xuất góc nhìn Khối/Trung tâm | 14 trang, 133 chart, 57 bộ lọc |
| [02_quan_tri_san_xuat_mo_ta_dashboard_va_prompt.md](02_quan_tri_san_xuat_mo_ta_dashboard_va_prompt.md) | Báo cáo **Quản trị sản xuất (1)** — KPIKQI khách hàng + OS + chất lượng dịch vụ | 49 trang, 181 chart, 277 bộ lọc |

Nguồn dữ liệu gốc: `powerbi_output/qtsx_v2/` và `powerbi_output/production_management_1/`.

## Cách đọc mỗi section

Mỗi **dashboard** (tương ứng 1 trang Power BI) là một section, gồm:

- **Mục đích** — trang này trả lời câu hỏi gì của người dùng.
- **Bộ lọc trang** — các slicer của Power BI. Trên Lightdash, đây là *dashboard filter*, không phải chart.
- **Bảng chart** — mỗi dòng là một chart, với 5 cột:

| Cột | Ý nghĩa |
|---|---|
| `#` | Số thứ tự chart trong trang (đọc từ trên xuống, trái sang phải theo layout gốc) |
| `Chart` | Tên hiển thị + loại biểu đồ tương ứng trên Lightdash |
| `Dữ liệu dùng` | Model dbt, dimension (trục/nhóm), metric (giá trị), và bộ lọc cứng gắn sẵn trong chart |
| `Ý nghĩa` | Giải thích nghiệp vụ: con số này nói lên điều gì, nhìn vào để làm gì |
| `Prompt tạo chart` | Câu lệnh dán thẳng vào công cụ tạo chart Lightdash |

## Ký hiệu trong tài liệu

| Ký hiệu | Nghĩa |
|---|---|
| ⚠️ | Metric này là **DAX measure không dịch tự động được**. Kể từ 2026-08, phần lớn ⚠️ có `priority ≥ 2` đã được viết tay — tra mục **"Trạng thái metric"** ở đầu tài liệu 01/02 để biết metric đó đã có chưa và nằm ở model nào (nhiều metric đã **đổi model** so với prompt bên dưới). Phần chưa làm vẫn liệt kê ở `layout/measures_todo.yml`. |
| ✅ | Metric đã được sinh tự động, dùng ngay được. |

## Metric viết tay để ở đâu

`powerbi_tools/run.py` **ghi đè** `dbt/models/*.sql` và `*__schema.yml` mỗi lần chạy.
Đừng sửa tay vào đó. Ba đường an toàn (chi tiết ở `powerbi_tools/README.md` mục 6a):

| Cách | File | Dùng khi |
|---|---|---|
| Thêm metric / join / sửa kiểu dimension | `powerbi_output/<domain>/metrics_overrides.yml` | Đa số trường hợp. Được merge vào `meta` của model khi sinh. |
| Thay thân model `.sql` | `powerbi_output/<domain>/sql_overrides/<model>.sql` | Bảng tính toán bằng DAX mà `.tsv` export ra rỗng (`Dim_Date`, `KPIKQI_ALL`). |
| Model mới hoàn toàn | `dbt/models/<tên mới>.sql` + `__schema.yml` | Cần đổi **grain**: fan-out theo nhiều cột ngày để dịch `USERELATIONSHIP`, hoặc gộp trước nhiều bảng để tránh fan-out khi chia hai đại lượng ở hai bảng. |

**Ba cái bẫy hay gặp khi viết tay:**

1. **`USERELATIONSHIP` ⇒ không dùng được một join dim_date.** Một measure một mốc ngày; Lightdash chỉ có 1 join active. Phải fan-out bảng fact thành (bản ghi × mốc ngày) rồi cho mỗi metric lọc theo `date_role`.
2. **Chia hai đại lượng ở hai bảng ⇒ fan-out.** Khai `join` rồi viết `SUM(a)/SUM(b)` sẽ làm mẫu số bị nhân lên theo số dòng của bảng kia. Phải gộp từng bảng về cùng grain **trước** rồi mới `FULL OUTER JOIN`.
3. **Power BI so chuỗi không phân biệt hoa thường, Trino thì có.** Mọi `SWITCH`/`IN` trên chuỗi phải `upper()` hai vế, nếu không sẽ mất dòng một cách âm thầm.
| 🔁 | Chart lặp lại y hệt một chart đã mô tả ở trang khác, chỉ khác bộ lọc. |
| 🐍 | Visual gốc là Python visual — Lightdash không hỗ trợ, cần thay bằng loại chart khác. |

## Quy đổi loại biểu đồ Power BI → Lightdash

| Power BI | Lightdash | Ghi chú |
|---|---|---|
| `cardVisual`, `card` | **Big value** | Mỗi card chỉ hiện 1 số. Card gốc có 2–3 số (Tổng / Đạt / Không đạt) phải tách thành nhiều Big value, hoặc dùng 1 bảng nhỏ. |
| `columnChart`, `clusteredColumnChart` | **Bar chart** (dọc) | |
| `hundredPercentStackedColumnChart` | **Bar chart** + bật *Stack* và *Normalize to 100%* | |
| `hundredPercentStackedBarChart` | **Bar chart** + *Horizontal* + *Stack 100%* | |
| `lineChart` | **Line chart** | |
| `pieChart`, `donutChart` | **Pie chart** | Donut = pie bật *Donut* |
| `lineStackedColumnComboChart`, `lineClusteredColumnComboChart` | **Mixed chart** | Cột trên trục trái, đường trên trục phải |
| `tableEx` | **Table** | Bảng phẳng, liệt kê bản ghi |
| `pivotTable` | **Table** + bật *Pivot* | Kéo dimension lên phần cột |
| `pythonVisual` | ❌ Không có tương đương | Thường là scatter/bubble nhiều chiều → thay bằng **Scatter** hoặc **Table** |

## Quy ước viết prompt

Prompt trong tài liệu tuân theo khuôn mẫu sau, để công cụ tạo chart hiểu đúng ngay lần đầu:

> Tạo **[loại chart]** trên Lightdash từ model `[model dbt]`.
> Trục X / lát cắt: dimension `[...]`. Giá trị: metric `[...]`.
> Nhóm màu (series): `[...]`. Bộ lọc: `[...]`. Sắp xếp: `[...]`.
> Đặt tiêu đề "[...]".

Vài lưu ý khi dùng lại prompt:

1. **Tên model đã có tiền tố domain** (`qtsx_v2_...`, `production_management_1_...`) đúng như trong `dbt/models/`. Không cần đổi.
2. **Join giữa các bảng đã khai báo sẵn** trong `meta.joins` của model. Prompt chỉ cần nêu tên field, Lightdash tự nối bảng.
3. **Bộ lọc "kỳ mới nhất"** (Power BI dùng TopN theo `YearMonth` / `WeekYear`) trên Lightdash nên làm bằng dashboard filter kiểu *relative date* hoặc thêm dimension `is_latest_period` vào model.
4. **Metric có ⚠️** thì phải bổ sung SQL vào `meta.metrics` của model dbt trước, nếu không Lightdash sẽ báo không tìm thấy field.
5. **Chart có trục phụ (Y2)** thì trong Lightdash phải vào tab *Series* và chuyển field đó sang *Right axis*.

## Thứ tự khuyến nghị khi dựng lại

1. Chạy `dbt run` để có đủ model, kiểm tra bảng lên đúng trong Lightdash.
2. Bổ sung SQL cho các metric ⚠️ theo thứ tự ưu tiên trong `measures_todo.yml` (sắp theo số chart đang dùng).
3. Dựng chart theo từng dashboard, bắt đầu từ trang tổng quan vì các trang chi tiết dùng lại rất nhiều metric của trang này.
4. Tạo dashboard, kéo chart vào, rồi mới thêm dashboard filter (mục *Bộ lọc trang*).
