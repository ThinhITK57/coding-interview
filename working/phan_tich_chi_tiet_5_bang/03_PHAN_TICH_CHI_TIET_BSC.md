# BẢNG TỪ ĐIỂN DỮ LIỆU & PHÂN TÍCH CHI TIẾT: THỰC THỂ BSC (BALANCED SCORECARD)
**Hệ thống Quản trị Mục tiêu Chiến lược & Giao ban Điều hành Doanh nghiệp (Planview Clarizen Lakehouse)**
**Nguồn dữ liệu gốc**: `D:\dataguystory\coding-interview-university\bsc_data_raw.txt` (53 trường)

---

## 1. TỔNG QUAN VỀ THỰC THỂ BSC (BALANCED SCORECARD)

Trong hệ sinh thái quản trị doanh nghiệp, **BSC (Thẻ điểm cân bằng)** đại diện cho tầng **Mục tiêu Chiến lược Cấp cao (Strategic Objectives & Directives)**. 

Bảng BSC đóng vai trò là "Ngọn hải đăng" định hướng toàn bộ hoạt động:
1. **Nghị quyết & Kết luận Giao ban Ban Lãnh đạo**: Mọi chỉ đạo điều hành lớn từ các phiên họp giao ban được mô hình hóa thành các Mục tiêu BSC chiến lược.
2. **Phân rã Mục tiêu Phân cấp (Cascading Objectives)**: Mục tiêu Tập đoàn $\rightarrow$ Mục tiêu Khối $\rightarrow$ Mục tiêu Phòng ban thông qua trường `ParentObjective`.
3. **Cơ chế Trọng số Chiến lược (Strategic Weighting)**: Phân bổ tỷ lệ % quan trọng (`Weight`, `C_TotalWeightPercentage`) để đánh giá mức độ đóng góp của từng bộ phận.
4. **Đo lường Đa kịch bản (Scenario Rollup M / N)**: Tự động tổng hợp tiến độ và doanh thu kế hoạch từ các Dự án và Chỉ tiêu cấp dưới (`C_PlannedRevenueMRollup`, `C_TotPlanContRollup`).
5. **Chuẩn hóa Điểm số (Normalized Scoring)**: Tính toán điểm KPI tổng hợp (`C_NormalizedScore`) phục vụ xếp loại thi đua tổ chức.

---

## 2. PHÂN NHÓM VÀ GIẢI THÍCH CHI TIẾT 53 TRƯỜNG DỮ LIỆU

### NHÓM 1: ĐỊNH DANH & CẤU TRÚC PHÂN CẤP MỤC TIÊU CHIẾN LƯỢC

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `SYSID` | String (PK) | Mã định danh duy nhất của mục tiêu BSC (ví dụ: `/Objective/BSC-2026-01`). |
| `Name` | String | Tên mục tiêu chiến lược / Tiêu đề nghị quyết giao ban. |
| `Description` / `C_Description` | Text | Nội dung thuyết minh chi tiết về mục tiêu, định hướng chỉ đạo của Lãnh đạo. |
| `ExternalID` | String | Mã số văn bản, số hiệu nghị quyết HĐQT hoặc biên bản họp giao ban. |
| `EntityType` | String | Luôn mang giá trị cố định `"Objective"` (hoặc `"BSC"`). |
| `ParentObjective` | EntityRef | **Khóa ngoại trỏ đến Mục tiêu cha cấp trên** (Phân cấp từ Tập đoàn xuống Khối/Phòng). |
| `SubObjectivesCount` | Integer | Số lượng mục tiêu con trực thuộc mục tiêu này. |
| `AssociatedItem` | EntityRef | **Khóa ngoại trỏ đến Dự án (`Project`) hoặc Chương trình (`Program`)** gắn với mục tiêu này. |
| `C_AssociatedDepartmentsPortfolio`| String | Danh mục các Khối / Phòng ban cùng tham gia thực hiện mục tiêu. |
| `C_Department` | EntityRef | Khối / Phòng ban chịu trách nhiệm chính. |
| `C_ObjectiveType` | String | Phân loại mục tiêu theo 4 viễn cảnh BSC: Tài chính, Khách hàng, Quy trình nội bộ, Học hỏi & Phát triển. |
| `C_RecordType` | String | Loại bản ghi mục tiêu (Chiến lược công ty, Mục tiêu năm, Chỉ đạo đột xuất). |

---

### NHÓM 2: TRỌNG SỐ & PHÂN BỔ MỤC TIÊU (STRATEGIC WEIGHTS)

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `Weight` | Double | Trọng số của mục tiêu này trong toàn bộ thẻ điểm BSC (ví dụ: 20.0%). |
| `C_TotalWeightPercentage` | Double | Tổng tỷ lệ phần trăm trọng số của toàn bộ các mục tiêu con trực thuộc (phải đạt 100%). |
| `C_TotalWeightAssignments` | Double | Tổng trọng số đã được phân bổ xuống các bảng giao việc (`Assignment`). |
| `C_AssociatedContributions` | String | Danh mục các khoản đóng góp chỉ tiêu từ các đơn vị thành viên. |

---

### NHÓM 3: TIẾN ĐỘ, KẾT QUẢ ĐẠT ĐƯỢC & CHUẨN HÓA ĐIỂM (PROGRESS & SCORING)

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `PlannedPercent` | Percentage | % tiến độ cần đạt được theo kế hoạch tại thời điểm xét duyệt. |
| `ActualPercent` | Percentage | % tiến độ thực tế đã hoàn thành của mục tiêu. |
| `PercentOfPlan` | Percentage | **Tỷ lệ hoàn thành so với kế hoạch**: $\frac{\text{ActualPercent}}{\text{PlannedPercent}} \times 100\%$. |
| `C_CalculateProgress` | Boolean | Cờ kích hoạt tự động tính tiến độ từ các chỉ tiêu Target cấp dưới. |
| `C_ObjectiveProgressM` | Percentage | Tiến độ mục tiêu theo kịch bản Mục tiêu cam kết (M). |
| `C_ObjectiveProgressN` | Percentage | Tiến độ mục tiêu theo kịch bản Mục tiêu phấn đấu (N). |
| `C_N1ResultResponse` | Text | Nội dung phản hồi, giải trình kết quả thực hiện kỳ trước (N-1). |
| `C_NormalizedScore` | Double | **Điểm số chuẩn hóa (KPI Score)**: Quy đổi kết quả thực hiện về thang điểm chuẩn (0 - 100) để đánh giá thi đua. |

---

### NHÓM 4: TỔNG HỢP DOANH THU & CHỈ TIÊU KẾ HOẠCH (ROLLUP METRICS)
Hệ thống trường Rollup tự động gom số liệu từ các Dự án và Chỉ tiêu con:

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `C_PlannedRevenueMRollup` | Currency | Doanh thu kế hoạch (Kịch bản M) tổng hợp tự động từ toàn bộ Dự án/Task trực thuộc. |
| `C_PlannedRevenueNRollup` | Currency | Doanh thu kế hoạch (Kịch bản N) tổng hợp từ cấp dưới. |
| `C_TotalPlannedObjectivesM` / `N` | Integer | Tổng số lượng mục tiêu kế hoạch được phân bổ theo từng kịch bản. |
| `C_TotalPlannedTargetM` / `N` | Integer | Tổng số lượng chỉ tiêu định lượng (Target) đã gắn vào mục tiêu này. |
| `C_TotPlanContRollupM` / `N` | Double | Tổng giá trị đóng góp kế hoạch của các phòng ban được rollup lên. |

---

### NHÓM 5: THỜI GIAN, TRẠNG THÁI, QUẢN TRỊ & AUDIT TRAIL

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `StartDate` | Date | Ngày bắt đầu kỳ đánh giá mục tiêu chiến lược (ví dụ: 01/01/2026). |
| `EndDate` | Date | Ngày kết thúc kỳ đánh giá chiến lược (ví dụ: 31/12/2026). |
| `State` | EntityRef | Trạng thái vòng đời mục tiêu (`/State/Draft`, `/State/Active`, `/State/Completed`). |
| `Status` | String | Đánh giá hiện trạng: Đang thực hiện, Hoàn thành, Hủy bỏ. |
| `C_Assignee` | EntityRef | **Lãnh đạo / Cá nhân chịu trách nhiệm chính** trước HĐQT về mục tiêu này. |
| `C_Reporter` | EntityRef | Cán bộ đầu mối phụ trách tổng hợp số liệu và báo cáo tiến độ. |
| `EntityOwner` | EntityRef | Người sở hữu bản ghi trong hệ thống. |
| `CreatedBy` / `CreatedOn` | EntityRef/Date | Người tạo và thời điểm tạo mục tiêu. |
| `LastUpdatedBy` / `LastUpdatedOn` | EntityRef/Date | Người cập nhật và **Watermark gia số để cào dữ liệu tăng dần**. |
| `LastUpdatedBySystemOn` | DateTime | Dấu thời gian hệ thống tự động cập nhật số liệu rollup. |
| `C_TriggerWFR` / `C_TriggerWR` | Boolean | Cờ kích hoạt quy trình Workflow phê duyệt hoặc tái phân bổ trọng số. |
| `AttachmentsCount` / `NotesCount` / `EmailsCount` | Integer | Số lượng tài liệu đính kèm (Biên bản giao ban, Nghị quyết, Quyết định). |

---

## 3. MỐI QUAN HỆ CỦA BSC VỚI 4 BẢNG CÒN LẠI

Bảng BSC đóng vai trò là **Đỉnh của Tháp Quản trị**:
1. **Liên kết với `Project`**: Thông qua `AssociatedItem` và `Project.C_AssociatedObjective`. Dự án sinh ra là để thực hiện một mục tiêu BSC cụ thể. Doanh thu của Dự án được rollup lên `C_PlannedRevenueMRollup`.
2. **Liên kết với `Target` (1-N)**: Một mục tiêu BSC sẽ được lượng hóa thành nhiều chỉ tiêu cụ thể trong bảng `Target` thông qua trường `Target.AssociatedObjective = BSC.SYSID`.
3. **Liên kết với `Assignment`**: Trọng số mục tiêu BSC được phân bổ thành các phiếu giao việc và cam kết KPI trong bảng `Assignment` qua trường `C_TotalWeightAssignments`.
4. **Liên kết với `Task`**: Các kết quả bàn giao (`Deliverable`) và tiến độ của Task con bên dưới sẽ tổng hợp lũy tiến lên Dự án rồi phản ánh trực tiếp vào % hoàn thành của BSC (`ActualPercent`).
