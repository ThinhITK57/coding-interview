# BẢNG TỪ ĐIỂN DỮ LIỆU & PHÂN TÍCH CHI TIẾT: THỰC THỂ TARGET
**Hệ thống Quản trị Chỉ tiêu Định lượng & Đo lường Hiệu quả (Planview Clarizen Lakehouse)**
**Nguồn dữ liệu gốc**: `D:\dataguystory\coding-interview-university\target_raw_data.txt` (122 trường)

---

## 1. TỔNG QUAN VỀ THỰC THỂ TARGET

Trong hệ sinh thái quản trị hiệu suất doanh nghiệp, **Target (Chỉ tiêu định lượng / Key Results / KPIs)** là công cụ đo lường mức độ hoàn thành của các Mục tiêu chiến lược (BSC) và Dự án.

Nếu như BSC trả lời câu hỏi *"Chúng ta muốn đi về đâu?"*, thì Target trả lời câu hỏi *"Bằng con số cụ thể nào để biết chúng ta đã đạt được mục tiêu?"*.

Bảng Target tích hợp 5 năng lực quản trị cốt lõi:
1. **Lượng hóa Kết quả Đầu ra (Quantitative Goal Setting)**: Định nghĩa chỉ tiêu qua Giá trị ban đầu (`InitialValue`), Giá trị cam kết (`TargetValue`), Đơn vị tính (`Unit`, `UnitType`, `UnitScale`) và Hạn hoàn thành (`TargetDate`).
2. **Đo lường Đa Kịch bản (Scenario Modeling M / N / S)**: Hỗ trợ kịch bản Mục tiêu Cam kết (M), Kịch bản Phấn đấu (N) và Kịch bản Tối thiểu (S) kèm toán tử so sánh (`C_TargetValueM/N/SOperator` như $\ge, \le, =$).
3. **Phân tích Khoảng cách & Độ lệch (Variance & Gap Analysis)**: Đo lường khoảng cách kế hoạch (`PlanningGap`), khoảng cách thực tế (`ActualGap`) và biến động ngày tháng (`C_DateVarianceM/N/S`).
4. **Hệ thống Trọng số & Chuẩn hóa Điểm số (Weighting & Normalization)**: Phân bổ trọng số chỉ tiêu trong phiếu giao việc (`C_AssignmentWeight`), tính điểm có trọng số (`WeightedActualPercent`) và chuẩn hóa điểm xếp loại (`C_NormalizedScore`).
5. **Ghi nhận Đóng góp Doanh thu (Revenue Contributions)**: Theo dõi đóng góp doanh thu trực tiếp (`DirectActualContributions`) và gián tiếp (`IndirectActualContributions`).

---

## 2. PHÂN NHÓM VÀ GIẢI THÍCH CHI TIẾT 122 TRƯỜNG DỮ LIỆU

### NHÓM 1: ĐỊNH DANH, CẤU TRÚC PHÂN CẤP & MẮT XÍCH LIÊN KẾT

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `SYSID` | String (PK) | Mã định danh duy nhất của chỉ tiêu (ví dụ: `/Target/TGT-2026-054`). |
| `Name` | String | Tên chỉ tiêu định lượng (ví dụ: "Tăng trưởng doanh thu 25%", "Độ khả dụng hệ thống 99.9%"). |
| `Description` / `C_TargetDescription` | Text | Thuyết minh phương pháp tính và tiêu chí đánh giá chỉ tiêu. |
| `C_TargetID` / `ExternalID` | String | Mã số chỉ tiêu quy chuẩn theo sổ tay KPI doanh nghiệp. |
| `EntityType` | String | Luôn mang giá trị cố định `"Target"`. |
| `ParentTarget` | EntityRef | **Khóa ngoại trỏ đến Chỉ tiêu cha cấp trên** (Chỉ tiêu công ty $\rightarrow$ Chỉ tiêu phòng ban). |
| `SubTargetsCount` / `C_SubTargetsCount` | Integer | Số lượng chỉ tiêu con trực thuộc. |
| `AssociatedObjective` | EntityRef | **Khóa ngoại trỏ trực tiếp đến Mục tiêu chiến lược trong bảng `BSC`** (`BSC.SYSID`). |
| `AssociatedItem` | EntityRef | **Khóa ngoại trỏ đến Dự án trong bảng `Project`** (`Project.SYSID`). |
| `C_AssociatedAssignment` | EntityRef | **Khóa ngoại trỏ đến Phiếu giao việc trong bảng `Assignment`** (`Assignment.SYSID`). |

---

### NHÓM 2: ĐỊNH NGHĨA CHỈ TIÊU & ĐƠN VỊ ĐO LƯỜNG (METRIC DEFINITION)

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `InitialValue` | Double | Mức giá trị xuất phát điểm ban đầu tại thời điểm bắt đầu kỳ đánh giá. |
| `TargetValue` | Double | **Chỉ tiêu giá trị đích cần phải đạt được** (ví dụ: 100 tỷ VND, 500 khách hàng). |
| `ActualValue` / `C_ActualValue` | Double | **Kết quả giá trị thực tế đã ghi nhận đạt được** tính đến ngày báo cáo. |
| `Unit` | String | Đơn vị tính (%, VND, USD, Hệ thống, Giờ, Người). |
| `UnitType` | String | Loại đơn vị (Số học, Tiền tệ, Phần trăm, Thời gian). |
| `UnitScale` | String | Quy mô tỷ lệ (Đơn vị, Hàng nghìn, Hàng triệu, Hàng tỷ). |
| `UnitDisplay` | String | Ký hiệu hiển thị trên biểu đồ và dashboard báo cáo. |
| `TargetType` | String | Loại chỉ tiêu (Càng cao càng tốt, Càng thấp càng tốt, Duy trì trong ngưỡng). |
| `C_Measurement` | String | Phương pháp đo kiểm (Đo trực tiếp từ hệ thống, Đánh giá hội đồng, Khảo sát). |

---

### NHÓM 3: MỤC TIÊU PHÂN KỲ & KỊCH BẢN ĐIỀU HÀNH (SCENARIOS M / N / S)
Hỗ trợ quản trị linh hoạt theo 3 kịch bản mục tiêu:

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `C_TargetValueM` / `C_TargetValueMOperator` | Double/Op | Giá trị và toán tử so sánh ($\ge, \le, =$) cho Kịch bản Cam kết (M). |
| `C_TargetValueN` / `C_TargetValueNOperator` | Double/Op | Giá trị và toán tử cho Kịch bản Phấn đấu (N). |
| `C_TargetValueS` / `C_TargetValueSOperator` | Double/Op | Giá trị và toán tử cho Kịch bản Tối thiểu chấp nhận được (S). |
| `C_TargetDateM`, `N`, `S` | Date | Thời hạn hoàn thành chỉ tiêu tương ứng theo từng kịch bản. |
| `C_TargetResultM`, `N`, `S` | Double | Kết quả thực hiện đo đạc được theo từng kịch bản M/N/S. |
| `C_DateVarianceM`, `N`, `S` | Duration | Biến động chênh lệch ngày hoàn thành so với hạn chót kịch bản. |
| `C_ValueVarianceM`, `N`, `S` | Double | Chênh lệch giá trị tuyệt đối giữa thực tế đạt được so với mục tiêu từng kịch bản. |
| `C_TargetShouldHave` / `C_AllPassedMustHave`| Boolean | Cờ kiểm tra tính hợp lệ của các chỉ tiêu bắt buộc phải vượt qua. |

---

### NHÓM 4: TIẾN ĐỘ, TRỌNG SỐ & PHÂN TÍCH KHOẢNG CÁCH (PROGRESS & GAP)

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `PercentCompleted` | Percentage | % hoàn thành tổng thể của chỉ tiêu. |
| `PercentOfPlan` | Percentage | **Tỷ lệ đạt được so với kế hoạch**: $\frac{\text{ActualValue}}{\text{TargetValue}} \times 100\%$. |
| `ActualPercent` | Percentage | % thực tế đã ghi nhận hoàn tất. |
| `PlanningGap` | Double | Khoảng cách kế hoạch: Độ dốc cần tăng tốc để đạt đích. |
| `ActualGap` | Double | **Khoảng cách thực tế còn thiếu để về đích**: $\text{TargetValue} - \text{ActualValue}$. |
| `Weight` | Double | Trọng số của chỉ tiêu trong mục tiêu tổng thể. |
| `C_AssignmentWeight` / `C_AssignmentWeightPercent`| Double | Trọng số của chỉ tiêu khi được phân bổ vào bản giao việc `Assignment`. |
| `C_AssignmentWeightResult` | Double | Điểm kết quả trọng số đóng góp vào đánh giá nhân sự. |
| `WeightedPercentOfPlan` | Percentage | % hoàn thành kế hoạch đã nhân với trọng số. |
| `WeightedActualPercent` | Percentage | % thực tế đã nhân với hệ số trọng số. |
| `C_NormalizedScore` / `C_Score` | Double | **Điểm số chuẩn hóa (KPI Normalized Score)** quy đổi về thang điểm 100. |

---

### NHÓM 5: ĐÓNG GÓP DOANH THU & TÀI CHÍNH (FINANCIAL CONTRIBUTIONS)

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `DirectPlannedContributions` | Currency | Giá trị doanh số/đóng góp tài chính kế hoạch giao trực tiếp cho chỉ tiêu này. |
| `DirectActualContributions` | Currency | Giá trị đóng góp tài chính thực tế đã mang lại. |
| `IndirectPlannedContributions` / `IndirectActualContributions` | Currency | Giá trị đóng góp gián tiếp (từ các đơn vị phối hợp hoặc hiệu ứng lan tỏa). |
| `C_PlannedRevenueMRollup` / `NRollup` | Currency | Doanh thu kế hoạch được rollup từ các dự án/task con lên chỉ tiêu. |
| `C_ContributionsCount` | Integer | Số lượng các khoản đóng góp đã ghi nhận. |

---

### NHÓM 6: THỜI GIAN, TRẠNG THÁI & PHÂN QUYỀN TRÁCH NHIỆM

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `InitialDate` / `TargetDate` | Date | Ngày bắt đầu kích hoạt chỉ tiêu và hạn chót về đích. |
| `C_ActualDate` | Date | Ngày thực tế chính thức đạt được chỉ tiêu đo lường. |
| `State` | EntityRef | Trạng thái vòng đời chỉ tiêu (`/State/Active`, `/State/Completed`). |
| `Status` | String | Đánh giá hiện trạng (Đạt, Vượt, Chưa đạt, Đang theo dõi). |
| `C_Assignee` | EntityRef | Nhân sự được giao cam kết thực hiện chỉ tiêu. |
| `C_Owner` / `EntityOwner` | EntityRef | Cán bộ sở hữu và quản lý chỉ tiêu trong tổ chức. |
| `C_Reporter` | EntityRef | Đầu mối phụ trách thu thập số liệu đo kiểm và báo cáo. |
| `C_Department` / `C_TargetDepartment` | EntityRef | Phòng ban / Đơn vị chịu trách nhiệm về chỉ tiêu này. |
| `CreatedBy` / `CreatedOn` | EntityRef/Date | Người tạo và thời điểm khởi tạo chỉ tiêu. |
| `LastUpdatedBy` / `LastUpdatedOn` | EntityRef/Date | Người cập nhật và **Watermark gia số thời gian để cào tăng dần**. |
| `LastUpdatedBySystemOn` | DateTime | Dấu thời gian hệ thống tự động tính toán lại điểm kết quả. |
| `AttachmentsCount` / `NotesCount` / `EmailsCount` | Integer | Bằng chứng, biên bản nghiệm thu, tài liệu chứng minh kết quả đạt chỉ tiêu. |

---

## 3. MỐI QUAN HỆ CỦA TARGET VỚI 4 BẢNG CÒN LẠI

Bảng Target là **Thước đo Định lượng** liên kết đa chiều trong hệ thống:
1. **Liên kết với `BSC` (N-1)**: Thông qua trường `AssociatedObjective = BSC.SYSID`. Target là công cụ cụ thể hóa mục tiêu chiến lược của BSC thành các chỉ tiêu số học đo lường được.
2. **Liên kết với `Project` (N-1)**: Thông qua trường `AssociatedItem = Project.SYSID`. Các dự án triển khai sẽ nhắm đến việc hoàn thành các Target này.
3. **Liên kết với `Assignment` (N-1)**: Thông qua trường `C_AssociatedAssignment = Assignment.SYSID`. Các Target được gán vào bản giao việc của từng cá nhân/đơn vị kèm trọng số `C_AssignmentWeight`.
4. **Liên kết với `Task`**: Các sản phẩm bàn giao (`Task.Deliverable`) và cột mốc (`Task.Milestone`) của công việc hàng ngày chính là bằng chứng xác nhận việc hoàn thành Target (`ActualValue`).
