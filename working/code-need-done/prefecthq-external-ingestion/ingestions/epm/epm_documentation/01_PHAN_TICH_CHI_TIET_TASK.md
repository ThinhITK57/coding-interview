# BẢNG TỪ ĐIỂN DỮ LIỆU & PHÂN TÍCH CHI TIẾT: THỰC THỂ TASK
**Hệ thống Quản trị Danh mục & Dự án Doanh nghiệp (Planview Clarizen Lakehouse)**
**Nguồn dữ liệu gốc**: `D:\dataguystory\coding-interview-university\task_data_raw.txt` (186 trường)

---

## 1. TỔNG QUAN VỀ THỰC THỂ TASK

Trong hệ thống điều hành doanh nghiệp, **Task** (Công việc / Gói công việc) là đơn vị thực thi cấp cơ sở nhất, phản ánh toàn bộ nỗ lực lao động, thời gian, chi phí và kết quả đầu ra để hoàn thành các mục tiêu dự án.

Bảng Task của Planview Clarizen không chỉ lưu trữ ngày bắt đầu hay ngày kết thúc, mà tích hợp 4 hệ thống nghiệp vụ tiêu chuẩn quốc tế:
1. **EVM (Earned Value Management)**: Quản lý giá trị thu được theo chuẩn PMI (Project Management Institute).
2. **Hạch toán Chi phí Dự án (Cost Accounting)**: Tách biệt chi phí nhân công (Labor Rate - LR) và ngoài nhân công (Non-Labor Rate - NLR), chi phí đầu tư tài sản (CAPEX) và chi phí vận hành (OPEX).
3. **Cấu trúc Cây công việc (WBS - Work Breakdown Structure)**: Phân cấp cha - con, đường găng (Critical Path), và ràng buộc trước sau (Predecessors/Successors).
4. **Phân bổ Nguồn lực & Chấm công (Resource Allocation & Timesheet)**: Theo dõi giờ công thực tế, giờ có thể tính phí (Billable) và đồng hồ bấm giờ (Stopwatch).

---

## 2. PHÂN NHÓM VÀ GIẢI THÍCH CHI TIẾT 186 TRƯỜNG DỮ LIỆU

### NHÓM 1: ĐỊNH DANH, CẤU TRÚC PHÂN CẤP & QUAN HỆ (HIERARCHY & WBS)
Quản lý cây công việc phân cấp và liên kết của Task với Dự án và các Task khác.

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `SYSID` | String (PK) | Khóa chính duy nhất do hệ thống sinh ra (ví dụ: `/Task/009decb8a7834c`). |
| `InternalId` | Long | ID số nội bộ tối ưu cho chỉ mục (indexing). |
| `ExternalID` | String | Mã nghiệp vụ từ hệ thống ngoài (mã Jira, số công văn, mã kết luận giao ban). |
| `ObjectAlias` | String | Bí danh định danh ngắn phục vụ URL và API query. |
| `InstanceNumber` | Integer | Số thứ tự phiên bản / lần khởi tạo của công việc. |
| `OrderID` | Double | Thứ tự hiển thị sắp xếp trên biểu đồ Gantt WBS. |
| `Parent` | EntityRef | Khóa ngoại trỏ đến đối tượng cha trực tiếp (Task cha hoặc Milestone cha). |
| `ParentProject` | EntityRef | Khóa ngoại trỏ đến Dự án cấp cao nhất (Root Project). |
| `Project` | EntityRef | Khóa ngoại trỏ trực tiếp đến Dự án trực thuộc chứa Task này. |
| `Phase` | EntityRef | Giai đoạn dự án mà Task này trực thuộc (ví dụ: Khởi tạo, Thiết kế, Triển khai). |
| `ChildrenCount` | Integer | Số lượng task con trực thuộc. |
| `ChildShortcutCount` | Integer | Số lượng đường dẫn tắt (shortcut links) trỏ đến task con. |
| `Milestone` | Boolean | Cờ đánh dấu Task có phải là Cột mốc quan trọng (thời lượng = 0) hay không. |
| `Deliverable` | EntityRef/Bool| Sản phẩm bàn giao cụ thể gắn liền với kết quả công việc. |
| `DeliverableType` | String | Phân loại sản phẩm bàn giao (Báo cáo, Phần mềm, Hồ sơ nghiệm thu). |
| `TaskType` | String | Phân loại hình thức công việc (Chuẩn, Tóm tắt, Họp định kỳ). |
| `EntityType` | String | Luôn mang giá trị cố định `"Task"`. |
| `Template` | Boolean | Đánh dấu task mẫu dùng để nhân bản quy trình. |
| `FloatingTask` | Boolean | Task trôi nổi không bị ràng buộc cứng vào lịch trình tổng thể. |
| `PredecessorsCount`| Integer | Số lượng task tiền nhiệm (phải hoàn thành trước khi task này bắt đầu). |
| `SuccessorsCount` | Integer | Số lượng task kế nhiệm (bị phụ thuộc vào tiến độ của task này). |
| `Conflicts` | Boolean | Cờ cảnh báo có xung đột lịch trình giữa các task phụ thuộc. |
| `ConflictType` | String | Loại xung đột (Lệch ngày, Tài nguyên bị phân bổ vượt quá 100%). |

---

### NHÓM 2: QUẢN LÝ THỜI GIAN, LỊCH TRÌNH & ĐƯỜNG GĂNG (SCHEDULING & VARIANCE)
Theo dõi kế hoạch cơ sở (Baseline), kế hoạch hiện tại (Planned) và thực tế (Actual).

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `StartDate` | DateTime | Ngày bắt đầu dự kiến theo kế hoạch hiện tại. |
| `DueDate` | DateTime | Hạn hoàn thành dự kiến theo kế hoạch hiện tại. |
| `Duration` | Duration | Tổng thời lượng làm việc dự kiến (ví dụ: 10d, 80h). |
| `WorkdaysOnly` | Boolean | Chỉ tính các ngày làm việc trong tuần (bỏ qua Thứ 7, Chủ Nhật). |
| `WeekDays` | String | Danh sách các ngày làm việc trong tuần được áp dụng. |
| `EarliestStartDate`| DateTime | Ngày bắt đầu sớm nhất có thể theo thuật toán Critical Path Method (CPM). |
| `EarliestEndDate` | DateTime | Ngày kết thúc sớm nhất có thể theo thuật toán CPM. |
| `LatestStartDate` | DateTime | Ngày bắt đầu muộn nhất mà không làm chậm toàn bộ dự án. |
| `LatestEndDate` | DateTime | Ngày kết thúc muộn nhất cho phép. |
| `OnCriticalPath` | Boolean | **Cờ Đường Găng**: Nếu task này chậm trễ, toàn bộ dự án sẽ bị chậm theo! |
| `EstimatedStartDate`| DateTime| Ngày bắt đầu ước tính dựa trên tốc độ làm việc thực tế hiện tại. |
| `EstimatedEndDate` | DateTime| Ngày kết thúc ước tính theo dự báo mới nhất. |
| `EstimatedDuration`| Duration | Thời lượng ước tính thực tế cần để hoàn thành. |
| `BaselineStartDate`| DateTime | Ngày bắt đầu theo Kế hoạch Cơ sở (Baseline) được phê duyệt ban đầu. |
| `BaselineDueDate` | DateTime | Hạn chót theo Baseline gốc (mốc đối chuẩn đánh giá biến động). |
| `BaselineDuration` | Duration | Thời lượng theo Baseline gốc. |
| `BaselineWork` | Duration | Tổng giờ công cam kết ban đầu theo Baseline. |
| `BaselineCreationDate`| DateTime| Ngày lập và đóng băng bản ghi Baseline. |
| `StartDateVariance`| Duration | Độ lệch ngày bắt đầu: $\text{StartDate} - \text{BaselineStartDate}$. |
| `DueDateVariance` | Duration | Độ lệch ngày hoàn thành: $\text{DueDate} - \text{BaselineDueDate}$. |
| `DurationVariance` | Duration | Biến động thời lượng: $\text{Duration} - \text{BaselineDuration}$. |
| `WorkVariance` | Duration | Biến động khối lượng giờ công: $\text{Work} - \text{BaselineWork}$. |
| `ActualStartDate` | DateTime | Thời điểm thực tế bấm nút bắt đầu công việc. |
| `ActualEndDate` | DateTime | Thời điểm thực tế công việc được xác nhận hoàn thành (100%). |
| `ActualDuration` | Duration | Tổng thời lượng thực tế từ lúc bắt đầu đến khi kết thúc. |
| `Recurrence...` (9 trường)| Mixed | Nhóm trường cấu hình công việc lặp lại định kỳ (tuần, tháng, năm). |

---

### NHÓM 3: KHỐI LƯỢNG CÔNG VIỆC, NHÂN SỰ & CHẤM CÔNG (EFFORT & ALLOCATION)
Theo dõi số giờ công (man-hours), tỷ lệ phân bổ nhân lực và nhật ký thời gian.

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `Work` | Duration | Tổng số giờ công kế hoạch cần để hoàn thành task. |
| `BudgetedHours` | Duration | Tổng số giờ công được ngân sách phê duyệt. |
| `ActualEffort` | Duration | Tổng số giờ công thực tế nhân sự đã bỏ ra (ghi nhận qua Timesheet). |
| `ActualEffortUpdatedFromTimesheets` | Boolean | Cờ xác nhận số giờ thực tế đã được đồng bộ tự động từ Timesheet. |
| `RemainingEffort` | Duration | Số giờ công ước tính còn lại cần bỏ ra để hoàn tất công việc. |
| `ActualBillableHours`| Duration | Số giờ thực tế có thể xuất hóa đơn tính tiền cho khách hàng/đối tác. |
| `ActualNonBillableHours`| Duration| Số giờ nội bộ không thể tính tiền cho khách hàng. |
| `Allocation` | Double | Tỷ lệ % phân bổ thời gian của nhân sự vào task này (ví dụ: 50%). |
| `SubAllocation` | Double | Tỷ lệ phân bổ chi tiết cho task con. |
| `TotalAllocation` | Double | Tổng tỷ lệ phân bổ trên toàn bộ nhân sự tham gia. |
| `Unallocated` | Double | Phần khối lượng công việc chưa có người nhận trách nhiệm. |
| `UserResourcesCount`| Integer | Số lượng nhân sự thật (Named Users) được gán vào task. |
| `ResourcesAndPlaceholdersCount`| Integer| Tổng số nhân sự thật + vai trò giữ chỗ (Placeholder, ví dụ: Senior Dev). |
| `AllUserResourcesCount`| Integer | Tổng nhân sự tính cả phân cấp task con. |
| `ActiveStopwatch` | Boolean | Cờ có đồng hồ bấm giờ (Stopwatch) đang chạy trực tiếp hay không. |
| `StopwatchesCount` | Integer | Số lượng đồng hồ bấm giờ đang gắn vào task. |
| `PendingTimeTrackingEffort`| Duration| Số giờ chấm công đang chờ cấp quản lý phê duyệt. |
| `TimeTrackingCost` | Currency | Tổng chi phí phát sinh từ số giờ chấm công thực tế. |

---

### NHÓM 4: TÀI CHÍNH, CHI PHÍ, DOANH THU & HẠCH TOÁN (FINANCIALS)
Hạch toán tài chính dự án, phân bổ ngân sách theo chuẩn kế toán doanh nghiệp.

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `PlannedBudget` | Currency | Tổng ngân sách kế hoạch được giao cho task. |
| `PlannedAmount` | Currency | Số tiền kế hoạch dự kiến chi tiêu. |
| `BudgetCostLR` | Currency | Ngân sách dành cho Chi phí Nhân công (**LR - Labor Rate**). |
| `BudgetCostNLR` | Currency | Ngân sách dành cho Chi phí Ngoài nhân công (**NLR - Non-Labor Rate**). |
| `BudgetCostCAPEX` | Currency | Ngân sách Đầu tư tài sản cố định (**CAPEX**). |
| `BudgetCostOPEX` | Currency | Ngân sách Chi phí vận hành thường xuyên (**OPEX**). |
| `BudgetThirdPartyNLRCosts`| Currency| Ngân sách thuê ngoài / nhà thầu phụ (Third-party contractors). |
| `ActualCost` | Currency | **Tổng chi phí thực tế đã giải ngân (AC - Actual Cost)**. |
| `ActualCostLR` | Currency | Chi phí nhân công thực tế: $\sum (\text{Giờ làm} \times \text{Đơn giá nhân sự})$. |
| `ActualCostNLR` | Currency | Chi phí vật tư, bản quyền, thiết bị thực tế phát sinh. |
| `ActualCostCAPEX` | Currency | Chi phí CAPEX thực tế đã hạch toán. |
| `ActualCostOPEX` | Currency | Chi phí OPEX thực tế đã giải ngân. |
| `CapexActualYTD` | Currency | Chi phí CAPEX lũy kế từ đầu năm tài chính đến hiện tại (Year-To-Date). |
| `OpexActualYTD` | Currency | Chi phí OPEX lũy kế từ đầu năm tài chính đến hiện tại. |
| `PlannedRevenue` | Currency | Doanh thu kế hoạch mang lại từ task này. |
| `ActualRevenue` | Currency | Doanh thu thực tế đã ghi nhận. |
| `ActualNetProfit` | Currency | Lợi nhuận ròng thực tế: $\text{ActualRevenue} - \text{ActualCost}$. |
| `ActualNetProfitability`| Percentage| Tỷ suất lợi nhuận thực tế: $\frac{\text{ActualNetProfit}}{\text{ActualRevenue}} \times 100\%$. |
| `Funding...` (5 trường) | Currency | Theo dõi nguồn vốn cấp, vốn khả dụng và thâm hụt tài trợ (Funding Gap). |
| `FixedCost` / `FixedPrice`| Currency | Đơn giá khoán cố định theo hợp đồng (nếu có). |

---

### NHÓM 5: QUẢN LÝ GIÁ TRỊ THU ĐƯỢC (EVM - EARNED VALUE MANAGEMENT)
Bộ chỉ số chuẩn quốc tế của PMI để đo lường sức khỏe dự án:

| Tên trường | Ký hiệu | Công thức toán học & Ý nghĩa nghiệp vụ |
| :--- | :---: | :--- |
| `EarnedValue` | **EV** | Giá trị công việc thực tế đã hoàn thành: $\text{PlannedBudget} \times \frac{\text{PercentCompleted}}{100}$. |
| `PlannedBudget` | **PV** | Giá trị công việc dự kiến phải hoàn thành tại thời điểm xét (Planned Value). |
| `ActualCost` | **AC** | Toàn bộ chi phí thực tế đã chi ra để đạt được giá trị công việc hiện tại. |
| `CPI` | **CPI** | **Chỉ số Hiệu quả Chi phí**: $\frac{EV}{AC}$. <br>• $\mathbf{CPI > 1.0}$: Tiết kiệm chi phí.<br>• $\mathbf{CPI < 1.0}$: Bội chi / Vượt ngân sách! |
| `SPI` | **SPI** | **Chỉ số Hiệu quả Lịch trình**: $\frac{EV}{PV}$. <br>• $\mathbf{SPI > 1.0}$: Vượt tiến độ.<br>• $\mathbf{SPI < 1.0}$: Chậm tiến độ so với kế hoạch! |
| `CurrencyEAC` | **EAC** | **Dự toán Chi phí khi Hoàn thành (Estimate At Completion)**: $\frac{\text{Budget}}{CPI}$. |
| `CurrencyETC` | **ETC** | **Dự toán Chi phí Còn lại cần bỏ ra (Estimate To Complete)**: $EAC - AC$. |
| `TCPI` | **TCPI** | **Hiệu quả Chi phí Cần đạt trong phần còn lại**: $\frac{\text{Budget} - EV}{\text{Budget} - AC}$. |
| `RevenueEarnedValue` | **REV** | Giá trị doanh thu thu được theo tiến độ hoàn thành. |

---

### NHÓM 6: TRẠNG THÁI, TIẾN ĐỘ & SỨC KHỎE CÔNG VIỆC (STATUS & HEALTH)
Đánh giá mức độ rủi ro và giám sát tiến độ hoàn thành.

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `PercentCompleted` | Double | % hoàn thành tổng thể của công việc ($0.0 \rightarrow 100.0\%$). |
| `ExpectedProgress` | Double | % tiến độ lẽ ra phải đạt được tính đến ngày hôm nay. |
| `CalculateCompletenessBasedOnEfforts`| Boolean| Tính % hoàn thành tự động theo tỷ lệ giờ công: $\frac{\text{ActualEffort}}{\text{Work}}$. |
| `TrackStatus` | String | **Trạng thái sức khỏe nhanh**: `"On Track"` (Bình thường), `"At Risk"` (Có rủi ro), `"Off Track"` (Mất kiểm soát). |
| `State` | EntityRef | Trạng thái vòng đời (`/State/Draft`, `/State/Active`, `/State/Completed`, `/State/Cancelled`). |
| `IssuesCount` | Integer | Số lượng sự cố/vấn đề đang mở cần giải quyết gắn với task này. |
| `CommitLevel` | String | Mức độ cam kết thực hiện (Thấp, Trung bình, Cao). |
| `CommittedDate` | DateTime | Ngày cam kết bàn giao kết quả cuối cùng. |

---

### NHÓM 7: CỘNG TÁC, SIÊU DỮ LIỆU & AUDIT TRAIL (COLLABORATION & AUDIT)
Truy vết người thao tác, thời gian cập nhật và nội dung ghi chú chỉ đạo.

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `Manager` | EntityRef | Người quản lý trực tiếp chịu trách nhiệm nghiệm thu task. |
| `EntityOwner` | EntityRef | Người sở hữu bản ghi trong hệ thống bảo mật. |
| `CreatedBy` | EntityRef | Nhân sự tạo mới bản ghi task. |
| `LastUpdatedBy` | EntityRef | Nhân sự cập nhật bản ghi gần nhất. |
| `CreatedOn` | DateTime | Thời điểm tạo bản ghi trong cơ sở dữ liệu. |
| `LastUpdatedOn` | DateTime | **Watermark thời gian**: Căn cứ cốt lõi để cào dữ liệu tăng dần (Incremental). |
| `Name` | String | Tên mô tả công việc. |
| `Description` | Text | Nội dung thuyết minh chi tiết yêu cầu công việc. |
| `Overview`, `Overview1..5`| Text | Các trường văn bản mở rộng lưu chỉ đạo của Lãnh đạo hoặc ghi chú chuyên môn. |
| `NotesCount` / `EmailsCount`| Integer | Số lượng ghi chú trao đổi và email trao đổi gắn với task. |

---

## 3. MỐI QUAN HỆ CỦA TASK VỚI 4 BẢNG CÒN LẠI

Bảng Task là **Trung tâm Thực thi** liên kết chặt chẽ với 4 thực thể trong hệ thống:
1. **Liên kết với `Project`**: Thông qua các trường `Project`, `ParentProject`, `Phase`. Mỗi Task bắt buộc phải thuộc về 1 Project.
2. **Liên kết với `User` / `Assignment`**: Thông qua `Manager`, `EntityOwner`, `CreatedBy`, `UserResourcesCount`. Mỗi Task được gán cho nhân sự cụ thể thực thi.
3. **Liên kết với `Target`**: Thông qua `Milestone`, `Deliverable`, `DeliverableType`, `ExpectedProgress`. Task là hành động cụ thể để tạo ra kết quả đạt Target.
4. **Liên kết với `BSC` (Giao ban / Chỉ đạo)**: Thông qua `ExternalID`, `Overview1..5`, `IssuesCount`, `Conflicts`. Các kết luận giao ban của lãnh đạo được phân rã thành các chỉ đạo và gỡ xung đột trực tiếp trên Task.
