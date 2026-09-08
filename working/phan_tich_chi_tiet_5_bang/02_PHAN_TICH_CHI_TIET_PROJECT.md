# BẢNG TỪ ĐIỂN DỮ LIỆU & PHÂN TÍCH CHI TIẾT: THỰC THỂ PROJECT
**Hệ thống Quản trị Danh mục & Dự án Doanh nghiệp (Planview Clarizen Lakehouse)**
**Nguồn dữ liệu gốc**: `D:\dataguystory\coding-interview-university\project_data_raw.txt` (368 trường)

---

## 1. TỔNG QUAN VỀ THỰC THỂ PROJECT

Trong hệ sinh thái Planview Clarizen, **Project** (Dự án) là thực thể trung tâm ở cấp độ điều hành chiến lược và quản trị danh mục (PPM - Project Portfolio Management).

Project bao hàm toàn bộ thông tin từ:
1. **Quản trị Chiến lược & Mục tiêu**: Liên kết với BSC (Balanced Scorecard), Target và Chương trình chuyển đổi (Program/Portfolio).
2. **Quản trị Cổng kiểm soát (Stage-Gate Reviews)**: Phê duyệt từng giai đoạn (Gate Phase, Next Gate) để kiểm soát rủi ro trước khi giải ngân tiếp.
3. **Cơ hội Kinh doanh & Giá trị Khách hàng**: Tích hợp CRM với dữ liệu Deal/Opportunity (Quy mô hợp đồng, xác suất thắng, khách hàng trọng điểm).
4. **Đặc thù Chuyên ngành An toàn Thông tin (Cyber Security / SOC)**: Hệ thống trường đo lường số lượng SIEM, SOAR, SOC, EDR, PenTest, Threat Hunting.
5. **Hạch toán Tài chính Toàn diện & EVM cấp Dự án**: Đầy đủ CAPEX/OPEX, Labor/Non-Labor, dòng tiền năm tài chính (FY), doanh thu, lợi nhuận, hoàn vốn đầu tư (ROI).
6. **Giám sát Sức khỏe Đa chiều**: Không chỉ có chỉ số EVM ($CPI, SPI$), mà còn tích hợp hệ thống chấm điểm rủi ro (Risk Scoring) và chỉ số phân tích cảm xúc (Sentiment Analysis).

---

## 2. PHÂN NHÓM VÀ GIẢI THÍCH CHI TIẾT 368 TRƯỜNG DỮ LIỆU

### NHÓM 1: CÁC TRƯỜNG TÙY BIẾN NGHIỆP VỤ (CUSTOM FIELDS `C_*`)
Nhóm trường phản ánh quy trình nghiệp vụ đặc thù của doanh nghiệp.

#### 1.1. Phân loại Tổ chức, Khách hàng & Điều hành
| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `C_Department` | EntityRef | Khối / Phòng ban chủ trì dự án. |
| `C_ActionDepartments` | String | Các phòng ban phối hợp thực thi. |
| `C_ActionResources` | String | Các nhân sự then chốt được điều động. |
| `C_CustomerName` / `C_MainCustomer` | String | Tên khách hàng hoặc đối tác chiến lược của dự án. |
| `C_Region` / `C_Product` | String | Vùng địa lý triển khai và dòng sản phẩm chủ lực. |
| `C_Assignee` | EntityRef | Nhân sự được giao trách nhiệm đầu mối dự án. |
| `C_ApprovalStatus` | String | Trạng thái phê duyệt chủ trương đầu tư dự án. |
| `C_Complexity` | String | Mức độ phức tạp của dự án (Thấp, Trung bình, Cao, Rất cao). |

#### 1.2. Quản trị Cổng kiểm soát (Stage-Gate Review Governance)
| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `C_CurrentPhase` | String | Giai đoạn thực thi hiện tại (Khảo sát, Phát triển, Triển khai, Nghiệm thu). |
| `C_GatePhase` | String | Cổng kiểm soát chất lượng đang đánh giá. |
| `C_NextGate` | String | Cổng kiểm soát tiếp theo cần phải vượt qua. |
| `C_OverallGateStatusAssessment` | String | Kết quả đánh giá tổng thể cổng kiểm soát (Đạt, Cần bổ sung, Không đạt). |

#### 1.3. Tích hợp Cơ hội Kinh doanh & Doanh số (CRM Integration)
| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `C_OpportunityID` | String | Mã cơ hội bán hàng liên kết từ hệ thống CRM (Salesforce/HubSpot). |
| `C_OpportunityAmount` / `C_OpportunityAmountNumeric` | Currency | Tổng giá trị hợp đồng/doanh số kỳ vọng từ cơ hội bán hàng. |
| `C_OpportunityStage` | String | Giai đoạn bán hàng (Proposal, Negotiation, Closed Won). |
| `C_OpportunityProbability` | Percentage | Xác suất chốt hợp đồng thành công ($0 \rightarrow 100\%$). |
| `C_OpportunityCloseDate` | Date | Ngày dự kiến ký kết hợp đồng thương mại. |

#### 1.4. Nhóm Chỉ số An toàn Thông tin & Vận hành Kỹ thuật (Cyber Security / SOC / EDR)
| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `C_SIEMQty` | Integer | Số lượng hệ thống giám sát an ninh SIEM triển khai trong dự án. |
| `C_SOARQty` | Integer | Số lượng kịch bản tự động hóa ứng phó sự cố SOAR. |
| `C_SOCQty` | Integer | Số lượng trung tâm điều hành an ninh mạng SOC liên quan. |
| `C_EDRQty` | Integer | Số lượng điểm cuối cài đặt phần mềm EDR (Endpoint Detection & Response). |
| `C_PenTestQty` | Integer | Số đợt kiểm thử xâm nhập bảo mật (Penetration Testing) theo cam kết. |
| `C_HuntingQty` | Integer | Số chiến dịch săn lùng mối nguy hại (Threat Hunting) được thực hiện. |
| `C_AuditQty` | Integer | Số lượng cuộc đánh giá kiểm toán an toàn thông tin định kỳ. |

#### 1.5. Mục tiêu Phân kỳ & Độ ưu tiên Chiến lược (Target Milestones)
| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `C_AssociatedObjective` | EntityRef | **Liên kết trực tiếp đến Mục tiêu BSC / Target cấp cao**. |
| `C_StrategicAlignment` | String | Mức độ phù hợp với chiến lược dài hạn của công ty. |
| `C_MustHave`, `C_ShouldHave`, `C_NiceToHave` | Text | Phân loại tính năng theo mô hình MoSCoW. |
| `C_TargetDateM`, `C_TargetDateN`, `C_TargetDateS` | Date | Các mốc thời hạn hoàn thành mục tiêu theo các kịch bản M/N/S. |
| `C_TargetValueM`, `C_TargetValueN`, `C_TargetValueS` | Double | Chỉ tiêu giá trị cần đạt tương ứng theo kịch bản. |
| `C_TotalSoftwareCost` | Currency | Tổng chi phí bản quyền phần mềm bản quyền đầu tư cho dự án. |
| `C_GanttURL`, `C_RoadmapURL`, `C_IdeaPlaceURL` | URL | Đường dẫn liên kết trực tiếp đến biểu đồ Gantt và Roadmap dự án. |

---

### NHÓM 2: ĐỊNH DANH & CẤU TRÚC PHÂN CẤP DANH MỤC (HIERARCHY & PORTFOLIO)
Quản lý cây phân cấp từ Tập đoàn $\rightarrow$ Danh mục (Portfolio) $\rightarrow$ Chương trình (Program) $\rightarrow$ Dự án (Project) $\rightarrow$ Giai đoạn (Phase).

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `SYSID` | String (PK) | Mã định danh duy nhất của dự án (ví dụ: `/Project/PRJ-2026-001`). |
| `InternalId` / `ExternalID`| Long/String | ID nội bộ và mã dự án quy chuẩn của doanh nghiệp. |
| `IsPortfolio` | Boolean | Cờ đánh dấu đây là Danh mục quản trị nhiều dự án con. |
| `Program` | EntityRef | Chương trình mục tiêu lớn mà dự án này trực thuộc. |
| `ParentProject` / `Parent`| EntityRef | Dự án cha cấp cao nhất hoặc đối tượng cha trực tiếp. |
| `Phase` | EntityRef | Giai đoạn hiện tại của dự án. |
| `ProjectType` | String | Phân loại dự án (R&D, Khách hàng, Chuyển đổi số, Hạ tầng). |
| `InvestmentType` | String | Loại hình đầu tư (Dự án chiến lược, Duy trì vận hành, Đổi mới sáng tạo). |
| `ProjectSize` | String | Quy mô dự án (S, M, L, XL căn cứ trên ngân sách và thời gian). |
| `ChildrenCount` / `ChildShortcutCount`| Integer | Số lượng phân cấp con và shortcut liên kết. |
| `PredecessorsCount` / `SuccessorsCount`| Integer | Số lượng dự án ràng buộc phụ thuộc trước/sau. |

---

### NHÓM 3: THỜI GIAN, LỊCH TRÌNH & ĐỐI CHUẨN BASELINE (SCHEDULING)

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `StartDate` / `DueDate` | DateTime | Ngày khởi động và ngày về đích theo kế hoạch hiện hành. |
| `Duration` | Duration | Tổng thời lượng kế hoạch của dự án (ngày/tháng). |
| `ActualStartDate` / `ActualEndDate` | DateTime | Ngày khởi động thực tế và ngày nghiệm thu bàn giao chính thức. |
| `ActualDuration` | Duration | Tổng thời gian thực tế đã thực hiện. |
| `BaselineStartDate` / `BaselineDueDate` | DateTime | Ngày bắt đầu và hạn chót theo **Kế hoạch cơ sở (Baseline) ban đầu**. |
| `BaselineDuration` / `BaselineWork` | Mixed | Thời lượng và khối lượng giờ công theo Baseline được phê duyệt. |
| `StartDateVariance` / `DueDateVariance` | Duration | Độ trễ ngày bắt đầu và ngày kết thúc so với kế hoạch gốc ban đầu. |
| `BaselineCostsVariance` / `BaselineRevenueVariance` | Currency | Chênh lệch chi phí và doanh thu so với ngân sách được phê duyệt tại Baseline. |
| `OnCriticalPath` | Boolean | Đánh dấu dự án có nằm trên đường găng chiến lược của cả chương trình. |

---

### NHÓM 4: KHỐI LƯỢNG CÔNG VIỆC, NHÂN LỰC & CHẤM CÔNG (RESOURCE EFFORT)

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `Work` / `BudgetedHours` | Duration | Tổng số giờ công kế hoạch và ngân sách giờ công được duyệt. |
| `ActualEffort` | Duration | **Tổng số giờ làm việc thực tế đã ghi nhận từ toàn bộ Timesheet của các Task con**. |
| `RemainingEffort` | Duration | Số giờ công ước tính còn lại để hoàn thành toàn bộ dự án. |
| `ActualBillableHours` / `ActualNonBillableHours` | Duration | Tách biệt số giờ có thể thu tiền khách hàng và giờ nội bộ. |
| `UserResourcesCount` / `AllUserResourcesCount` | Integer | Tổng số nhân sự tham gia trực tiếp và toàn bộ dự án. |
| `TotalAllocation` / `Unallocated` | Double | Tỷ lệ phân bổ nguồn lực và phần công việc chưa có người đảm nhiệm. |

---

### NHÓM 5: TÀI CHÍNH, HẠCH TOÁN CHI PHÍ & NĂM TÀI CHÍNH (CAPEX/OPEX & FY)

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `PlannedBudget` / `PlannedAmount` | Currency | **Tổng ngân sách được phê duyệt cho dự án**. |
| `BudgetCostLR` / `BudgetCostNLR` | Currency | Ngân sách nhân công (Labor Rate) và chi phí ngoài nhân công. |
| `BudgetCostCAPEX` / `CapexBudgetYTD` | Currency | Ngân sách Đầu tư tài sản (CAPEX) kế hoạch và lũy kế năm hiện tại. |
| `BudgetCostOPEX` / `OpexBudgetYTD` | Currency | Ngân sách Vận hành (OPEX) kế hoạch và lũy kế năm hiện tại. |
| `BudgetThirdPartyNLRCosts` | Currency | Ngân sách dành cho nhà thầu phụ / đối tác thuê ngoài. |
| `BudgetVariance` / `BudgetVariancePercent` | Mixed | Độ lệch ngân sách tuyệt đối và % chênh lệch so với kế hoạch. |
| `RemainingBudget` | Currency | Ngân sách còn lại khả dụng: $\text{PlannedBudget} - \text{ActualCost}$. |
| `ActualCost` | Currency | **TỔNG CHI PHÍ THỰC TẾ ĐÃ GIẢI NGÂN (AC - Actual Cost)**. |
| `ActualCostLR` / `ActualCostNLR` | Currency | Chi phí nhân công thực tế và chi phí mua sắm vật tư thực tế. |
| `ActualCostCAPEX` / `ActualCostOPEX` | Currency | Hạch toán thực tế chi phí CAPEX và OPEX. |
| `PlannedRevenue` / `ActualRevenue` | Currency | Doanh thu kế hoạch và doanh thu thực tế đã ghi nhận. |
| `PlannedNetProfit` / `ActualNetProfit` | Currency | Lợi nhuận ròng dự kiến và lợi nhuận ròng thực tế. |
| `Profitability` / `TargetMargin` | Percentage | Tỷ suất lợi nhuận và biên lợi nhuận mục tiêu của dự án. |
| `ExpectedROI` | Percentage | Tỷ suất hoàn vốn đầu tư kỳ vọng (Return On Investment). |
| `FYBudgetedCost...` / `FYForecastCost...` (10 trường) | Currency | Toàn bộ dòng tiền ngân sách và dự báo chi phí theo từng Năm tài chính (Fiscal Year). |
| `Funding`, `FundingAvailable`, `FundingGap` | Currency | Nguồn vốn cấp, vốn thực có và thâm hụt nguồn vốn tài trợ. |

---

### NHÓM 6: QUẢN LÝ GIÁ TRỊ THU ĐƯỢC CẤP DỰ ÁN (PROJECT-LEVEL EVM)

Toàn bộ các chỉ số EVM của Project được tổng hợp (rollup) từ tất cả các Task con:
* **`EarnedValue` (EV)**: Tổng giá trị công việc thực tế đã hoàn thành của toàn bộ dự án.
* **`CPI` (Cost Performance Index)**: $\frac{\sum EV}{\sum AC}$ $\rightarrow$ Đánh giá dự án đang tiết kiệm hay bội chi.
* **`SPI` (Schedule Performance Index)**: $\frac{\sum EV}{\sum PV}$ $\rightarrow$ Đánh giá dự án đang nhanh hay chậm tiến độ.
* **`CostVariance` (CV)**: $EV - AC$.
* **`CurrencyEAC` (Estimate At Completion)**: Dự toán tổng mức đầu tư khi dự án về đích.
* **`CurrencyETC` (Estimate To Complete)**: Dự toán số tiền cần rót thêm để hoàn tất dự án.
* **`TCPI` (To-Complete Performance Index)**: Hiệu quả chi phí bắt buộc phải đạt trong giai đoạn còn lại.
* **`RevenueEarnedValue`, `CurrencyREAC`, `RPI`**: Bộ chỉ số EVM đo lường tiến độ thu hồi dòng tiền doanh thu.

---

### NHÓM 7: QUẢN TRỊ RỦI RO & CHỈ SỐ CẢM XÚC (RISK & SENTIMENT)

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `TrackStatus` | String | Đánh giá tổng quan: `"On Track"`, `"At Risk"`, `"Off Track"`. |
| `PercentCompleted` / `ExpectedProgress`| Percentage| % hoàn thành thực tế và % kỳ vọng tại ngày báo cáo. |
| `Risks` | EntityRef | Danh mục các rủi ro đã nhận diện của dự án. |
| `RisksImpact` / `RisksRate` | Integer | Điểm số tác động rủi ro và xác suất xảy ra rủi ro. |
| `RisksTotalScore` | Double | **Điểm rủi ro tổng hợp**: $\text{Tác động} \times \text{Xác suất}$. |
| `Mitigation` | Text | Phương án giảm thiểu rủi ro đã được phê duyệt. |
| `IssuesCount` | Integer | Số sự cố nghiêm trọng đang mở cần Ban giám đốc chỉ đạo giải quyết. |
| `SentimentScore` | Double | Điểm cảm xúc tự động tính toán từ các ghi chú, trao đổi của dự án (-1.0 đến +1.0). |
| `SentimentStatus` | String | Trạng thái cảm xúc đội ngũ (Tích cực, Trung lập, Tiêu cực/Căng thẳng). |

---

### NHÓM 8: PHÂN QUYỀN, QUẢN TRỊ & AUDIT TRAIL

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `ProjectManager` | EntityRef | **Giám đốc Dự án (PM)** chịu trách nhiệm toàn diện. |
| `ProjectSponsor` | EntityRef | **Nhà tài trợ Dự án (Sponsor)** — Lãnh đạo cấp cao bảo trợ nguồn vốn. |
| `EntityOwner` / `Manager` | EntityRef | Người sở hữu bản ghi và người kiểm duyệt. |
| `CreatedBy` / `CreatedOn` | EntityRef/Date| Người tạo và ngày khởi tạo dự án. |
| `LastUpdatedBy` / `LastUpdatedOn` | EntityRef/Date| Người cập nhật và **Watermark thời gian dùng để cào dữ liệu tăng dần**. |
| `Overview`, `Overview1..5` | Text | Nội dung tóm tắt điều hành dành cho Ban Lãnh đạo. |

---

## 3. MỐI QUAN HỆ CỦA PROJECT VỚI 4 BẢNG CÒN LẠI

1. **Quan hệ với `Task` (1-N)**: Project là thực thể cha trực tiếp chứa các Task con thông qua khóa ngoại `Task.Project = Project.SYSID`. Mọi số liệu EVM, chi phí $AC$, giờ công của Project đều được rollup từ Task con lên.
2. **Quan hệ với `BSC` (Mục tiêu chiến lược)**: Project liên kết tới BSC thông qua trường `C_AssociatedObjective`. Dự án sinh ra là để hiện thực hóa mục tiêu trong Balanced Scorecard.
3. **Quan hệ với `Target` (Chỉ tiêu KPI)**: Các trường `C_TargetDateM/N/S` và `C_TargetValueM/N/S` liên kết với bảng Target để đo lường các mốc định lượng của dự án.
4. **Quan hệ với `Assignment` (Phân công nhân sự)**: Giao trách nhiệm dự án thông qua `ProjectManager`, `ProjectSponsor`, `C_Assignee`.
