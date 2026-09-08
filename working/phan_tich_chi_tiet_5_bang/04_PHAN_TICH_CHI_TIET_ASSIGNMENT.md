# BẢNG TỪ ĐIỂN DỮ LIỆU & PHÂN TÍCH CHI TIẾT: THỰC THỂ ASSIGNMENT
**Hệ thống Quản trị Phân công Nhiệm vụ & Đánh giá KPI Nhân sự (Planview Clarizen Lakehouse)**
**Nguồn dữ liệu gốc**: `D:\dataguystory\coding-interview-university\assignment_data_raw.txt` (40 trường)

---

## 1. TỔNG QUAN VỀ THỰC THỂ ASSIGNMENT

Trong hệ thống quản trị nhân sự và điều hành công việc, **Assignment (Phân công nhiệm vụ / Bản giao việc & Cam kết KPI)** là mắt xích gắn kết giữa **Mục tiêu tổ chức** với **Trách nhiệm cá nhân**.

Bảng Assignment đóng vai trò là "Bản khế ước giao việc" cụ thể hóa:
1. **Quan hệ Giao việc - Nhận việc (Assignor - Assignee)**: Xác định rõ Lãnh đạo giao việc (`C_Assignor`), Nhân sự/Đơn vị nhận việc (`C_Assignee`), và Đầu mối giám sát báo cáo (`C_Reporter`).
2. **Cơ cấu Trọng số Toàn diện (Multi-dimensional Weighting)**: Không chỉ chấm điểm khối lượng công việc, Assignment phân rã trọng số đánh giá thành 4 cấu phần:
   * **Nhiệm vụ cốt lõi** (`C_EssentialWeightTotalPercent`).
   * **Mức độ phối hợp liên phòng ban** (`C_WeightCollaboration`).
   * **Mức độ tuân thủ quy chế, quy trình** (`C_WeightCompliance`).
   * **Thực thi chính sách & Điểm thưởng vượt trội** (`C_WeightPolicy`, `C_WeightBonus`).
3. **Cơ chế Tổng hợp Kết quả Chỉ tiêu (Target Rollup Calculation)**: Tự động gom điểm số từ các chỉ tiêu chi tiết trong bảng `Target` (`C_SumTargetWeightPercent`, `C_SumTargetResultWeightAll`) để tính ra **Tỷ lệ hoàn thành KPI cuối cùng (`C_AchievementRate`, `C_TotalAssignmentWeightResult`)**.

---

## 2. PHÂN NHÓM VÀ GIẢI THÍCH CHI TIẾT 40 TRƯỜNG DỮ LIỆU

### NHÓM 1: ĐỊNH DANH & CẤU TRÚC PHÂN CẤP PHÂN CÔNG (HIERARCHY)

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `SYSID` | String (PK) | Mã định danh duy nhất của bản ghi giao việc (ví dụ: `/Assignment/ASN-2026-089`). |
| `Name` | String | Tên bản giao việc / Tiêu đề phiếu giao nhiệm vụ KPI. |
| `Description` / `C_UpdateDescription` | Text | Thuyết minh chi tiết nội dung giao việc và các lưu ý chỉ đạo điều hành. |
| `C_AssignmentNumber` | String | Số hiệu phiếu giao việc / Mã hợp đồng KPI nội bộ quy chuẩn. |
| `ExternalID` | String | Mã tham chiếu văn bản chỉ đạo hoặc số quyết định bổ nhiệm/giao việc. |
| `EntityType` | String | Luôn mang giá trị cố định `"Assignment"`. |
| `C_ParentAssignment` | EntityRef | **Khóa ngoại trỏ đến Phiếu giao việc cha cấp trên** (Giao từ Cấp Trưởng phòng xuống Nhân viên). |
| `C_CountofSubAssignments` | Integer | Số lượng phiếu giao việc con trực thuộc đã được phân rã tiếp. |
| `ImageUrl` | URL | Ảnh đại diện của nhân sự hoặc chứng chỉ/quyết định giao việc. |

---

### NHÓM 2: CÁC BÊN THAM GIA & PHÂN QUYỀN TRÁCH NHIỆM (PARTIES INVOLVED)

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `C_Assignor` | EntityRef | **Người giao việc / Thủ trưởng đơn vị** phê duyệt giao nhiệm vụ KPI. |
| `C_Assignee` | EntityRef | **Người nhận việc / Cán bộ nhân viên** chịu trách nhiệm thực thi. |
| `C_Reporter` | EntityRef | Cán bộ đầu mối phụ trách theo dõi, cập nhật tiến độ và lập báo cáo đánh giá. |
| `C_Department` | EntityRef | Phòng ban / Đơn vị tiếp nhận khối lượng công việc. |
| `EntityOwner` | EntityRef | Người sở hữu bản ghi trong hệ thống bảo mật phân quyền. |
| `CreatedBy` | EntityRef | Nhân sự khởi tạo phiếu giao việc trong hệ thống. |
| `LastUpdatedBy` | EntityRef | Nhân sự cập nhật kết quả đánh giá gần nhất. |

---

### NHÓM 3: CƠ CẤU TRỌNG SỐ ĐÁNH GIÁ (KPI WEIGHTING CRITERIA)
Cấu trúc chấm điểm toàn diện đảm bảo đánh giá công bằng và đa chiều:

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `C_TotalWeight` | Double | **Tổng trọng số quy chuẩn của bản giao việc** (chuẩn hóa = 100%). |
| `C_EssentialWeightTotalPercent` | Double | Tổng tỷ lệ trọng số dành cho các nhiệm vụ cốt lõi bắt buộc phải hoàn thành. |
| `C_WeightCollaboration` | Double | Trọng số đánh giá mức độ phối hợp, hỗ trợ giữa các phòng ban. |
| `C_WeightCollaborationDescription`| Text | Tiêu chuẩn và tiêu chí cụ thể để chấm điểm phối hợp. |
| `C_WeightCompliance` | Double | Trọng số đánh giá mức độ tuân thủ nội quy, thời hạn và quy trình ISO. |
| `C_WeightPolicy` | Double | Trọng số đánh giá việc thực thi đường lối, văn hóa doanh nghiệp và chỉ đạo nóng. |
| `C_WeightPolicyDescription` | Text | Nội dung thuyết minh chính sách cần tuân thủ. |
| `C_WeightBonus` | Double | Điểm/trọng số thưởng khuyến khích cho các sáng kiến, giải pháp vượt chỉ tiêu. |
| `C_WeightBonusDescription` | Text | Điều kiện để được xét điểm thưởng vượt trội. |

---

### NHÓM 4: KẾT QUẢ ĐÁNH GIÁ & TỔNG HỢP CHỈ TIÊU (ACHIEVEMENT & RESULTS)

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `C_AchievementRate` | Percentage | **Tỷ lệ Hoàn thành Nhiệm vụ (%)**: Đánh giá tổng thể mức độ hoàn thành bản giao việc. |
| `C_SumTargetWeightPercent` | Double | Tổng tỷ lệ trọng số của toàn bộ các chỉ tiêu định lượng (`Target`) gắn trong Assignment. |
| `C_SumTargetResultWeightAll` | Double | Tổng điểm kết quả thực tế đạt được từ tất cả các chỉ tiêu con. |
| `C_SumTargetWResultCompliance` | Double | Điểm kết quả đạt được đối với phần trách nhiệm tuân thủ. |
| `C_TotalAssignmentWeightResult` | Double | **Điểm Đánh giá KPI Cuối cùng**: Căn cứ chính thức để xếp loại lao động và chi trả thưởng hiệu quả (A, B, C). |

---

### NHÓM 5: THỜI GIAN, AUDIT TRAIL & TƯƠNG TÁC (TIMELINE & AUDIT)

| Tên trường | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- |
| `C_StartDate` | Date | Ngày bắt đầu có hiệu lực của bản giao nhiệm vụ. |
| `C_EndDate` | Date | Hạn cuối hoàn thành và chốt đánh giá kết quả công việc. |
| `CreatedOn` | DateTime | Thời điểm tạo bản ghi phiếu giao việc. |
| `LastUpdatedOn` | DateTime | **Watermark gia số thời gian dùng để cào dữ liệu tăng dần (Incremental)**. |
| `LastUpdatedBySystemOn` | DateTime | Dấu thời gian hệ thống tự động chạy batch tính toán lại điểm kết quả. |
| `CurrencyExchangeDate` | Date | Ngày áp dụng tỷ giá quy đổi tài chính (nếu KPI gắn với doanh số ngoại tệ). |
| `DefaultIntegrationPath` | String | Đường dẫn tích hợp với hệ thống Quản trị Nhân sự HRM (SAP/SuccessFactors/Workday). |
| `EmailsCount` | Integer | Số lượng email phản hồi/thảo luận liên quan đến phiếu đánh giá này. |

---

## 3. MỐI QUAN HỆ CỦA ASSIGNMENT VỚI 4 BẢNG CÒN LẠI

Bảng Assignment là **Cầu nối Nhân sự - Trách nhiệm** trong hệ sinh thái:
1. **Liên kết với `Target` (1-N)**: Một bản giao việc `Assignment` chứa nhiều chỉ tiêu định lượng cụ thể trong bảng `Target` thông qua khóa ngoại `Target.C_AssociatedAssignment = Assignment.SYSID`. Toàn bộ điểm số chỉ tiêu từ Target được rollup vào `C_SumTargetResultWeightAll`.
2. **Liên kết với `BSC`**: Trọng số của mục tiêu chiến lược trong BSC được phân bổ thành các phiếu giao việc cho từng Khối/Phòng qua trường `BSC.C_TotalWeightAssignments`.
3. **Liên kết với `User`**: Xác định danh tính 3 vai trò then chốt: Lãnh đạo giao việc (`C_Assignor`), Nhân sự nhận việc (`C_Assignee`), Cán bộ theo dõi (`C_Reporter`).
4. **Liên kết với `Task`**: Các Task công việc cụ thể được giao cho nhân sự (`Task.Manager`, `Task.EntityOwner`) chính là hành động hiện thực hóa các cam kết KPI trong phiếu Assignment này.
