# Data Type Pruned — Schema Hợp Đồng Tinh Gọn Cho 5 Bảng Clarizen

Thư mục này chứa các định nghĩa Schema Contract tinh gọn theo phạm vi yêu cầu của Data Analyst (DA Scope) kết hợp với các trường khóa kỹ thuật cần thiết cho tầng Data Warehouse (Kimball Star Schema).

## So sánh số lượng cột: Full Contract vs Pruned Contract

| Thực thể | Endpoint | File Full (`data_type/`) | File Pruned (`data_type_pruned/`) | Tỷ lệ tinh giản |
|:---|:---|:---:|:---:|:---:|
| **Project** | `projects` | 364 cột | **21 cột** | Giảm 94.2% |
| **Task** | `tasks` | 256 cột | **25 cột** | Giảm 90.2% |
| **Objective / BSC** | `bsc` | 51 cột | **22 cột** | Giảm 56.9% |
| **Assignment** | `c_assignments` | 39 cột | **19 cột** | Giảm 51.3% |
| **Target** | `targets` | 116 cột | **33 cột** | Giảm 71.6% |
| **TỔNG** | | **826 cột** | **120 cột** | **Giảm 85.5%** |

---

## Cơ chế chuyển đổi (Contract Switching)

Lớp `SchemaContract` tại `transform/schema_contract.py` tự động ưu tiên nạp từ thư mục `data_type_pruned/` nếu tồn tại:

1. **Mặc định:** Sử dụng `./data_type_pruned` (tinh gọn, tối ưu tốc độ và dung lượng).
2. **Khi cần tham chiếu toàn bộ schema gốc:** Đặt biến môi trường `CONTRACT_DIR=./data_type` hoặc truyền tham số `contract_dir="./data_type"` vào `SchemaContract.load()`.
3. **File gốc:** Toàn bộ các file trong thư mục `data_type/` được bảo toàn 100% làm nguồn sự thật đối chuẩn với API của Planview Clarizen.

---

## Danh sách trường chi tiết từng bảng

### 1. `project_dataType.sql` (21 cột)
- **DA Scope (9):** `Name`, `ProjectType`, `TrackStatus`, `PercentCompleted`, `C_Department`, `C_Assignee`, `CreatedBy`, `ProjectManager`, `C_ActionResources`
- **Kỹ thuật & Khóa (12):** `SYSID` (PK), `State`, `Parent`, `ParentProject`, `Manager`, `C_AssociatedObjective` (FK → BSC), `LastUpdatedOn` (Watermark), `LastUpdatedBySystemOn`, `CreatedOn`, `EntityOwner`, `ExternalID`, `EntityType`

### 2. `task_dataType.sql` (25 cột)
- **DA Scope (12):** `TaskType`, `EntityOwner`, `ParentProject`, `Parent`, `ExternalID`, `Name`, `Description`, `Work`, `Duration`, `Priority`, `StartDate`, `DueDate`
- **Kỹ thuật & Đo lường (13):** `SYSID` (PK), `State`, `Project`, `PercentCompleted`, `ActualDuration`, `ActualEffort`, `ActualStartDate`, `ActualEndDate`, `TrackStatus`, `LastUpdatedOn`, `CreatedBy`, `CreatedOn`, `LastUpdatedBySystemOn`

### 3. `objective_dataType.sql` (22 cột)
- **DA Scope (7):** `Name`, `Description`, `CreatedBy`, `C_Assignee`, `StartDate`, `EndDate`, `ParentObjective`
- **Kỹ thuật & Trọng số (15):** `SYSID` (PK), `State`, `Status`, `C_Department`, `C_ObjectiveType`, `EntityOwner`, `LastUpdatedOn`, `LastUpdatedBySystemOn`, `CreatedOn`, `Weight`, `PercentOfPlan`, `ActualPercent`, `PlannedPercent`, `ExternalID`, `EntityType`

### 4. `c_assignment_dataType.sql` (19 cột)
- **DA Scope (7):** `Name`, `Description`, `CreatedBy`, `C_Assignor`, `C_Assignee`, `C_StartDate`, `C_EndDate`, `C_ParentAssignment`
- **Kỹ thuật & Trọng số (12):** `SYSID` (PK), `C_Department`, `EntityOwner`, `C_AchievementRate`, `C_TotalWeight`, `C_SumTargetWeightPercent`, `LastUpdatedOn`, `LastUpdatedBySystemOn`, `CreatedOn`, `ExternalID`, `EntityType`

### 5. `target_dataType.sql` (33 cột)
- **DA Scope (21):** `Name`, `Description`, `C_UpdateDescription`, `Unit`, `AssociatedObjective`, `C_AssociatedAssignment`, `ParentTarget`, `TargetType`, `C_Assignee`, `CreatedBy`, `C_Department`, `C_Teams`, `C_TargetValueM`, `C_TargetDateM`, `C_TargetResultValueM`, `C_TargetValueN`, `C_TargetDateN`, `C_TargetResultValueN`, `C_TargetValueS`, `C_TargetDateS`, `C_TargetResultValueS`
- **Kỹ thuật & Tiến độ (12):** `SYSID` (PK), `State`, `Status`, `AssociatedItem` (FK → Project), `PercentCompleted`, `Weight`, `LastUpdatedOn`, `LastUpdatedBySystemOn`, `CreatedOn`, `EntityOwner`, `ExternalID`, `EntityType`
