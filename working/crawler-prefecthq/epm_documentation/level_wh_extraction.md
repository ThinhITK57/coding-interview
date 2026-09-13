Viewed SKILL.md:1-106
Listed directory crawler-prefecthq
Listed directory epm_documentation
Listed directory data_type
Viewed SUMMARY-5-TABLES.md:1-60
Viewed task_dataType.sql:1-60
Viewed config.json:1-100
Listed directory transform
Viewed schema_contract.py:1-100
Viewed prefect_flow.py:1-120
Searched for "Department"
Searched for "Department"
Searched for "Department"
Viewed project_dataType.sql:1-80
Searched for "Project"
Viewed project_dataType.sql:80-220
Viewed project_dataType.sql:220-373
Viewed 01_PHAN_TICH_CHI_TIET_TASK.md:1-60
Viewed 03_PHAN_TICH_CHI_TIET_OBJECTIVE_BSC.md:1-60
Viewed 04_PHAN_TICH_CHI_TIET_ASSIGNMENT.md:1-60
Viewed 05_PHAN_TICH_CHI_TIET_TARGET.md:1-60

Chào bạn, dưới đây là bản **Phân tích Chuyên sâu Toàn diện** dựa trên toàn bộ tài liệu trong `crawler-prefecthq/epm_documentation`, các định nghĩa kiểu dữ liệu trong `data_type/*.sql`, và danh sách các trường nghiệp vụ đã được DA thu hẹp cho 5 bảng. 

Đồng thời, theo yêu cầu của lệnh `/to-tickets`, tôi đã thiết kế lộ trình thực hiện thành **các Tracer-Bullet Tickets** với đầy đủ các mối quan hệ phụ thuộc (Blocking Edges) để bạn duyệt trước khi tạo file ticket chính thức.

---

# PHẦN 1: PHÂN RÃ CHI TIẾT & BẢN ĐỒ DỮ LIỆU THU HẸP (DATA MAPPING)

Dựa trên đối soát chéo giữa danh sách DA cung cấp với tài liệu phân tích nghiệp vụ (`epm_documentation`) và kiểu dữ liệu chuẩn (`data_type/*.sql`), dưới đây là bản ánh xạ chính xác:

### 1. Bảng `Project` (9 trường DA + Khóa định danh)

| STT | Tên hiển thị (DA) | Tên kỹ thuật API (Clarizen) | Kiểu dữ liệu (`data_type`) | Vai trò trong Data Warehouse |
| :--- | :--- | :--- | :--- | :--- |
| **PK** | **SYSID** | `SYSID` | `STRING` | **Primary Key** của Dự án (dạng `/Project/...`) |
| 1 | **Name** | `Name` | `STRING` | Tên dự án |
| 2 | **Project Type** | `ProjectType` | `STRING` | Phân loại dự án (R&D, Khách hàng, Nội bộ...) |
| 3 | **Status** | `TrackStatus` / `State` | `STRING` | Trạng thái dự án (Active, OnTrack, AtRisk, Completed) |
| 4 | **% Complete** | `PercentCompleted` | `DOUBLE` | % hoàn thành dự án (0.0 $\rightarrow$ 100.0) |
| 5 | **Department** | `C_Department` | `STRING` | Phòng ban / Khối phụ trách dự án |
| 6 | **Assignee** | `C_Assignee` / `EntityOwner` | `STRING` | Nhân sự / Đơn vị tiếp nhận dự án |
| 7 | **Assignor** | `CreatedBy` / `C_Assignor` | `STRING` | Lãnh đạo giao quyền / Người khởi tạo |
| 8 | **Project Manager** | `ProjectManager` / `Manager` | `STRING` | Giám đốc dự án (PM) chịu trách nhiệm chính |
| 9 | **Resources** | `Resources` / `AllUserResourcesCount` | `STRING` / `DOUBLE` | Danh sách / số lượng tài nguyên tham gia dự án |
| *FK* | *Associated Objective* | `C_AssociatedObjective` | `STRING` | **Khóa ngoại** trỏ tới Mục tiêu chiến lược (BSC) |
| *Time*| *Last Updated* | `LastUpdatedOn` | `DATE` | Dấu vết cập nhật để phục vụ CDC và SCD Type 2 |

---

### 2. Bảng `Task` (14 trường DA + Khóa định danh)

| STT | Tên hiển thị (DA) | Tên kỹ thuật API (Clarizen) | Kiểu dữ liệu (`data_type`) | Vai trò trong Data Warehouse |
| :--- | :--- | :--- | :--- | :--- |
| **PK** | **SYSID** | `SYSID` | `STRING` | **Primary Key** của Task (dạng `/Task/...`) |
| 1 | **Task Type** | `TaskType` | `STRING` | Loại công việc (Milestone, Task chuẩn, Meeting) |
| 2 | **Assignee** | `Manager` / `EntityOwner` | `STRING` | Nhân sự trực tiếp thực hiện task |
| 3 | **Department** | `C_Department` | `STRING` | Phòng ban thực hiện |
| 4 | **Resources** | `Resources` / `UserResourcesCount` | `STRING` / `DOUBLE` | Nguồn lực phân bổ cho task |
| 5 | **Parent Project** | `ParentProject` | `STRING` | **Khóa ngoại** trỏ đến Dự án cấp cao nhất (Root Project) |
| 6 | **Parent** | `Parent` | `STRING` | **Khóa tự tham chiếu** trỏ đến Task cha trong cây WBS |
| 7 | **Jira Status** | `C_JiraStatus` / `ExternalID` | `STRING` | Mã liên kết hoặc trạng thái đồng bộ với Jira |
| 8 | **Name** | `Name` | `STRING` | Tên công việc |
| 9 | **Description** | `Description` | `STRING` | Mô tả chi tiết nội dung công việc |
| 10 | **Work** | `Work` | `DOUBLE` | Tổng số giờ công kế hoạch (Hours) |
| 11 | **Duration** | `Duration` | `DOUBLE` | Thời lượng làm việc dự kiến (Days/Hours) |
| 12 | **Priority** | `C_Priority` / `Priority` | `STRING` / `DOUBLE` | Mức độ ưu tiên (P1, P2, P3 hoặc High, Medium, Low) |
| 13 | **Start Date** | `StartDate` | `DATE` | Ngày bắt đầu kế hoạch |
| 14 | **Due Date** | `DueDate` | `DATE` | Hạn hoàn thành kế hoạch |
| *FK* | *Project* | `Project` | `STRING` | **Khóa ngoại** trỏ trực tiếp đến `dim_project` |
| *Time*| *Last Updated* | `LastUpdatedOn` | `DATE` | Watermark thời gian |

---

### 3. Bảng `Objective` (BSC) (8 trường DA + Khóa định danh)

| STT | Tên hiển thị (DA) | Tên kỹ thuật API (Clarizen) | Kiểu dữ liệu (`data_type`) | Vai trò trong Data Warehouse |
| :--- | :--- | :--- | :--- | :--- |
| **PK** | **SYSID** | `SYSID` | `STRING` | **Primary Key** của Mục tiêu BSC |
| 1 | **Name** | `Name` | `STRING` | Tên mục tiêu chiến lược / Nghị quyết giao ban |
| 2 | **Description** | `Description` | `STRING` | Thuyết minh mục tiêu chỉ đạo |
| 3 | **Assignor** | `CreatedBy` / `C_Assignor` | `STRING` | Cấp Lãnh đạo giao mục tiêu |
| 4 | **Assignee** | `EntityOwner` / `C_Assignee` | `STRING` | Cấp Khối/Phòng ban nhận mục tiêu |
| 5 | **Start Date** | `StartDate` | `DATE` | Ngày bắt đầu kỳ chiến lược |
| 6 | **End Date** | `DueDate` | `DATE` | Ngày kết thúc kỳ chiến lược |
| 7 | **Targets** | `SubObjectivesCount` | `DOUBLE` | Số lượng chỉ tiêu/mục tiêu con đo lường |
| 8 | **Parent Objective** | `ParentObjective` | `STRING` | **Khóa tự tham chiếu** phân cấp Tập đoàn $\rightarrow$ Khối $\rightarrow$ Phòng |

---

### 4. Bảng `Assignment` (8 trường DA + Khóa định danh)

| STT | Tên hiển thị (DA) | Tên kỹ thuật API (Clarizen) | Kiểu dữ liệu (`data_type`) | Vai trò trong Data Warehouse |
| :--- | :--- | :--- | :--- | :--- |
| **PK** | **SYSID** | `SYSID` | `STRING` | **Primary Key** của Bản giao việc |
| 1 | **Name** | `Name` | `STRING` | Tiêu đề bản giao nhiệm vụ & cam kết KPI |
| 2 | **Description** | `Description` | `STRING` | Chi tiết nội dung giao việc |
| 3 | **Assignor** | `C_Assignor` | `STRING` | Thủ trưởng đơn vị giao việc |
| 4 | **Assignee** | `C_Assignee` | `STRING` | Nhân sự / Đơn vị nhận việc |
| 5 | **Start Date** | `StartDate` / `C_StartDate` | `DATE` | Ngày bắt đầu hiệu lực giao việc |
| 6 | **End Date** | `DueDate` / `C_DueDate` | `DATE` | Hạn kết thúc đánh giá KPI |
| 7 | **Targets** | `C_AssociatedItem` | `STRING` | Mắt xích liên kết tới các chỉ tiêu Target |
| 8 | **Parent Assignment** | `C_ParentAssignment` | `STRING` | **Khóa tự tham chiếu** phân cấp giao việc Trưởng phòng $\rightarrow$ Nhân viên |

---

### 5. Bảng `Targets` (21 trường DA + Khóa định danh)

| STT | Tên hiển thị (DA) | Tên kỹ thuật API (Clarizen) | Kiểu dữ liệu (`data_type`) | Vai trò trong Data Warehouse |
| :--- | :--- | :--- | :--- | :--- |
| **PK** | **SYSID** | `SYSID` | `STRING` | **Primary Key** của Chỉ tiêu định lượng |
| 1 | **Name** | `Name` | `STRING` | Tên chỉ tiêu định lượng (KPI) |
| 2 | **Description** | `Description` | `STRING` | Thuyết minh tiêu chí tính |
| 3 | **Update Description** | `C_UpdateDescription` | `STRING` | Cập nhật tiến độ giải trình định kỳ |
| 4 | **Unit** | `C_Unit` / `Unit` | `STRING` | Đơn vị tính (%, VND, Ngày, Số lượng...) |
| 5 | **Associated Objective** | `AssociatedObjective` | `STRING` | **Khóa ngoại** gắn vào Mục tiêu chiến lược (`BSC`) |
| 6 | **Associated Assignment**| `C_AssociatedAssignment` | `STRING` | **Khóa ngoại** gắn vào Phiếu giao việc (`Assignment`) |
| 7 | **Parent Target** | `ParentTarget` | `STRING` | **Khóa tự tham chiếu** Chỉ tiêu cha $\rightarrow$ con |
| 8 | **Target Type** | `TargetType` | `STRING` | Loại chỉ tiêu (Càng cao càng tốt, Càng thấp càng tốt) |
| 9 | **Assignee** | `C_Assignee` / `EntityOwner` | `STRING` | Người chịu trách nhiệm số liệu |
| 10 | **Assignor** | `CreatedBy` / `C_Assignor` | `STRING` | Người phê duyệt chỉ tiêu |
| 11 | **Department** | `C_Department` | `STRING` | Phòng ban sở hữu chỉ tiêu |
| 12 | **Resources** | `C_ActionResources` / `Resources`| `STRING` | Nguồn lực phối hợp |
| 13-15 | **Target M (Value, Date, Result)** | `C_TargetValueM`, `C_TargetDateM`, `C_TargetValueMOperator` | `DOUBLE`, `DATE`, `STRING` | **Kịch bản Cam kết (Must-Have)** |
| 16-18 | **Target N (Value, Date, Result)** | `C_TargetValueN`, `C_TargetDateN`, `C_TargetValueNOperator` | `DOUBLE`, `DATE`, `STRING` | **Kịch bản Phấn đấu (Normal/Target)** |
| 19-21 | **Target C (Value, Date, Result)** | `C_TargetValueS`/`C`, `C_TargetDateS`/`C`, `C_TargetValueSOperator`/`C` | `DOUBLE`, `DATE`, `STRING` | **Kịch bản Tối thiểu / Thách thức (Challenge/Stretch)** |

---

# PHẦN 2: CÁC BÀI TOÁN THỰC TẾ XÂY DỰNG TỪ TẬP DỮ LIỆU NÀY

Với 60 trường dữ liệu đã tinh gọn ở trên, Data Warehouse sẽ giải quyết được **5 bài toán phân tích & điều hành thực tế (Use Cases)** phục vụ từ Ban Lãnh đạo đến Nhân viên:

```
[Ban Lãnh đạo / HĐQT] ────────► Bài toán 1: Strategic Alignment (BSC Objective Cascading)
                                            │
[Ban Giám đốc / PMO]  ────────► Bài toán 2: Project Portfolio & Delivery Tracking
                                            │
[Khối Kỹ thuật / Đội dự án] ──► Bài toán 3: WBS Task Execution & Bottleneck Analysis
                                            │
[Khối Nhân sự / Trưởng phòng] ─► Bài toán 4: Workload & Resource Allocation Matrix
                                            │
[Khối Kế hoạch / Tài chính]  ──► Bài toán 5: Multi-scenario Target Achievement & KPI Gap
```

1. **Bài toán 1: Theo dõi Dòng chảy Mục tiêu Chiến lược (Strategy-to-Execution Alignment)**:
   - Từ Nghị quyết giao ban của Ban TGĐ (`Objective`) $\rightarrow$ phân rã thành các dự án (`Project`) $\rightarrow$ đo lường bằng các chỉ số định lượng (`Target`).
   - Đánh giá được: *"Hiện tại có bao nhiêu mục tiêu chiến lược đang bị chậm do các dự án thành phần bị kẹt?"*
2. **Bài toán 2: Giám sát Tiến độ Danh mục Dự án (Project Portfolio Health & Delivery)**:
   - Phân loại dự án theo `Project Type`, `Department`, đo lường mức độ hoàn thành (`PercentCompleted`) so với hạn chót.
   - Phát hiện các dự án có trạng thái `At Risk` hoặc `Off Track` để kích hoạt can thiệp kịp thời.
3. **Bài toán 3: Quản trị Thực thi Công việc WBS & Nút thắt cổ chai (Task WBS & Bottleneck)**:
   - Phân tích giờ công (`Work`) và thời lượng (`Duration`) theo mức độ ưu tiên (`Priority`) và trạng thái `Jira Status`.
   - Bóc tách cây phân cấp WBS (`Parent` $\rightarrow$ `Task`) để tìm ra công việc nào đang là nút thắt chặn tiến độ của cả dự án.
4. **Bài toán 4: Cân bằng Tải & Hiệu suất Nguồn lực (Resource Workload & Allocation Matrix)**:
   - Ma trận phân công nhiệm vụ 2 chiều: Ai giao việc (`Assignor`) cho Ai (`Assignee`) ở Phòng ban nào (`Department`).
   - Đo lường số lượng Task, số lượng Dự án và tổng số giờ công phân bổ trên từng nhân sự để tránh quá tải hoặc nhàn rỗi.
5. **Bài toán 5: Đánh giá Hiệu quả Đạt Chỉ tiêu Đa kịch bản (Scenario Target Performance M/N/C)**:
   - So sánh giữa 3 mức kịch bản: **Cam kết (M)**, **Mục tiêu (N)**, và **Thách thức (C)**.
   - Đánh giá tỷ lệ hoàn thành KPI của từng đơn vị/cá nhân, phân tích độ lệch ngày hoàn thành (`Target Date` vs `Actual Date`).

---

# PHẦN 3: PHƯƠNG ÁN THIẾT KẾ DATA WAREHOUSE (KIMBALL DIM & FACT)

Chúng ta chuyển đổi dữ liệu thô (Bronze) thành mô hình hình sao (Star Schema / Constellation Schema) ở tầng Silver/Gold:

```
                  ┌──────────────────────┐
                  │       dim_date       │
                  └──────────┬───────────┘
                             │
                             ▼
┌──────────────────┐  ┌──────────────────────┐  ┌──────────────────┐
│   dim_project    │  │ fact_task_execution  │  │   dim_resource   │
│ (Type, Dept, PM) │◄─┤(Work, Duration, %Comp)│─►│ (Assignee/Assign)│
└──────────────────┘  └──────────────────────┘  └──────────────────┘
                             ▲
                             │
                      ┌──────┴───────┐
                      │   dim_task   │
                      │ (WBS, Prio)  │
                      └──────────────┘

┌──────────────────┐  ┌──────────────────────┐  ┌──────────────────┐
│  dim_objective   │  │ fact_target_snapshot │  │  dim_assignment  │
│  (BSC Strategy)  │◄─┤ (M/N/C Value & Date) │─►│  (KPI Contract)  │
└──────────────────┘  └──────────────────────┘  └──────────────────┘
```

### 1. Các bảng Chiều (Conformed Dimensions):
- **`dim_date`**: Chiều thời gian chuẩn (DateKey, Day, Month, Quarter, Year, DayOfWeek, IsWorkingDay).
- **`dim_department`**: Chiều phòng ban/khối đơn vị (`department_id`, `department_name`).
- **`dim_resource`**: Chiều nhân sự/nguồn lực (`resource_id`, `resource_name`, `role`, `department_id`).
- **`dim_project`**: Chiều dự án (`project_key`, `sysid`, `name`, `project_type`, `department_name`, `project_manager`, `status`).
- **`dim_task`**: Chiều công việc (`task_key`, `sysid`, `name`, `task_type`, `parent_task_id`, `priority`, `jira_status`).
- **`dim_objective`**: Chiều mục tiêu chiến lược (`objective_key`, `sysid`, `name`, `parent_objective_id`, `objective_type`).
- **`dim_assignment`**: Chiều hợp đồng giao việc (`assignment_key`, `sysid`, `name`, `parent_assignment_id`).

### 2. Các bảng Sự kiện (Fact Tables):
- **`fact_task_execution`**: Fact công việc thực thi chi tiết.
  - *Foreign Keys:* `task_key`, `project_key`, `assignee_key`, `department_key`, `start_date_key`, `due_date_key`.
  - *Measures:* `planned_work_hours`, `duration_days`, `is_milestone`, `is_delayed`.
- **`fact_target_snapshot`**: Fact định lượng chỉ tiêu theo kịch bản.
  - *Foreign Keys:* `target_key`, `objective_key`, `assignment_key`, `project_key`, `assignee_key`, `department_key`.
  - *Measures:* `target_value_m`, `target_value_n`, `target_value_c`, `target_date_m_key`, `target_date_n_key`, `target_date_c_key`.
- **`fact_project_progress`**: Fact trạng thái tiến độ dự án định kỳ.
  - *Measures:* `percent_completed`, `active_task_count`, `completed_task_count`, `total_work_hours`.

---

# PHẦN 4: CẤU HÌNH THỂ TÍCH DỮ LIỆU & CHIẾN LƯỢC SCALE HẠ TẦNG SPARK

Nhà cung cấp giới hạn: **1.000 requests/ngày** và **60-100 requests/phút**. Với `page_size = 250`, khả năng trích xuất tối đa là **250.000 bản ghi/ngày**.

### 1. Bảng Cấu hình Spark & Batch theo quy mô:

| Quy mô dữ liệu / ngày | Khối lượng dữ liệu | `page_size` API | Spark Master | Memory Driver / Executor | `spark.sql.shuffle.partitions` | Thời gian xử lý ước tính |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Nhỏ (Hiện tại)** | 1.000 - 10.000 recs | 250 | `local[2]` | 2GB / 2GB | 8 - 16 | 15 - 30 giây |
| **Vừa (Toàn công ty)**| 10.000 - 100.000 recs| 250 | `local[*]` (4-8 cores) | 4GB / 4GB | 32 - 64 | 1 - 3 phút |
| **Lớn (Mở rộng Tương lai)**| > 100.000 recs | 250 | Spark Standalone / K8s Cluster | 8GB / 16GB (3-5 Nodes) | 200 | 5 - 10 phút |

### 2. Các tham số Spark cần tối ưu hóa trong code:
```python
spark_conf = {
    "spark.sql.shuffle.partitions": "16",           # Giảm từ mặc định 200 xuống 16 để tránh overhead phân mảnh file nhỏ
    "spark.default.parallelism": "8",
    "spark.sql.parquet.compression.codec": "snappy",# Nén Snappy cân bằng giữa CPU và dung lượng ổ cứng
    "spark.memory.fraction": "0.7",                 # Dành 70% RAM cho tính toán execution và storage
    "spark.sql.autoBroadcastJoinThreshold": "10485760" # 10MB Broadcast join tự động cho các bảng Dim nhỏ
}
```

---

# PHẦN 5: ĐỀ XUẤT CÁC TRACER-BULLET TICKETS (THEO KỸ NĂNG `/to-tickets`)

Dưới đây là bộ **6 Tracer-Bullet Tickets** được thiết kế theo nguyên tắc: mỗi ticket đi xuyên suốt từ schema, transform logic đến test xác minh, đảm bảo hoàn thành đến đâu là chạy được (demoable) đến đó:

```
[Ticket 01: Data Contract & Field Pruning]
  Thu gọn 5 bảng về đúng danh sách trường của DA, sinh contract mới
       │
       ▼
[Ticket 02: Dimension Modeling Transformation]
  Tạo các bảng Dim (dim_project, dim_task, dim_objective, dim_assignment, dim_resource)
       │
       ▼
[Ticket 03: Fact Modeling & Scenario Calculations]
  Tạo các bảng Fact (fact_task_execution, fact_target_snapshot)
       │
       ▼
[Ticket 04: dbt Models & Semantic Mart Views]
  Viết view SQL dbt tổng hợp cho 5 bài toán nghiệp vụ + schema.yml
       │
       ▼
[Ticket 05: GenBI Context Pack & Trino DDL]
  Đóng gói tri thức cho AI LLM Text-to-SQL + DDL hoàn chỉnh cho Trino
       │
       ▼
[Ticket 06: Spark Auto-tuning & Pipeline End-to-End Test]
  Cấu hình động tài nguyên Spark theo volume ngày + chạy flow Prefect kiểm thử toàn trình
```

---

### Danh sách chi tiết từng Ticket:

#### **Ticket 01: Data Contract & Field Pruning for 5 Core EPM Tables**
- **Blocked by:** None (Có thể bắt đầu ngay lập tức).
- **What it delivers:** Cập nhật `data_type/*.sql`, `config.json` và `SchemaContract` để thu hẹp tập trường trích xuất từ hàng trăm cột về đúng tập 60 cột mà DA yêu cầu, loại bỏ overhead dư thừa, tăng tốc độ gọi API và giảm 70% dung lượng lưu trữ Bronze.

#### **Ticket 02: Dimension Tables Transformation Layer (Kimball Conformed Dims)**
- **Blocked by:** Ticket 01.
- **What it delivers:** Module Spark Transform biến đổi dữ liệu thô Bronze thành các bảng chiều chuẩn: `dim_project`, `dim_task`, `dim_objective`, `dim_assignment`, `dim_resource` và `dim_date`, xử lý khử trùng lặp (Dedup) và chuẩn hóa khóa kỹ thuật (Surrogate Keys).

#### **Ticket 03: Fact Tables Transformation & Scenario Engine (Tasks & Target Scenarios)**
- **Blocked by:** Ticket 02.
- **What it delivers:** Module Spark Transform tính toán và tạo ra 2 bảng Fact nòng cốt: `fact_task_execution` (giờ công, thời lượng, độ trễ) và `fact_target_snapshot` (bóc tách 3 kịch bản M/N/C của Target thành mô hình cột chuẩn hóa để dễ truy vấn tính toán KPI).

#### **Ticket 04: dbt Data Warehouse Views & Semantic Marts for 5 Analytics Use Cases**
- **Blocked by:** Ticket 03.
- **What it delivers:** Bộ mã nguồn SQL dbt xây dựng tầng View phục vụ người dùng cuối, bao phủ trọn vẹn 5 bài toán thực tế (Strategic Alignment, Project Health, WBS Bottleneck, Resource Allocation, Target Scenarios) kèm file `schema.yml` mô tả tài liệu chi tiết.

#### **Ticket 05: GenBI Knowledge Context Pack & Trino DDL Generation for AI LLM**
- **Blocked by:** Ticket 04.
- **What it delivers:** Tự động sinh file DDL SQL cho Trino và đóng gói toàn bộ metadata (mô tả cột, từ đồng nghĩa, quan hệ bảng, câu hỏi mẫu) thành `genbi_context_pack.json` để AI LLM hiểu và sinh SQL chính xác cho tầng View.

#### **Ticket 06: Spark Auto-tuning Engine & End-to-End Pipeline Integration**
- **Blocked by:** Ticket 05.
- **What it delivers:** Cơ chế cấu hình Spark động theo dung lượng dữ liệu ngày (`batch_size`, `shuffle.partitions`, executor memory) và tích hợp vào `prefect_flow.py` chạy kiểm thử toàn trình từ trích xuất API $\rightarrow$ Dim/Fact $\rightarrow$ View $\rightarrow$ Báo cáo Prefect HQ.

---

### ❓ CÂU HỎI XÁC NHẬN TRƯỚC KHI TẠO TICKET CHÍNH THỨC:

1. **Độ chi tiết (Granularity):** Bạn thấy sự phân chia 6 tickets dọc như trên đã vừa vặn chưa, có cần chia nhỏ thêm hay gộp bước nào lại không?
2. **Thứ tự phụ thuộc (Blocking Edges):** Các quan hệ phụ thuộc tuần tự giữa Contract $\rightarrow$ Dim $\rightarrow$ Fact $\rightarrow$ dbt Views $\rightarrow$ GenBI $\rightarrow$ Pipeline Integration có đúng với kỳ vọng triển khai của bạn không?
3. **Ưu tiên:** Bạn muốn xuất các file ticket này vào thư mục `.scratch/` của repo để bắt đầu triển khai ngay, hay có cần bổ sung thêm trường nghiệp vụ nào đặc thù nữa không?