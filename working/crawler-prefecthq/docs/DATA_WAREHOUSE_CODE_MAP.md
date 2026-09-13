# EPM DATA WAREHOUSE — BẢN ĐỒ MÃ NGUỒN & CẨM NANG BẢO TRÌ (CODE MAP)

> **Tài liệu quản trị, vận hành, nâng cấp và bảo trì hệ thống Data Warehouse**  
> **Dự án:** `crawler-prefecthq` (Planview Clarizen Lakehouse Pipeline)  
> **Phạm vi:** 4 Tickets Kỹ thuật (01 - Schema Pruning, 02 - Dimensions, 03 - Facts, 04 - Semantic Marts)  
> **Môi trường mục tiêu:** Python 3.7.1 | Apache Spark 2.3.2 | Java 8 | Trino (Hive Metastore)  
> **Ngày cập nhật:** 14/09/2026

---

## 1. Tổng Quan Kiến Trúc Tầng Dữ Liệu (Lakehouse Architecture)

Hệ thống Data Warehouse được thiết kế theo mô hình **Kimball Star / Constellation Schema** kết hợp tầng **Semantic Marts tính toán sẵn (Pre-computed Materialized Views)** nhằm triệt tiêu hiện tượng "Cold-start" cho người dùng và trợ lý GenBI:

```
                  ┌─────────────────────────────────────────────────────────┐
                  │                 CLARIZEN REST API v2.0                  │
                  └────────────────────────────┬────────────────────────────┘
                                               │
                                               ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│ TẦNG BRONZE (RAW & CONFORMED LAKEHOUSE)                                                   │
│   - data_type_pruned/           : 5 Schema Contracts tinh gọn (giảm 85.5% số cột)         │
│   - data/warehouse/personal_raw : Parquet append-only + _raw_payload                      │
│   - data/warehouse/global_clean : Parquet deduplicated (sysid + last_updated_on)           │
└──────────────────────┬────────────────────────────────────────────┬───────────────────────┘
                       │                                            │
                       ▼                                            ▼
┌──────────────────────────────────────────────┐ ┌──────────────────────────────────────────┐
│ TẦNG SILVER: DIMENSIONS                      │ │ TẦNG SILVER: FACTS                       │
│ (transform/dimensions/)                      │ │ (transform/facts/)                       │
│   - dim_date (Spine 2020-2030)               │ │   - fact_task_execution (Task Snapshots) │
│   - dim_department (Cross-table Union)       │ │   - fact_target_snapshot (M/N/S Gaps)    │
│   - dim_resource (Cross-table Union)         │ │   - fact_project_progress (Health Score) │
│   - dim_project (SCD Type 2: PM/Dept/Status) │ │                                          │
│   - dim_task, dim_objective, dim_assignment  │ │ Engine: ScenarioEngine (Unpivot M/N/S)   │
└──────────────────────┬───────────────────────┘ └──────────────────┬───────────────────────┘
                       │                                            │
                       └──────────────────────┬─────────────────────┘
                                              │
                                              ▼
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│ TẦNG GOLD: PRE-COMPUTED SEMANTIC MARTS (transform/marts/)                                 │
│   - mart_strategic_alignment : BSC -> Project -> Target Cascading + Weighted Score        │
│   - mart_project_health      : Composite Health Score (40/25/20/15) + Portfolio Percentile│
│   - mart_task_execution      : WBS Bottlenecks (BLOCKING/DELAYED) + Urgency Score         │
│   - mart_resource_allocation : Workload Score (0-100) + Workload Flag + Utilization       │
│   - mart_kpi_gap_analysis    : M/N/S Gap Analysis + Risk Category + Improvement Needed %  │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Bản Đồ Chi Tiết Từng File Mã Nguồn (Source File Inventory)

### 2.1. Tầng Schema Pruning & Hợp Đồng Dữ Liệu (Ticket 01)

| Đường dẫn File | Loại | Mô tả chức năng & Nghiệp vụ | Cột đầu ra |
|:---|:---:|:---|:---:|
| [data_type_pruned/project_dataType.sql](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/data_type_pruned/project_dataType.sql) | SQL Contract | Hợp đồng kiểu dữ liệu bảng Dự án. Cắt giảm từ 364 cột xuống đúng tập DA cần + các khóa kỹ thuật (`SYSID`, `C_AssociatedObjective`, `Manager`, `TrackStatus`...). | 21 cột |
| [data_type_pruned/task_dataType.sql](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/data_type_pruned/task_dataType.sql) | SQL Contract | Hợp đồng bảng Task. Cắt giảm từ 256 cột xuống 25 cột (giữ `Work`, `Duration`, `Priority`, `ParentProject`, `Parent`...). | 25 cột |
| [data_type_pruned/objective_dataType.sql](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/data_type_pruned/objective_dataType.sql) | SQL Contract | Hợp đồng Mục tiêu BSC. Cắt giảm từ 51 cột xuống 22 cột (giữ `ParentObjective`, `Weight`, `C_ObjectiveType`...). | 22 cột |
| [data_type_pruned/c_assignment_dataType.sql](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/data_type_pruned/c_assignment_dataType.sql) | SQL Contract | Hợp đồng Phân công giao việc. Cắt giảm từ 39 cột xuống 19 cột (giữ `C_ParentAssignment`, `C_TotalWeight`...). | 19 cột |
| [data_type_pruned/target_dataType.sql](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/data_type_pruned/target_dataType.sql) | SQL Contract | Hợp đồng Chỉ tiêu Target. Cắt giảm từ 116 cột xuống 33 cột (giữ toàn bộ bộ 3 kịch bản M/N/S cho Value, Date, Result). | 33 cột |
| [transform/schema_contract.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/schema_contract.py) | Python Core | Lớp `SchemaContract` nạp hợp đồng. Đã cập nhật `DEFAULT_CONTRACT_DIR` ưu tiên `./data_type_pruned` và hỗ trợ biến `CONTRACT_DIR`. | — |

---

### 2.2. Tầng Dimensions — 7 Bảng Chiều (Ticket 02)

Thư mục gốc: `transform/dimensions/`

| File | Class chính | Mô tả chức năng kỹ thuật | Nguyên tắc cập nhật (SCD) |
|:---|:---|:---|:---:|
| [base_dimension.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/dimensions/base_dimension.py) | `BaseDimension` | Lớp cơ sở trừu tượng (`ABC`) định nghĩa 4 bước chuẩn: `extract()`, `transform()`, `load()`, `build()`. Ép cấu hình Spark ghi legacy parquet. | — |
| [dim_date_generator.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/dimensions/dim_date_generator.py) | `DimDateGenerator` | Sinh độc lập bảng thời gian chuẩn 2020-2030 (4.018 ngày). Khóa chính `date_key` dạng số `YYYYMMDD`, các thuộc tính tài chính, quý, tuần, thứ. | Static Spine |
| [dim_department_builder.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/dimensions/dim_department_builder.py) | `DimDepartmentBuilder` | Quét và UNION trường `c_department` từ cả 5 bảng bronze. Bóc tách tiền tố URI `/C_Department/`. Tạo bản ghi mặc định `department_sk = 0` (`UNKNOWN`). | SCD Type 1 |
| [dim_resource_builder.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/dimensions/dim_resource_builder.py) | `DimResourceBuilder` | Quét và UNION 8 trường user reference across 5 bảng. Phân loại `resource_type` (User, Placeholder, Team). Tạo bản ghi `resource_sk = 0`. | SCD Type 1 |
| [dim_project_builder.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/dimensions/dim_project_builder.py) | `DimProjectBuilder` | **Triển khai SCD Type 2**: Quản lý lịch sử mốc thời gian qua `valid_from`, `valid_to`, `is_current`, `version`. Phương thức `merge_scd2()` tự đóng bản ghi cũ và mở bản ghi mới khi đổi PM, Dept, Status, BSC. | **SCD Type 2** |
| [dim_task_builder.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/dimensions/dim_task_builder.py) | `DimTaskBuilder` | Ánh xạ cấu trúc WBS (`task_id`, `project_id`, `parent_task_id`, `assignee_id`). Tách riêng thuộc tính mô tả khỏi biến thiên tiến độ. | SCD Type 1 |
| [dim_objective_builder.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/dimensions/dim_objective_builder.py) | `DimObjectiveBuilder` | Tự động làm phẳng cấu trúc cây BSC (Hierarchy Flattening) qua self-join có kiểm soát: xác định `hierarchy_level` và `root_objective_id`. | SCD Type 1 |
| [dim_assignment_builder.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/dimensions/dim_assignment_builder.py) | `DimAssignmentBuilder` | Ánh xạ phân công giao việc, cấu trúc phân công cha-con (`parent_assignment_id`), và các trọng số hoàn thành. | SCD Type 1 |
| [__init__.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/dimensions/__init__.py) | Package Init | Export toàn bộ 7 builders và `BaseDimension`. | — |

---

### 2.3. Tầng Facts & Scenario Engine (Ticket 03)

Thư mục gốc: `transform/facts/`

| File | Class chính | Mô tả chức năng kỹ thuật | Grain dữ liệu |
|:---|:---|:---|:---:|
| [base_fact.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/facts/base_fact.py) | `BaseFact` | Lớp cơ sở cho Facts. Cung cấp phương thức chuẩn hóa `add_date_key()` đổi `DATE` $\rightarrow$ `INTEGER YYYYMMDD` an toàn với fallback `19700101`. | — |
| [scenario_engine.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/facts/scenario_engine.py) | `ScenarioEngine` | **Bộ máy xử lý Đa kịch bản**: Unpivot ngang sang dọc cho 3 kịch bản M/N/S (tương thích Spark 2.3.2). Tính sẵn: `achievement_pct`, `gap_value`, `gap_pct`, `is_achieved`, `days_to_deadline`, `is_at_risk`. | — |
| [fact_task_execution.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/facts/fact_task_execution.py) | `FactTaskExecution` | Snapshot tiến độ thực thi công việc theo ngày. Tính sẵn: `is_overdue`, `days_overdue`, `duration_variance`, `effort_variance`, `effort_burn_rate`. Gán 5 date keys. | 1 row / Task / Snapshot Date |
| [fact_target_snapshot.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/facts/fact_target_snapshot.py) | `FactTargetSnapshot` | Snapshot kết quả mục tiêu. Tích hợp `ScenarioEngine`, liên kết đa chiều với objective, project, assignment, department, assignee. | 1 row / Target / Scenario / Snapshot Date |
| [fact_project_progress.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/facts/fact_project_progress.py) | `FactProjectProgress` | Tổng hợp tiến độ dự án từ Tasks và Targets. **Tính toán Composite Health Score** theo công thức đa chiều (40% tiến độ, 25% kỷ luật task, 20% đạt target, 15% hiệu quả công sức). | 1 row / Project / Snapshot Date |
| [__init__.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/facts/__init__.py) | Package Init | Export toàn bộ 3 Fact builders, `ScenarioEngine`, và `BaseFact`. | — |

---

### 2.4. Tầng Semantic Marts — Pre-Computed Intelligence (Ticket 04)

Thư mục gốc: `transform/marts/`

| File | Class chính | Nghiệp vụ đáp ứng & Chỉ số tính toán sẵn (Pre-computed Metrics) |
|:---|:---|:---|
| [base_mart.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/marts/base_mart.py) | `BaseMart` | Lớp cơ sở điều phối nạp Dims + Facts, chạy transform và xuất bản Materialized Parquet tại `data/warehouse/marts/<mart_name>`. |
| [mart_strategic_alignment.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/marts/mart_strategic_alignment.py) | `MartStrategicAlignment` | **Use Case 1 (BSC -> Project -> Target)**:<br>• `alignment_coverage_pct`: Tỷ lệ dự án có liên kết chiến lược<br>• `target_achievement_rate_m/n`: Tỷ lệ đạt mục tiêu kịch bản M và N<br>• `weighted_score`: Điểm số trọng số BSC<br>• `objective_health`: Phân loại sức khỏe mục tiêu (ACHIEVED / ON_TRACK / AT_RISK / CRITICAL)<br>• `dept_rank`: Thứ hạng mục tiêu trong phòng ban<br>• `vs_dept_avg_achievement`: Độ lệch so với trung bình phòng ban |
| [mart_project_health.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/marts/mart_project_health.py) | `MartProjectHealth` | **Use Case 2 (Sức khỏe Danh mục Dự án)**:<br>• `health_score`: Điểm sức khỏe tổng hợp (0 - 100)<br>• `health_category`: HEALTHY / WARNING / CRITICAL<br>• `health_trend`: Mức độ tăng/giảm điểm so với snapshot trước<br>• `health_trend_direction`: IMPROVING / STABLE / DECLINING<br>• `portfolio_percentile`: Vị trí bách phân vị toàn công ty (0 - 100%)<br>• `dept_health_rank`: Xếp hạng trong khối phòng ban<br>• `is_stale`: Cờ dự án không được cập nhật quá 7 ngày |
| [mart_task_execution.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/marts/mart_task_execution.py) | `MartTaskExecution` | **Use Case 3 (WBS & Điểm nghẽn Thực thi)**:<br>• `bottleneck_flag`: Tự động gán nhãn BLOCKING (trễ & tiến độ < 30%) / DELAYED / ON_TRACK<br>• `urgency_score`: Điểm cấp bách tổng hợp ($Priority \times (1 + days\_overdue / 7)$)<br>• `assignee_concurrent_tasks`: Số task đồng thời của nhân sự<br>• `assignee_workload_flag`: Cờ tải (OVERLOADED / NORMAL / LIGHT)<br>• `project_task_rank` & `dept_overdue_rank`: Xếp hạng độ trễ |
| [mart_resource_allocation.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/marts/mart_resource_allocation.py) | `MartResourceAllocation` | **Use Case 4 (Ma trận Phân bổ Tải & Nguồn lực)**:<br>• `workload_score`: Điểm tải tổng hợp kết hợp 4 tiêu chí (0 - 100)<br>• `workload_flag`: OVERLOADED / BALANCED / UNDERUTILIZED<br>• `work_utilization_pct`: Tỷ lệ thực tế vs kế hoạch ($actual\_effort / work\_planned \times 100$)<br>• `overdue_ratio`: Tỷ lệ trễ hạn cá nhân<br>• `projects_involved`: Độ dàn trải trên nhiều dự án<br>• `company_workload_rank`: Xếp hạng tải toàn công ty |
| [mart_kpi_gap_analysis.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/marts/mart_kpi_gap_analysis.py) | `MartKPIGapAnalysis` | **Use Case 5 (Khoảng cách Đạt Target M/N/S)**:<br>• `gap_value` & `gap_pct`: Khoảng cách số lượng và tỷ lệ % cần bù đắp<br>• `risk_category`: Phân loại rủi ro (ACHIEVED / ON_TRACK / AT_RISK / CRITICAL)<br>• `improvement_needed_pct`: Tỷ lệ nỗ lực cần cải thiện để đạt kịch bản M<br>• `days_to_deadline`: Số ngày đếm ngược đến hạn chót<br>• `gap_rank_in_objective`: Xếp hạng độ lớn của gap trong cùng mục tiêu |
| [__init__.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/marts/__init__.py) | Package Init | Export toàn bộ 5 Semantic Marts và `BaseMart`. |

---

## 3. Ma Trận Quan Hệ Phụ Thuộc & Dòng Chảy Dữ Liệu (Data Lineage)

```
Nguồn Bronze (data/warehouse/global_clean/)
  │
  ├──► [dim_date_generator]        ──► dim_date
  │
  ├──► [dim_department_builder]    ──► dim_department (union: project, task, bsc, assignment, target)
  │
  ├──► [dim_resource_builder]      ──► dim_resource   (union: 8 user columns)
  │
  ├──► [dim_project_builder]       ──► dim_project    (SCD Type 2: PM, Dept, Status, BSC)
  │
  ├──► [dim_task_builder]          ──► dim_task       (WBS structure)
  │
  ├──► [dim_objective_builder]     ──► dim_objective  (Hierarchy Flattening)
  │
  └──► [dim_assignment_builder]    ──► dim_assignment (Assignment Hierarchy)
            │
            ▼
Tầng Facts (data/warehouse/facts/)
  │
  ├──► [fact_task_execution]       ◄── Đọc: bronze tasks + dim_project
  │
  ├──► [fact_target_snapshot]      ◄── Đọc: bronze targets + ScenarioEngine (M/N/S)
  │
  └──► [fact_project_progress]     ◄── Đọc: bronze projects + fact_task_execution + fact_target_snapshot
            │
            ▼
Tầng Semantic Marts (data/warehouse/marts/)
  │
  ├──► [mart_strategic_alignment]  ◄── Đọc: dim_objective + dim_project + fact_target_snapshot + dims
  ├──► [mart_project_health]       ◄── Đọc: fact_project_progress + dim_project (current) + dims
  ├──► [mart_task_execution]       ◄── Đọc: fact_task_execution + dim_task + dim_project + dims
  ├──► [mart_resource_allocation]  ◄── Đọc: dim_resource + fact_task_execution + fact_target_snapshot
  └──► [mart_kpi_gap_analysis]     ◄── Đọc: fact_target_snapshot + dim_objective + dim_project + dims
```

---

## 4. Cẩm Nang Vận Hành, Bảo Trì & Nâng Cấp (Maintenance Playbook)

### Kịch bản 1: DA yêu cầu bổ sung một trường mới từ Clarizen API
*Ví dụ: DA muốn thêm trường `C_RiskLevel` vào bảng Project.*
1. **Bước 1:** Mở file [data_type_pruned/project_dataType.sql](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/data_type_pruned/project_dataType.sql), thêm dòng: `C_RiskLevel STRING,` (đối chuẩn đúng kiểu từ `data_type/project_dataType.sql`).
2. **Bước 2:** Mở [config.json](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/config.json), trong phần `"fields"` của endpoint `projects`, thêm `"C_RiskLevel"`.
3. **Bước 3:** Mở [transform/dimensions/dim_project_builder.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/dimensions/dim_project_builder.py), bổ sung cột ánh xạ vào hàm `transform()`:
   ```python
   F.coalesce(F.col("c_risk_level"), F.lit("Medium")).alias("risk_level"),
   ```
4. **Bước 4:** Nếu muốn biến động mức độ rủi ro này kích hoạt tạo phiên bản mới trong SCD Type 2, chỉ cần thêm `"risk_level"` vào mảng `SCD2_MONITORED_COLUMNS`.

### Kịch bản 2: Điều chỉnh công thức Sức khỏe Dự án (`health_score`)
*Ví dụ: PMO muốn tăng tỷ lệ kỷ luật task từ 25% lên 30% và giảm tỷ lệ đạt target từ 20% xuống 15%.*
1. Mở file [transform/facts/fact_project_progress.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/facts/fact_project_progress.py).
2. Tìm đến bước tính toán `health_score` (dòng ~140) và điều chỉnh hệ số:
   ```python
   "health_score",
   F.round(
       (F.col("percent_completed") * 0.40) +
       ((F.lit(1.0) - F.col("task_overdue_ratio")) * 100.0 * 0.30) +  # Đổi thành 0.30
       (F.col("target_achievement_rate") * 0.15) +                    # Đổi thành 0.15
       (F.col("work_efficiency_capped") * 0.15),
       2
   )
   ```
3. Chạy lại fact builder: `FactProjectProgress(spark).build(sources)`. Toàn bộ các Mart liên quan (`mart_project_health`) sẽ tự động hưởng số liệu mới.

### Kịch bản 3: Thêm một Semantic Mart mới cho Use Case mới
*Ví dụ: Xây dựng Mart chuyên phân tích Ngân sách & Chi phí (`mart_cost_tracking`).*
1. Tạo file mới: `transform/marts/mart_cost_tracking.py` kế thừa từ `BaseMart`.
2. Định nghĩa hàm `extract(dim_tables, fact_tables)` và `build_mart(data)`.
3. Khai báo export trong [transform/marts/__init__.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/marts/__init__.py).
4. Khởi chạy độc lập:
   ```python
   from transform.marts import MartCostTracking
   mart = MartCostTracking(spark).build(dims, facts)
   ```

### Kịch bản 4: Xử lý sự cố lệch dữ liệu hoặc phát hiện lỗi Schema Drift
1. **Kiểm tra Corrupt Records:** Kiểm tra thư mục Dead Letter Queue tại `data/warehouse/dlq/`.
2. **Kiểm tra Schema Contract Loss:** Sử dụng hàm chẩn đoán có sẵn trong [transform/contract_conformer.py](file:///d:/dataguystory/coding-interview-university/working/crawler-prefecthq/transform/contract_conformer.py):
   ```python
   from transform.contract_conformer import cast_quality
   report = cast_quality(df_raw, contract)
   print(report["lossy_columns"])  # Các cột bị mất dữ liệu khi ép kiểu
   ```
3. **Chuyển đổi khẩn cấp về Full Schema (Rollback):**
   Nếu cần quay lại schema đầy đủ 364 cột của Clarizen, đặt biến môi trường:
   ```bash
   set CONTRACT_DIR=./data_type
   ```
   Hệ thống sẽ ngay lập tức nạp hợp đồng gốc mà không cần sửa bất kỳ dòng mã nguồn nào.

---

## 5. Quy Chuẩn Kỹ Thuật Bắt Buộc (Coding Standards)

Để đảm bảo mã nguồn chạy ổn định trên hạ tầng máy chủ công ty, các lập trình viên bảo trì **bắt buộc tuân thủ các nguyên tắc sau**:

1. **Tuân thủ Python 3.7.1**:
   - ❌ KHÔNG dùng cú pháp f-string chứa ký tự escape gạch chéo ngược: `f"{'\n'.join(...)}"`.
   - ❌ KHÔNG dùng toán tử walrus `:=` (chỉ có từ Python 3.8+).
   - ❌ KHÔNG dùng `typing.final` hoặc `Literal` từ thư viện gốc nếu không có `typing_extensions`.
2. **Tuân thủ Apache Spark 2.3.2**:
   - ❌ KHÔNG dùng hàm `df.unpivot()` (chỉ có từ Spark 3.4+) $\rightarrow$ Dùng `ScenarioEngine.unpivot()` qua Union.
   - ❌ KHÔNG dùng `F.desc_nulls_last()` (chỉ có từ Spark 2.4+) $\rightarrow$ Dùng `F.col().desc()` (trong Spark 2.3 `desc()` mặc định đã xếp NULL xuống cuối).
   - ❌ KHÔNG dùng `F.element_at()` hoặc `F.array_sort()` $\rightarrow$ Dùng `regexp_extract` hoặc Python UDF.
   - ⚠️ Luôn duy trì cấu hình `spark.sql.parquet.writeLegacyFormat = true` để tương thích định dạng Decimal/Date với Hive Metastore và Trino.
3. **Khả năng chạy độc lập (Standalone Capability)**:
   - Toàn bộ các builder trong `transform/dimensions/`, `transform/facts/`, và `transform/marts/` đều là **Pure Spark Modules** (không phụ thuộc cứng vào Prefect Server). Có thể chạy bằng lệnh Python trực tiếp, chạy trong Jupyter/Zeppelin Notebook, hoặc gọi qua Airflow/Prefect task tùy ý.
