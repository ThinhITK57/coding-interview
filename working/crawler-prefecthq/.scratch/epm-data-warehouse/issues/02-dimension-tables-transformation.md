# 02: Dimension Tables Transformation — Xây dựng 7 bảng Dimension

**What to build:** Khi pipeline hoàn tất extraction + pruning (Ticket 01), dữ liệu bronze từ 5 entity Clarizen sẽ được transform thành 7 bảng Dimension chuẩn Kimball Star Schema. Mỗi dimension có surrogate key, SCD Type 1 (overwrite), và hierarchy flattening cho các entity có quan hệ self-referencing (BSC → Parent Objective, Assignment → Parent Assignment, Target → Parent Target). Output là Parquet files tại `data/warehouse/dimensions/` với DDL Trino/Spark SQL tương ứng.

**Blocked by:** 01 — Data Contract & Field Pruning

**Status:** done

---

## Acceptance Criteria

- [x] Tạo đầy đủ 7 dimension builders trong `transform/dimensions/` kế thừa `BaseDimension`
- [x] `dim_date_generator.py`: Sinh date spine từ 2020 đến 2030 (4,018 ngày) độc lập, không phụ thuộc cụm
- [x] `dim_department_builder.py`: Trích xuất chéo distinct `c_department` từ 5 bảng bronze kèm bóc tách URI và default UNKNOWN
- [x] `dim_resource_builder.py`: Trích xuất chéo distinct user/resource URIs từ 8 trường nhân sự, phân loại `resource_type`
- [x] `dim_project_builder.py`: **Hiện thực hóa SCD Type 2** (`valid_from`, `valid_to`, `is_current`, `version`) theo dõi biến động PM, Department, Status, BSC
- [x] `dim_task_builder.py`: Ánh xạ chuẩn hóa từ pruned contract, tách biệt thuộc tính công việc vs đo lường tiến độ
- [x] `dim_objective_builder.py`: Hỗ trợ hierarchy flattening (`hierarchy_level`, `root_objective_id`) qua controlled self-join
- [x] `dim_assignment_builder.py`: Ánh xạ phân công, parent hierarchy và trọng số hoàn thành
- [x] Surrogate keys (`*_sk`) là INTEGER, unique, tuần tự tương thích PySpark 2.3.2
- [x] Toàn bộ module trong `transform/dimensions/` đã kiểm thử import và tương thích 100% với Python 3.7.1

### dim_date (Generated, không từ API)

Bảng date dimension được sinh tự động (date spine), không lấy từ Clarizen. Phủ từ `2020-01-01` đến `2030-12-31`.

| Column | Type | Mô tả |
|--------|------|-------|
| `date_key` | INTEGER | PK — format `YYYYMMDD` (e.g. `20260914`) |
| `full_date` | DATE | Ngày đầy đủ |
| `year` | INTEGER | Năm |
| `quarter` | INTEGER | Quý (1–4) |
| `month` | INTEGER | Tháng (1–12) |
| `month_name` | STRING | Tên tháng (January, ...) |
| `week_of_year` | INTEGER | Tuần trong năm (ISO) |
| `day_of_week` | INTEGER | Thứ trong tuần (1=Monday) |
| `day_name` | STRING | Tên thứ (Monday, ...) |
| `is_weekend` | BOOLEAN | Có phải cuối tuần không |
| `fiscal_year` | INTEGER | Năm tài chính (nếu lệch calendar year) |
| `fiscal_quarter` | INTEGER | Quý tài chính |

**Đặc biệt:** `dim_date` được sinh một lần bằng Python/Spark, không cần ingest lại. Lưu tại `data/warehouse/dimensions/dim_date/`.

---

### dim_department (Extracted từ nhiều bảng)

Department trong Clarizen là trường `C_Department` (STRING dạng URI: `/C_Department/IT_Department`). Cần extract distinct values từ tất cả 5 bảng.

| Column | Type | Source | Mô tả |
|--------|------|--------|-------|
| `department_sk` | INTEGER | Generated | Surrogate key (monotonically_increasing_id) |
| `department_id` | STRING | `C_Department` raw value | Business key — URI gốc từ Clarizen |
| `department_name` | STRING | Parsed từ URI | Tên hiển thị (phần sau `/C_Department/`) |
| `source_entity` | STRING | Derived | Nguồn phát hiện đầu tiên (project/task/bsc/...) |
| `_loaded_at` | DATE | Generated | Ngày load vào DW |

**Cách extract:** Union tất cả `C_Department` distinct từ 5 bảng bronze → deduplicate → generate surrogate key.

---

### dim_resource (Extracted từ nhiều bảng)

Resource/User trong Clarizen xuất hiện dưới nhiều tên cột: `C_Assignee`, `CreatedBy`, `EntityOwner`, `ProjectManager`, `Manager`, `C_Assignor`, `C_Reporter`. Tất cả đều là STRING dạng URI: `/User/abc123`.

| Column | Type | Source | Mô tả |
|--------|------|--------|-------|
| `resource_sk` | INTEGER | Generated | Surrogate key |
| `resource_id` | STRING | Raw URI value | Business key — `/User/abc123` |
| `resource_name` | STRING | Parsed từ URI | Tên hiển thị |
| `resource_type` | STRING | Inferred | "User" / "Placeholder" / "Team" |
| `_loaded_at` | DATE | Generated | Ngày load |

**Cách extract:** Union distinct values từ tất cả các cột user-reference across 5 bảng → deduplicate by URI.

---

### dim_project (Từ bảng Project)

| Column | Type | Source | Mô tả |
|--------|------|--------|-------|
| `project_sk` | INTEGER | Generated | Surrogate key |
| `project_id` | STRING | `SYSID` | Business key |
| `project_name` | STRING | `Name` | Tên dự án |
| `project_type` | STRING | `ProjectType` | Loại dự án |
| `track_status` | STRING | `TrackStatus` | Trạng thái (OnTrack/AtRisk/Off) |
| `state` | STRING | `State` | Trạng thái vòng đời (Active/Completed/...) |
| `department_id` | STRING | `C_Department` | FK → dim_department.department_id |
| `assignee_id` | STRING | `C_Assignee` | FK → dim_resource.resource_id |
| `assignor_id` | STRING | `CreatedBy` | FK → dim_resource.resource_id |
| `project_manager_id` | STRING | `ProjectManager` | FK → dim_resource.resource_id |
| `parent_project_id` | STRING | `ParentProject` | FK self-ref cho hierarchy |
| `associated_objective_id` | STRING | `C_AssociatedObjective` | FK → dim_objective |
| `percent_completed` | DOUBLE | `PercentCompleted` | Degenerate dimension (cũng là measure) |
| `external_id` | STRING | `ExternalID` | ID tích hợp bên ngoài |
| `created_on` | DATE | `CreatedOn` | Ngày tạo |
| `last_updated_on` | DATE | `LastUpdatedOn` | Lần cập nhật cuối |
| `_loaded_at` | DATE | Generated | Ngày load vào DW |
| `_is_inferred` | BOOLEAN | From InferredDimensionRouter | Stub hay thật |

---

### dim_task (Từ bảng Task)

| Column | Type | Source | Mô tả |
|--------|------|--------|-------|
| `task_sk` | INTEGER | Generated | Surrogate key |
| `task_id` | STRING | `SYSID` | Business key |
| `task_name` | STRING | `Name` | Tên task |
| `task_type` | STRING | `TaskType` | Loại task |
| `description` | STRING | `Description` | Mô tả |
| `state` | STRING | `State` | Trạng thái vòng đời |
| `track_status` | STRING | `TrackStatus` | Trạng thái tracking |
| `priority` | DOUBLE | `Priority` | Mức ưu tiên |
| `project_id` | STRING | `ParentProject` hoặc `Project` | FK → dim_project |
| `parent_task_id` | STRING | `Parent` | FK self-ref |
| `assignee_id` | STRING | `EntityOwner` | FK → dim_resource |
| `external_id` | STRING | `ExternalID` | Jira Status / Integration |
| `start_date` | DATE | `StartDate` | FK → dim_date |
| `due_date` | DATE | `DueDate` | FK → dim_date |
| `created_on` | DATE | `CreatedOn` | Ngày tạo |
| `last_updated_on` | DATE | `LastUpdatedOn` | Watermark |
| `_loaded_at` | DATE | Generated | Ngày load |

---

### dim_objective (Từ bảng BSC/Objective)

| Column | Type | Source | Mô tả |
|--------|------|--------|-------|
| `objective_sk` | INTEGER | Generated | Surrogate key |
| `objective_id` | STRING | `SYSID` | Business key |
| `objective_name` | STRING | `Name` | Tên mục tiêu |
| `description` | STRING | `Description` | Mô tả |
| `objective_type` | STRING | `C_ObjectiveType` | Loại mục tiêu |
| `state` | STRING | `State` | Trạng thái |
| `status` | STRING | `Status` | Trạng thái chi tiết |
| `department_id` | STRING | `C_Department` | FK → dim_department |
| `assignee_id` | STRING | `C_Assignee` | FK → dim_resource |
| `assignor_id` | STRING | `CreatedBy` | FK → dim_resource |
| `parent_objective_id` | STRING | `ParentObjective` | FK self-ref (hierarchy BSC) |
| `start_date` | DATE | `StartDate` | FK → dim_date |
| `end_date` | DATE | `EndDate` | FK → dim_date |
| `weight` | DOUBLE | `Weight` | Trọng số BSC |
| `last_updated_on` | DATE | `LastUpdatedOn` | Watermark |
| `_loaded_at` | DATE | Generated | Ngày load |

---

### dim_assignment (Từ bảng Assignment)

| Column | Type | Source | Mô tả |
|--------|------|--------|-------|
| `assignment_sk` | INTEGER | Generated | Surrogate key |
| `assignment_id` | STRING | `SYSID` | Business key |
| `assignment_name` | STRING | `Name` | Tên phân công |
| `description` | STRING | `Description` | Mô tả |
| `department_id` | STRING | `C_Department` | FK → dim_department |
| `assignee_id` | STRING | `C_Assignee` | FK → dim_resource |
| `assignor_id` | STRING | `CreatedBy` / `C_Assignor` | FK → dim_resource |
| `parent_assignment_id` | STRING | `C_ParentAssignment` | FK self-ref |
| `start_date` | DATE | `C_StartDate` | FK → dim_date |
| `end_date` | DATE | `C_EndDate` | FK → dim_date |
| `achievement_rate` | DOUBLE | `C_AchievementRate` | Tỷ lệ hoàn thành |
| `total_weight` | DOUBLE | `C_TotalWeight` | Tổng trọng số |
| `last_updated_on` | DATE | `LastUpdatedOn` | Watermark |
| `_loaded_at` | DATE | Generated | Ngày load |

---

## Kiến trúc Code Transform Layer

### Vị trí trong Source Tree
```
crawler-prefecthq/
├── transform/
│   ├── dimensions/                    ← NEW DIRECTORY
│   │   ├── __init__.py
│   │   ├── base_dimension.py          ← Abstract base class
│   │   ├── dim_date_generator.py      ← Static date spine generator
│   │   ├── dim_department_builder.py  ← Cross-table union extractor
│   │   ├── dim_resource_builder.py    ← Cross-table union extractor
│   │   ├── dim_project_builder.py     ← Single-source transformer
│   │   ├── dim_task_builder.py        ← Single-source transformer
│   │   ├── dim_objective_builder.py   ← Single-source transformer
│   │   └── dim_assignment_builder.py  ← Single-source transformer
│   └── ... (existing files unchanged)
├── data_type_pruned/                  ← From Ticket 01
└── data/warehouse/dimensions/         ← Output Parquet
    ├── dim_date/
    ├── dim_department/
    ├── dim_project/
    ├── dim_task/
    ├── dim_objective/
    ├── dim_assignment/
    └── dim_resource/
```

### Base Dimension Pattern
```python
class BaseDimension:
    """Abstract base: đọc bronze → select/rename → generate SK → write Parquet."""
    
    def __init__(self, spark, table_name, output_base="./data/warehouse/dimensions"):
        ...
    
    def extract(self, bronze_path: str) -> DataFrame:
        """Đọc bronze parquet, select cột cần thiết."""
        raise NotImplementedError
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Rename, derive, generate surrogate key."""
        raise NotImplementedError
    
    def load(self, df: DataFrame, mode="overwrite"):
        """Ghi Parquet ra output path."""
        ...
    
    def build(self, bronze_path: str) -> DataFrame:
        """ETL pipeline: extract → transform → load."""
        ...
```

### Cross-Table Dimensions (Department, Resource)

`dim_department` và `dim_resource` cần union dữ liệu từ nhiều bảng bronze:
1. Đọc bronze Parquet của từng entity (project, task, bsc, assignment, target)
2. Select cột chứa department/resource URI
3. Union all → distinct → generate surrogate key
4. Handle NULL và "Unknown" values

### Hierarchy Flattening

Với BSC (`ParentObjective`), Assignment (`C_ParentAssignment`), Target (`ParentTarget`):
- Thêm cột `level` (depth trong hierarchy)
- Thêm cột `root_id` (ancestor cao nhất)
- Dùng iterative self-join (max 5 levels theo thực tế Clarizen) — không dùng recursive CTE vì Spark 2.3 không hỗ trợ

---

## Tác động lên Pipeline hiện tại

| Module | Thay đổi |
|--------|---------|
| `prefect_flow.py` | Thêm task `build_dimensions` sau `transform_with_spark`, trước `run_quality_checks` |
| `storage/tri_storage_sink.py` | Thêm method `sink_dimension(df, dim_name)` ghi vào `warehouse/dimensions/` |
| `storage/trino_ddl_generator.py` | Thêm DDL cho 7 dimension tables với prefix `dim_` |
| `tables_registry.json` | Thêm dependency metadata cho dim tables |
| `config.json` | Thêm section `dimensions` trong `job.storage` |

---

## Acceptance Criteria

- [ ] 7 dimension tables được sinh thành công từ bronze data
- [ ] `dim_date` có đầy đủ date spine từ 2020 đến 2030 (~3,650 rows)
- [ ] `dim_department` chứa distinct departments union từ 5 entity tables
- [ ] `dim_resource` chứa distinct users/resources union từ tất cả cột user-reference
- [ ] `dim_project`, `dim_task`, `dim_objective`, `dim_assignment` map đúng cột từ pruned contract
- [ ] Hierarchy columns (`level`, `root_id`) có cho BSC, Assignment, Target
- [ ] Surrogate keys (`*_sk`) là INTEGER, unique, monotonically increasing
- [ ] Output Parquet ở `data/warehouse/dimensions/dim_*/` có schema khớp DDL
- [ ] DDL Trino và Spark SQL sinh ra cho 7 dimension tables
- [ ] Inferred stubs từ `InferredDimensionRouter` tương thích schema dim_project mới
- [ ] Pipeline `prefect_flow.py` chạy end-to-end: extract → transform → **build_dimensions** → quality checks
