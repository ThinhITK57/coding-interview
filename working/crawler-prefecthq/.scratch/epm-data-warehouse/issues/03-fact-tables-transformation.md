# 03: Fact Tables Transformation & Scenario Engine — 3 bảng Fact + Multi-Scenario KPI

**What to build:** Từ bronze data đã pruned (Ticket 01) và dimension tables đã xây (Ticket 02), transform thành 3 bảng Fact chuẩn Kimball: `fact_task_execution` (theo dõi tiến độ task), `fact_target_snapshot` (theo dõi target KPI theo 3 kịch bản M/N/S), và `fact_project_progress` (tổng hợp tiến độ dự án). Bao gồm Scenario Engine để tính toán gap analysis, variance, và achievement rate cho Target KPI.

**Blocked by:** 02 — Dimension Tables Transformation

**Status:** done

---

## Acceptance Criteria

- [x] Tạo đầy đủ package `transform/facts/` với `BaseFact`, `ScenarioEngine`, và 3 Fact Builders
- [x] `fact_task_execution.py`: Grain 1 row/task/snapshot date, tính `is_overdue`, `days_overdue`, `duration_variance`, `effort_variance`, `effort_burn_rate`
- [x] `scenario_engine.py`: Unpivot 3 kịch bản M/N/S tương thích Spark 2.3.2, tính toán `achievement_pct`, `gap_value`, `gap_pct`, `is_achieved`, `days_to_deadline`, `is_at_risk`, `weighted_achievement`
- [x] `fact_target_snapshot.py`: Tích hợp ScenarioEngine, liên kết đa chiều mục tiêu, sinh `target_snapshot_sk`
- [x] `fact_project_progress.py`: Tổng hợp số liệu task và target theo project, tính toán `health_score` theo công thức đa chiều (40% tiến độ, 25% kỷ luật task, 20% đạt mục tiêu, 15% hiệu quả công sức)
- [x] Chuyển đổi Date Keys chuẩn hóa sang INTEGER YYYYMMDD (`start_date_key`, `due_date_key`, `snapshot_date_key`) khớp với `dim_date.date_key`
- [x] Xử lý an toàn mẫu số 0 (ZeroDivisionError protection) và giá trị NULL bằng `coalesce` và `when`
- [x] Toàn bộ module trong `transform/facts/` đã kiểm thử import và tương thích 100% với Python 3.7.1 và Spark 2.3.2

### fact_task_execution (Grain: 1 row per Task per Snapshot Date)

Tracking tiến độ thực thi task: so sánh kế hoạch vs thực tế, phát hiện delay, bottleneck.

| Column | Type | Source | Mô tả |
|--------|------|--------|-------|
| `task_execution_sk` | INTEGER | Generated | Surrogate key |
| `task_id` | STRING | `SYSID` | FK → dim_task (degenerate) |
| `project_id` | STRING | `ParentProject` / `Project` | FK → dim_project |
| `assignee_id` | STRING | `EntityOwner` | FK → dim_resource |
| `start_date_key` | INTEGER | `StartDate` → YYYYMMDD | FK → dim_date |
| `due_date_key` | INTEGER | `DueDate` → YYYYMMDD | FK → dim_date |
| `actual_start_date_key` | INTEGER | `ActualStartDate` → YYYYMMDD | FK → dim_date |
| `actual_end_date_key` | INTEGER | `ActualEndDate` → YYYYMMDD | FK → dim_date |
| `snapshot_date_key` | INTEGER | `ingest_date` → YYYYMMDD | FK → dim_date — ngày chụp ảnh |
| **— Measures —** | | | |
| `percent_completed` | DOUBLE | `PercentCompleted` | % hoàn thành (0–100) |
| `work_planned` | DOUBLE | `Work` | Effort kế hoạch (giờ) |
| `duration_planned` | DOUBLE | `Duration` | Duration kế hoạch (ngày) |
| `actual_duration` | DOUBLE | `ActualDuration` | Duration thực tế |
| `actual_effort` | DOUBLE | `ActualEffort` | Effort thực tế |
| `priority` | DOUBLE | `Priority` | Mức ưu tiên |
| **— Derived Measures —** | | | |
| `is_overdue` | BOOLEAN | Derived | `DueDate < TODAY AND PercentCompleted < 100` |
| `days_overdue` | INTEGER | Derived | `MAX(0, DATEDIFF(TODAY, DueDate))` khi chưa hoàn thành |
| `duration_variance` | DOUBLE | Derived | `ActualDuration - Duration` (dương = trễ) |
| `effort_variance` | DOUBLE | Derived | `ActualEffort - Work` (dương = vượt effort) |
| `completion_rate` | DOUBLE | Derived | `PercentCompleted / ExpectedProgress * 100` (>100 = ahead) |
| **— Audit —** | | | |
| `_batch_id` | STRING | Pipeline | Batch identifier |
| `_loaded_at` | DATE | Generated | Ngày load vào DW |

---

### fact_target_snapshot (Grain: 1 row per Target per Scenario per Snapshot Date)

Theo dõi KPI target theo 3 kịch bản: M (Must-have/Minimum), N (Normal/Expected), S/C (Stretch/Challenge). Đây là bảng trung tâm cho use case "Multi-scenario Target Achievement & KPI Gap Analysis".

| Column | Type | Source | Mô tả |
|--------|------|--------|-------|
| `target_snapshot_sk` | INTEGER | Generated | Surrogate key |
| `target_id` | STRING | `SYSID` | FK → (degenerate key cho Target) |
| `objective_id` | STRING | `AssociatedObjective` | FK → dim_objective |
| `assignment_id` | STRING | `C_AssociatedAssignment` | FK → dim_assignment |
| `project_id` | STRING | `AssociatedItem` | FK → dim_project |
| `assignee_id` | STRING | `C_Assignee` | FK → dim_resource |
| `department_id` | STRING | `C_Department` | FK → dim_department |
| `snapshot_date_key` | INTEGER | `ingest_date` → YYYYMMDD | FK → dim_date |
| **— Scenario Measures —** | | | |
| `scenario` | STRING | Derived | 'M' / 'N' / 'S' — unpivoted scenario code |
| `target_value` | DOUBLE | `C_TargetValueM/N/S` | Giá trị mục tiêu |
| `target_result` | DOUBLE | `C_TargetResultValueM/N/S` | Giá trị kết quả thực tế |
| `target_date` | DATE | `C_TargetDateM/N/S` | Hạn chót mục tiêu |
| **— Common Measures —** | | | |
| `target_name` | STRING | `Name` | Tên target |
| `target_type` | STRING | `TargetType` | Loại target |
| `unit` | STRING | `Unit` | Đơn vị đo |
| `weight` | DOUBLE | `Weight` | Trọng số trong BSC |
| `percent_completed` | DOUBLE | `PercentCompleted` | % hoàn thành tổng |
| `state` | STRING | `State` | Trạng thái |
| **— Derived Measures (Scenario Engine) —** | | | |
| `achievement_pct` | DOUBLE | Derived | `(target_result / target_value) * 100` — NULL-safe |
| `gap_value` | DOUBLE | Derived | `target_value - target_result` (dương = thiếu) |
| `gap_pct` | DOUBLE | Derived | `gap_value / target_value * 100` |
| `is_achieved` | BOOLEAN | Derived | `target_result >= target_value` |
| `days_to_deadline` | INTEGER | Derived | `DATEDIFF(target_date, snapshot_date)` |
| `is_at_risk` | BOOLEAN | Derived | `NOT is_achieved AND days_to_deadline <= 7` |
| `weighted_achievement` | DOUBLE | Derived | `achievement_pct * weight / 100` |
| **— Audit —** | | | |
| `_batch_id` | STRING | Pipeline | Batch identifier |
| `_loaded_at` | DATE | Generated | Ngày load |

#### Scenario Engine — Unpivot Logic

Dữ liệu Target từ Clarizen lưu 3 kịch bản theo cột (wide format):
```
C_TargetValueM | C_TargetValueN | C_TargetValueS
C_TargetResultValueM | C_TargetResultValueN | C_TargetResultValueS
C_TargetDateM | C_TargetDateN | C_TargetDateS
```

Cần unpivot sang long format (1 row per scenario):
```python
# PySpark 2.3 compatible — không có unpivot() built-in
scenarios = [
    ("M", "c_target_value_m", "c_target_result_value_m", "c_target_date_m"),
    ("N", "c_target_value_n", "c_target_result_value_n", "c_target_date_n"),
    ("S", "c_target_value_s", "c_target_result_value_s", "c_target_date_s"),
]

dfs = []
for code, val_col, result_col, date_col in scenarios:
    scenario_df = base_df.select(
        F.lit(code).alias("scenario"),
        F.col(val_col).alias("target_value"),
        F.col(result_col).alias("target_result"),
        F.col(date_col).alias("target_date"),
        *common_cols
    )
    dfs.append(scenario_df)

fact_target = dfs[0].union(dfs[1]).union(dfs[2])
```

**Lưu ý:** Chỉ tạo row cho scenario có `target_value IS NOT NULL` — tránh inflate bảng với row rỗng.

---

### fact_project_progress (Grain: 1 row per Project per Snapshot Date)

Tracking tiến độ tổng thể dự án: aggregation từ task-level lên project-level, kết hợp trực tiếp project attributes.

| Column | Type | Source | Mô tả |
|--------|------|--------|-------|
| `project_progress_sk` | INTEGER | Generated | Surrogate key |
| `project_id` | STRING | `SYSID` | FK → dim_project |
| `department_id` | STRING | `C_Department` | FK → dim_department |
| `project_manager_id` | STRING | `ProjectManager` | FK → dim_resource |
| `snapshot_date_key` | INTEGER | `ingest_date` → YYYYMMDD | FK → dim_date |
| **— Direct Project Measures —** | | | |
| `percent_completed` | DOUBLE | `PercentCompleted` | % hoàn thành dự án |
| `track_status` | STRING | `TrackStatus` | OnTrack / AtRisk / OffTrack |
| `state` | STRING | `State` | Active / Completed / ... |
| **— Aggregated from Tasks —** | | | |
| `total_tasks` | INTEGER | Derived | COUNT tasks thuộc project |
| `completed_tasks` | INTEGER | Derived | COUNT tasks có State='Completed' |
| `overdue_tasks` | INTEGER | Derived | COUNT tasks có is_overdue=True |
| `avg_task_completion` | DOUBLE | Derived | AVG(PercentCompleted) of tasks |
| `total_work_planned` | DOUBLE | Derived | SUM(Work) of tasks |
| `total_work_actual` | DOUBLE | Derived | SUM(ActualEffort) of tasks |
| **— Aggregated from Targets —** | | | |
| `total_targets` | INTEGER | Derived | COUNT targets linked to project |
| `achieved_targets_m` | INTEGER | Derived | COUNT targets đạt scenario M |
| `achieved_targets_n` | INTEGER | Derived | COUNT targets đạt scenario N |
| `target_achievement_rate` | DOUBLE | Derived | % targets đạt / tổng targets |
| **— Derived Health Indicators —** | | | |
| `health_score` | DOUBLE | Derived | Composite score (xem công thức bên dưới) |
| `task_overdue_ratio` | DOUBLE | Derived | `overdue_tasks / total_tasks` |
| `work_efficiency` | DOUBLE | Derived | `total_work_planned / total_work_actual` (>1 = hiệu quả) |
| `is_at_risk` | BOOLEAN | Derived | `health_score < 50 OR task_overdue_ratio > 0.3` |
| **— Audit —** | | | |
| `_batch_id` | STRING | Pipeline | Batch identifier |
| `_loaded_at` | DATE | Generated | Ngày load |

#### Health Score Formula

```
health_score = (
    0.40 × percent_completed +
    0.25 × (1 - task_overdue_ratio) × 100 +
    0.20 × target_achievement_rate +
    0.15 × work_efficiency_capped
)

where work_efficiency_capped = MIN(work_efficiency, 1.5) × 66.67
      (cap at 1.5 to avoid extreme outlier inflation, normalize to 0-100)
```

**Weights rationale:**
- 40% tiến độ trực tiếp (% completed) — chỉ số quan trọng nhất
- 25% quản lý task (tỷ lệ không trễ)
- 20% đạt target KPI
- 15% hiệu quả sử dụng effort

---

## Kiến trúc Code

### Vị trí Source Tree
```
crawler-prefecthq/
├── transform/
│   ├── dimensions/          ← From Ticket 02
│   ├── facts/               ← NEW DIRECTORY
│   │   ├── __init__.py
│   │   ├── base_fact.py            ← Abstract base class
│   │   ├── fact_task_execution.py  ← Task snapshot builder
│   │   ├── fact_target_snapshot.py ← Target unpivot + scenario engine
│   │   ├── fact_project_progress.py ← Project aggregation builder
│   │   └── scenario_engine.py      ← Reusable M/N/S unpivot + gap calc
│   └── ...
└── data/warehouse/facts/           ← Output Parquet
    ├── fact_task_execution/
    ├── fact_target_snapshot/
    └── fact_project_progress/
```

### Base Fact Pattern
```python
class BaseFact:
    """Abstract: đọc bronze → join dims → compute measures → write Parquet."""
    
    def __init__(self, spark, fact_name, output_base="./data/warehouse/facts"):
        ...
    
    def extract_bronze(self, bronze_paths: dict) -> dict:
        """Đọc tất cả bronze tables cần thiết cho fact này."""
        raise NotImplementedError
    
    def compute_measures(self, dfs: dict) -> DataFrame:
        """Tính toán derived measures, join, aggregate."""
        raise NotImplementedError
    
    def add_date_keys(self, df: DataFrame, date_columns: list) -> DataFrame:
        """Convert DATE columns → INTEGER date keys (YYYYMMDD format)."""
        ...
    
    def load(self, df: DataFrame, mode="overwrite"):
        """Ghi Parquet."""
        ...
```

### Scenario Engine (Reusable)
```python
class ScenarioEngine:
    """Unpivot Target M/N/S scenarios + compute gap analysis metrics."""
    
    SCENARIOS = [
        ("M", "Must-have / Minimum"),
        ("N", "Normal / Expected"),
        ("S", "Stretch / Challenge"),
    ]
    
    def unpivot(self, df, common_cols):
        """Wide → Long: 1 row per target per scenario."""
        ...
    
    def compute_gaps(self, df):
        """Add achievement_pct, gap_value, gap_pct, is_achieved, is_at_risk."""
        ...
    
    def compute_weighted(self, df):
        """Add weighted_achievement = achievement_pct × weight / 100."""
        ...
```

---

## Execution Order trong Pipeline

```
extract_from_api  →  transform_with_spark (bronze)
                          ↓
                  build_dimensions (Ticket 02)
                          ↓
                  build_fact_tables (Ticket 03)  ← THIS TICKET
                     ├── fact_task_execution (đọc bronze task + dim_project)
                     ├── fact_target_snapshot (đọc bronze target + scenario engine)
                     └── fact_project_progress (aggregate task facts + target facts + project bronze)
                          ↓
                  run_quality_checks
```

**Thứ tự build facts:**
1. `fact_task_execution` — chỉ cần bronze task + bronze project FK
2. `fact_target_snapshot` — chỉ cần bronze target + scenario engine
3. `fact_project_progress` — cần kết quả từ 1 và 2 (aggregate lên project level)

---

## Tác động lên Pipeline hiện tại

| Module | Thay đổi |
|--------|---------|
| `prefect_flow.py` | Thêm task `build_fact_tables` với 3 sub-tasks cho từng fact |
| `storage/tri_storage_sink.py` | Thêm method `sink_fact(df, fact_name)` ghi vào `warehouse/facts/` |
| `storage/trino_ddl_generator.py` | Thêm DDL cho 3 fact tables với prefix `fact_` |
| `config.json` | Thêm section `facts` trong `job.storage` với dependencies |

---

## Acceptance Criteria

- [ ] `fact_task_execution` sinh đúng grain (1 row per task per snapshot date)
- [ ] `fact_target_snapshot` unpivot thành công 3 scenarios M/N/S (3x rows nếu cả 3 có dữ liệu)
- [ ] Scenario Engine tính đúng: `achievement_pct`, `gap_value`, `gap_pct`, `is_achieved`, `is_at_risk`, `weighted_achievement`
- [ ] `fact_project_progress` aggregate đúng task counts và target achievement từ 2 fact trên
- [ ] `health_score` tính đúng theo formula (40%/25%/20%/15%)
- [ ] Derived measures: `is_overdue`, `days_overdue`, `duration_variance`, `effort_variance` đúng logic
- [ ] Date columns được convert sang INTEGER date keys (YYYYMMDD) — match `dim_date.date_key`
- [ ] NULL handling: division by zero → NULL, not error; missing scenario → skip row, not NULL row
- [ ] Output Parquet ở `data/warehouse/facts/fact_*/` có schema nhất quán
- [ ] DDL Trino và Spark SQL sinh ra cho 3 fact tables
- [ ] Pipeline chạy end-to-end: extract → bronze → dims → **facts** → quality checks
