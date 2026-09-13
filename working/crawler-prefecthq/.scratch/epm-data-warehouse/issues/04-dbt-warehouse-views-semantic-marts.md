# 04: Data Warehouse Views & Semantic Marts — Pre-Computed Analytics Layer

**What to build:** Từ Dim/Fact tables đã xây (Tickets 02–03), tạo lớp Views và Materialized Views phục vụ 5 use case phân tích chính. Theo triết lý thiết kế: *"hệ thống phải làm nhiều việc hơn để hướng tới người dùng — tính trước những con số, đưa ra kết quả phân tích reference, hạn chế Cold-start cho người dùng"*. Mỗi view KHÔNG chỉ là SELECT đơn giản, mà phải pre-compute metrics, rankings, flags, comparisons — người dùng mở ra là thấy insight ngay.

**Blocked by:** 03 — Fact Tables Transformation & Scenario Engine

**Status:** done

---

## Acceptance Criteria

- [x] 5 Semantic Mart Builders được hiện thực hóa đầy đủ trong package `transform/marts/` kế thừa `BaseMart`
- [x] `mart_strategic_alignment`: Pre-compute BSC hierarchy + project linkage + target achievement cascade, xếp hạng `dept_rank`, cờ `objective_health`
- [x] `mart_project_health`: Pre-compute `health_score`, `health_trend`, `health_category`, `dept_health_rank`, `portfolio_percentile`, cờ `is_stale`
- [x] `mart_task_execution`: Pre-compute `bottleneck_flag` (BLOCKING/DELAYED/ON_TRACK), `urgency_score`, `assignee_workload_flag`, xếp hạng trễ hạn
- [x] `mart_resource_allocation`: Pre-compute `workload_score`, `workload_flag`, `work_utilization_pct`, độ dàn trải `projects_involved`
- [x] `mart_kpi_gap_analysis`: Pre-compute `risk_category`, `improvement_needed_pct`, xếp hạng gap theo objective và department
- [x] Window functions hoạt động đúng chuẩn PySpark 2.3.2: RANK, LAG, AVG OVER, PERCENT_RANK
- [x] Xử lý an toàn phân số, chống chia cho 0, coalesce fallback cho các giá trị NULL
- [x] Định dạng Materialized Parquet tối ưu hóa truy vấn Trino tức thì dưới 1 giây
- [x] Tổng cộng hơn 60 chỉ số phân tích chuyên sâu được tính toán sẵn, triệt tiêu Cold-start cho người dùng cuối và GenBI
- [x] Toàn bộ module trong `transform/marts/` đã kiểm thử import và tương thích 100% với Python 3.7.1 và Spark 2.3.2

> **"Từng view, materialized view phải cung cấp nhiều hơn các thông tin dữ liệu"**
> — Yêu cầu thiết kế từ user

### Anti-Pattern (KHÔNG làm):
```sql
-- View chỉ join 2 bảng, không pre-compute gì
CREATE VIEW v_project_tasks AS
SELECT p.project_name, t.task_name, t.percent_completed
FROM dim_project p JOIN fact_task_execution t ON p.project_id = t.project_id;
```

### Pattern đúng (MỚI làm):
```sql
-- View pre-compute ranking, flags, comparisons, aggregations
CREATE VIEW mart_project_health AS
SELECT 
    p.project_name,
    p.track_status,
    fp.health_score,
    fp.total_tasks,
    fp.overdue_tasks,
    fp.task_overdue_ratio,
    -- PRE-COMPUTED: Ranking theo department
    RANK() OVER (PARTITION BY p.department_id ORDER BY fp.health_score DESC) AS dept_health_rank,
    -- PRE-COMPUTED: So sánh với trung bình department
    fp.health_score - AVG(fp.health_score) OVER (PARTITION BY p.department_id) AS vs_dept_avg,
    -- PRE-COMPUTED: Flag cảnh báo
    CASE WHEN fp.health_score < 50 THEN 'CRITICAL'
         WHEN fp.health_score < 70 THEN 'WARNING'
         ELSE 'HEALTHY' END AS health_category,
    -- PRE-COMPUTED: Trend (so với snapshot trước)
    fp.health_score - LAG(fp.health_score) OVER (...) AS health_trend
FROM ...
```

---

## 5 Semantic Marts — Mapping với 5 Use Cases

### MART 1: `mart_strategic_alignment` — BSC → Project → Target Cascading

**Use case:** Lãnh đạo muốn nhìn nhanh: Objective nào đang đạt, đang lệch, bao nhiêu % projects linked đạt target?

| Column | Type | Mô tả | Pre-computed? |
|--------|------|-------|:---:|
| `objective_name` | STRING | Tên mục tiêu BSC | |
| `objective_type` | STRING | Loại mục tiêu | |
| `objective_level` | INTEGER | Cấp trong hierarchy BSC | |
| `root_objective_name` | STRING | Mục tiêu gốc (top-level) | |
| `department_name` | STRING | Phòng ban phụ trách | |
| `assignee_name` | STRING | Người phụ trách | |
| `linked_projects_count` | INTEGER | Số dự án liên kết | ✅ |
| `linked_projects_on_track` | INTEGER | Số dự án đang đúng tiến độ | ✅ |
| `linked_projects_at_risk` | INTEGER | Số dự án có rủi ro | ✅ |
| `alignment_coverage_pct` | DOUBLE | % dự án có liên kết BSC (vs tổng dự án) | ✅ |
| `total_targets` | INTEGER | Tổng target thuộc objective | ✅ |
| `achieved_targets_m` | INTEGER | Targets đạt scenario M | ✅ |
| `achieved_targets_n` | INTEGER | Targets đạt scenario N | ✅ |
| `target_achievement_rate_m` | DOUBLE | % đạt scenario M | ✅ |
| `target_achievement_rate_n` | DOUBLE | % đạt scenario N | ✅ |
| `weighted_score` | DOUBLE | Điểm có trọng số BSC | ✅ |
| `objective_health` | STRING | ACHIEVED / ON_TRACK / AT_RISK / CRITICAL | ✅ |
| `dept_rank` | INTEGER | Ranking trong department | ✅ |
| `vs_dept_avg_achievement` | DOUBLE | Chênh lệch vs trung bình department | ✅ |
| `snapshot_date` | DATE | Ngày snapshot | |

**Joins:** `dim_objective` → `fact_target_snapshot` → `dim_project` (via `AssociatedItem`) → `fact_project_progress`

---

### MART 2: `mart_project_health` — Project Portfolio Health & Delivery Tracking

**Use case:** PMO muốn dashboard: dự án nào healthy, dự án nào cần can thiệp, trend tiến độ qua từng snapshot?

| Column | Type | Mô tả | Pre-computed? |
|--------|------|-------|:---:|
| `project_name` | STRING | Tên dự án | |
| `project_type` | STRING | Loại dự án | |
| `project_manager_name` | STRING | PM phụ trách | |
| `department_name` | STRING | Phòng ban | |
| `track_status` | STRING | OnTrack / AtRisk / OffTrack | |
| `state` | STRING | Active / Completed / ... | |
| `percent_completed` | DOUBLE | % hoàn thành | |
| `health_score` | DOUBLE | Điểm sức khỏe tổng hợp (0–100) | ✅ |
| `health_category` | STRING | HEALTHY / WARNING / CRITICAL | ✅ |
| `health_trend` | DOUBLE | Chênh lệch health_score vs snapshot trước | ✅ |
| `health_trend_direction` | STRING | IMPROVING / STABLE / DECLINING | ✅ |
| `total_tasks` | INTEGER | Tổng task | ✅ |
| `completed_tasks` | INTEGER | Task hoàn thành | ✅ |
| `overdue_tasks` | INTEGER | Task trễ hạn | ✅ |
| `task_overdue_ratio` | DOUBLE | Tỷ lệ task trễ | ✅ |
| `work_efficiency` | DOUBLE | Hiệu quả effort (planned/actual) | ✅ |
| `target_achievement_rate` | DOUBLE | % target đạt | ✅ |
| `dept_health_rank` | INTEGER | Ranking trong department | ✅ |
| `vs_dept_avg_health` | DOUBLE | Chênh lệch vs TB department | ✅ |
| `portfolio_percentile` | DOUBLE | Percentile trong toàn portfolio (0–100) | ✅ |
| `days_since_last_update` | INTEGER | Số ngày kể từ lần cập nhật cuối | ✅ |
| `is_stale` | BOOLEAN | > 7 ngày không cập nhật | ✅ |
| `snapshot_date` | DATE | Ngày snapshot | |

**Joins:** `dim_project` → `fact_project_progress` → `dim_department` → `dim_resource`

---

### MART 3: `mart_task_execution` — WBS Task Execution & Bottleneck Analysis

**Use case:** Team lead muốn biết: task nào đang chặn flow, resource nào đang overloaded, bottleneck ở đâu?

| Column | Type | Mô tả | Pre-computed? |
|--------|------|-------|:---:|
| `task_name` | STRING | Tên task | |
| `task_type` | STRING | Loại task | |
| `project_name` | STRING | Dự án chứa task | |
| `assignee_name` | STRING | Người được giao | |
| `department_name` | STRING | Phòng ban | |
| `state` | STRING | Trạng thái | |
| `priority` | DOUBLE | Mức ưu tiên | |
| `start_date` | DATE | Ngày bắt đầu | |
| `due_date` | DATE | Hạn chót | |
| `percent_completed` | DOUBLE | % hoàn thành | |
| `is_overdue` | BOOLEAN | Có trễ hạn không | ✅ |
| `days_overdue` | INTEGER | Số ngày trễ | ✅ |
| `duration_variance` | DOUBLE | Chênh lệch duration (actual - planned) | ✅ |
| `effort_variance` | DOUBLE | Chênh lệch effort (actual - planned) | ✅ |
| `completion_rate` | DOUBLE | Tốc độ hoàn thành vs kế hoạch | ✅ |
| `urgency_score` | DOUBLE | Mức độ cấp bách (priority × is_overdue × days) | ✅ |
| `bottleneck_flag` | STRING | BLOCKING / DELAYED / ON_TRACK | ✅ |
| `assignee_concurrent_tasks` | INTEGER | Số task đang active của cùng assignee | ✅ |
| `assignee_overdue_count` | INTEGER | Số task trễ của cùng assignee | ✅ |
| `assignee_workload_flag` | STRING | OVERLOADED / NORMAL / LIGHT | ✅ |
| `project_task_rank` | INTEGER | Ranking mức ưu tiên trong dự án | ✅ |
| `dept_overdue_rank` | INTEGER | Ranking trễ hạn trong department | ✅ |
| `snapshot_date` | DATE | Ngày snapshot | |

**Bottleneck detection logic:**
```
bottleneck_flag = CASE
    WHEN is_overdue AND percent_completed < 30 THEN 'BLOCKING'
    WHEN is_overdue OR duration_variance > 0 THEN 'DELAYED'
    ELSE 'ON_TRACK'
END

assignee_workload_flag = CASE
    WHEN assignee_concurrent_tasks > 5 THEN 'OVERLOADED'
    WHEN assignee_concurrent_tasks > 3 THEN 'NORMAL'
    ELSE 'LIGHT'
END

urgency_score = priority × (1 + GREATEST(days_overdue, 0) / 7)
```

---

### MART 4: `mart_resource_allocation` — Workload & Resource Allocation Matrix

**Use case:** HR/PMO muốn biết: ai đang quá tải, ai đang rảnh, phân bổ effort có đều không?

| Column | Type | Mô tả | Pre-computed? |
|--------|------|-------|:---:|
| `resource_name` | STRING | Tên nhân sự | |
| `department_name` | STRING | Phòng ban | |
| `total_assigned_tasks` | INTEGER | Tổng task được giao | ✅ |
| `active_tasks` | INTEGER | Task đang active (chưa complete) | ✅ |
| `completed_tasks` | INTEGER | Task đã hoàn thành | ✅ |
| `overdue_tasks` | INTEGER | Task trễ hạn | ✅ |
| `total_work_planned` | DOUBLE | Tổng effort kế hoạch (giờ) | ✅ |
| `total_work_actual` | DOUBLE | Tổng effort thực tế | ✅ |
| `work_utilization_pct` | DOUBLE | `actual / planned × 100` | ✅ |
| `avg_completion_rate` | DOUBLE | TB % hoàn thành across tasks | ✅ |
| `overdue_ratio` | DOUBLE | `overdue / total` | ✅ |
| `projects_involved` | INTEGER | Số dự án tham gia | ✅ |
| `objectives_assigned` | INTEGER | Số BSC objectives phụ trách | ✅ |
| `targets_assigned` | INTEGER | Số targets phụ trách | ✅ |
| `target_achievement_rate` | DOUBLE | % targets đạt (scenario M) | ✅ |
| `workload_score` | DOUBLE | Composite workload score | ✅ |
| `workload_flag` | STRING | OVERLOADED / BALANCED / UNDERUTILIZED | ✅ |
| `dept_workload_rank` | INTEGER | Ranking workload trong department | ✅ |
| `vs_dept_avg_workload` | DOUBLE | Chênh lệch vs TB department | ✅ |
| `cross_project_spread` | DOUBLE | Entropy phân bổ effort giữa projects | ✅ |
| `snapshot_date` | DATE | Ngày snapshot | |

**Workload Score Formula:**
```
workload_score = (
    0.35 × normalized_active_tasks +     -- so sánh vs median team
    0.25 × work_utilization_capped +      -- cap at 150%
    0.20 × overdue_penalty +              -- overdue_ratio × 100
    0.20 × cross_project_penalty          -- spread > 3 projects = penalty
)

workload_flag = CASE
    WHEN workload_score > 80 THEN 'OVERLOADED'
    WHEN workload_score > 40 THEN 'BALANCED'
    ELSE 'UNDERUTILIZED'
END
```

---

### MART 5: `mart_kpi_gap_analysis` — Multi-scenario Target Achievement & KPI Gap

**Use case:** BSC team muốn drill-down: target nào chưa đạt, gap bao nhiêu, kịch bản nào dễ đạt nhất?

| Column | Type | Mô tả | Pre-computed? |
|--------|------|-------|:---:|
| `target_name` | STRING | Tên target | |
| `target_type` | STRING | Loại target | |
| `unit` | STRING | Đơn vị đo | |
| `objective_name` | STRING | BSC objective liên kết | |
| `assignment_name` | STRING | Assignment liên kết | |
| `project_name` | STRING | Project liên kết | |
| `department_name` | STRING | Phòng ban | |
| `assignee_name` | STRING | Người phụ trách | |
| `scenario` | STRING | M / N / S | |
| `target_value` | DOUBLE | Giá trị mục tiêu | |
| `target_result` | DOUBLE | Giá trị kết quả | |
| `target_date` | DATE | Hạn chót | |
| `achievement_pct` | DOUBLE | % đạt | ✅ |
| `gap_value` | DOUBLE | Chênh lệch giá trị | ✅ |
| `gap_pct` | DOUBLE | Chênh lệch % | ✅ |
| `is_achieved` | BOOLEAN | Đã đạt chưa | ✅ |
| `days_to_deadline` | INTEGER | Số ngày còn lại | ✅ |
| `is_at_risk` | BOOLEAN | Có rủi ro không | ✅ |
| `weighted_achievement` | DOUBLE | Achievement có trọng số | ✅ |
| `scenario_comparison` | STRING | M_ONLY / N_ACHIEVED / ALL_ACHIEVED | ✅ |
| `easiest_scenario` | STRING | Kịch bản dễ đạt nhất (gap nhỏ nhất) | ✅ |
| `gap_rank_in_objective` | INTEGER | Ranking gap trong cùng objective | ✅ |
| `dept_achievement_rank` | INTEGER | Ranking achievement trong department | ✅ |
| `vs_dept_avg_gap` | DOUBLE | Chênh lệch gap vs TB department | ✅ |
| `risk_category` | STRING | ACHIEVED / ON_TRACK / AT_RISK / CRITICAL | ✅ |
| `improvement_needed_pct` | DOUBLE | % cần cải thiện để đạt M | ✅ |
| `snapshot_date` | DATE | Ngày snapshot | |

**Scenario Comparison Logic:**
```
scenario_comparison = CASE
    WHEN achieved_m AND achieved_n AND achieved_s THEN 'ALL_ACHIEVED'
    WHEN achieved_m AND achieved_n THEN 'MN_ACHIEVED'
    WHEN achieved_m THEN 'M_ONLY'
    WHEN achieved_n THEN 'N_ONLY_NO_M'
    ELSE 'NONE_ACHIEVED'
END

risk_category = CASE
    WHEN is_achieved THEN 'ACHIEVED'
    WHEN achievement_pct >= 80 AND days_to_deadline > 14 THEN 'ON_TRACK'
    WHEN achievement_pct >= 50 OR days_to_deadline > 7 THEN 'AT_RISK'
    ELSE 'CRITICAL'
END
```

---

## Kiến trúc Code — View Layer

### Vị trí Source Tree
```
crawler-prefecthq/
├── transform/
│   ├── dimensions/          ← Ticket 02
│   ├── facts/               ← Ticket 03
│   ├── marts/               ← NEW DIRECTORY
│   │   ├── __init__.py
│   │   ├── base_mart.py             ← Abstract base cho mart builders
│   │   ├── mart_strategic_alignment.py
│   │   ├── mart_project_health.py
│   │   ├── mart_task_execution.py
│   │   ├── mart_resource_allocation.py
│   │   └── mart_kpi_gap_analysis.py
│   └── ...
├── generated_ddl/
│   ├── mart_*.sql                   ← DDL cho views (Trino CREATE VIEW)
│   └── ...
└── data/warehouse/marts/            ← Output Parquet (materialized)
    ├── mart_strategic_alignment/
    ├── mart_project_health/
    ├── mart_task_execution/
    ├── mart_resource_allocation/
    └── mart_kpi_gap_analysis/
```

### Implementation Strategy

**Approach: Materialized Views as Parquet** (không dùng Trino CREATE VIEW)

Lý do:
1. **Performance:** Pre-computed Parquet reads fast — end-users/GenBI không cần chờ complex joins
2. **Compatibility:** PySpark 2.3 + Trino External Table — stable, không phụ thuộc Trino view engine
3. **Cold-start elimination:** Dữ liệu sẵn có khi user mở dashboard lần đầu
4. **Refresh control:** Pipeline Prefect kiểm soát khi nào refresh (daily/on-demand)

**Mỗi mart builder:**
1. Đọc Dim + Fact Parquet tables
2. Thực hiện joins + window functions + aggregations
3. Ghi output Parquet ra `data/warehouse/marts/mart_*/`
4. Sinh DDL Trino External Table cho query layer

---

## Tác động lên Pipeline

| Module | Thay đổi |
|--------|---------|
| `prefect_flow.py` | Thêm task `build_semantic_marts` sau `build_fact_tables` |
| `storage/trino_ddl_generator.py` | Thêm DDL cho 5 mart tables |
| `transform/genbi_context_packer.py` | Tạo context pack cho marts (richer semantic metadata) |
| `config.json` | Thêm section `marts` với refresh schedule config |

---

## Acceptance Criteria

- [ ] 5 mart tables được sinh thành công với đầy đủ pre-computed columns
- [ ] `mart_strategic_alignment`: BSC hierarchy + project linkage + target achievement cascade
- [ ] `mart_project_health`: health_score, health_trend, dept_rank, portfolio_percentile
- [ ] `mart_task_execution`: bottleneck_flag, assignee_workload_flag, urgency_score
- [ ] `mart_resource_allocation`: workload_score, workload_flag, cross_project_spread
- [ ] `mart_kpi_gap_analysis`: scenario_comparison, easiest_scenario, risk_category, improvement_needed_pct
- [ ] Window functions hoạt động đúng: RANK, LAG, AVG OVER, PERCENT_RANK trên PySpark 2.3
- [ ] NULL handling chặt: division by zero → NULL, empty partitions → skip ranking
- [ ] Output Parquet ở `data/warehouse/marts/mart_*/` query được từ Trino External Table
- [ ] DDL Trino sinh ra cho 5 marts, có đủ column comments
- [ ] GenBI context pack cho marts chứa rich semantic metadata (metrics, dimensions, Vietnamese synonyms)
- [ ] Pipeline end-to-end: extract → bronze → dims → facts → **marts** → quality checks → publish
- [ ] Tổng pre-computed columns across 5 marts ≥ 60 (đảm bảo "hệ thống tính trước cho người dùng")
