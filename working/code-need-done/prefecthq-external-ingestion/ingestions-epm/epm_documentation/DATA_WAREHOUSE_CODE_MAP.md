> **Tài liệu phân tích tổng quan dữ liệu EPM**  
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

## 2. Ma Trận Quan Hệ Phụ Thuộc & Dòng Chảy Dữ Liệu (Data Lineage)

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

