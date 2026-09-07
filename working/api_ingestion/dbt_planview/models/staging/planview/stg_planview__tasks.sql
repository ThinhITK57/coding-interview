{{ config(materialized='view') }}

/*
    Model: stg_planview__tasks
    Nhiệm vụ:
    1. Trích xuất và chuẩn hóa kiểu dữ liệu từ bảng Hive/Trino (hive.global_clean.tasks).
    2. Bóc tách an toàn các ID quan hệ từ đối tượng struct lồng nhau (e.g. State.id, Parent.id, Project.id).
    3. Xử lý giá trị NULL mặc định, chuẩn hóa đơn vị tiền tệ và thời gian.
*/

with source_tasks as (
    select * from {{ source('planview_clean', 'tasks') }}
),

renamed_and_cast as (
    select
        -- =========================================================================
        -- 1. ĐỊNH DANH & PHÂN CẤP (IDENTIFIERS & HIERARCHY / WBS)
        -- =========================================================================
        cast(id as varchar)                                         as task_id,
        cast(sysid as varchar)                                      as task_code,               -- e.g. 'T-36140'
        cast(name as varchar)                                       as task_name,
        cast(description as varchar)                                as task_description,

        -- Bóc tách Khóa ngoại (Parent, Project, Phase)
        cast(coalesce(parent_id, json_extract_scalar(parent, '$.id')) as varchar)               as parent_task_id,
        cast(coalesce(project_id, json_extract_scalar(project, '$.id')) as varchar)             as project_id,
        cast(coalesce(parent_project_id, json_extract_scalar(parentproject, '$.id')) as varchar) as parent_project_id,
        cast(coalesce(phase_id, json_extract_scalar(phase, '$.id')) as varchar)                 as phase_id,

        -- Phân loại công việc
        cast(coalesce(milestone, false) as boolean)                 as is_milestone,
        cast(coalesce(oncriticalpath, false) as boolean)            as is_on_critical_path,
        cast(deliverable as varchar)                                as deliverable_name,
        cast(deliverabletype as varchar)                            as deliverable_type,
        cast(tasktype as varchar)                                   as task_type,
        cast(coalesce(childrencount, 0) as integer)                 as children_count,
        cast(coalesce(predecessorscount, 0) as integer)             as predecessors_count,
        cast(coalesce(successorscount, 0) as integer)               as successors_count,

        -- =========================================================================
        -- 2. LỊCH TRÌNH & THỜI GIAN (SCHEDULING, BASELINE & VARIANCE)
        -- =========================================================================
        -- Kế hoạch hiện tại
        cast(startdate as timestamp)                                as planned_start_date,
        cast(duedate as timestamp)                                  as planned_due_date,
        cast(coalesce(duration, 0.0) as double)                     as planned_duration_days,

        -- Cơ sở gốc (Baseline)
        cast(baselinestartdate as timestamp)                        as baseline_start_date,
        cast(baselineduedate as timestamp)                          as baseline_due_date,
        cast(coalesce(baselineduration, 0.0) as double)             as baseline_duration_days,
        cast(coalesce(baselinework, 0.0) as double)                 as baseline_work_hours,

        -- Thực tế diễn ra
        cast(actualstartdate as timestamp)                          as actual_start_date,
        cast(actualenddate as timestamp)                            as actual_end_date,
        cast(coalesce(actualduration, 0.0) as double)               as actual_duration_days,

        -- Độ lệch lịch trình (Variance)
        cast(coalesce(startdatevariance, 0.0) as double)            as start_date_variance_days,
        cast(coalesce(duedatevariance, 0.0) as double)              as due_date_variance_days,
        cast(coalesce(durationvariance, 0.0) as double)             as duration_variance_days,
        cast(coalesce(workvariance, 0.0) as double)                 as work_variance_hours,

        -- =========================================================================
        -- 3. NỖ LỰC & NGUỒN LỰC (EFFORT, TIMESHEET & RESOURCE ALLOCATION)
        -- =========================================================================
        cast(coalesce(work, budgetedhours, 0.0) as double)          as budgeted_work_hours,
        cast(coalesce(actualeffort, 0.0) as double)                 as actual_effort_hours,
        cast(coalesce(remainingeffort, 0.0) as double)              as remaining_effort_hours,
        cast(coalesce(actualbillablehours, 0.0) as double)          as actual_billable_hours,
        cast(coalesce(actualnonbillablehours, 0.0) as double)       as actual_non_billable_hours,

        -- Phân bổ tài nguyên
        cast(coalesce(allocation, 0.0) as double)                   as allocation_percentage,
        cast(coalesce(userresourcescount, 0) as integer)            as user_resources_count,

        -- =========================================================================
        -- 4. TÀI CHÍNH & CHI PHÍ (FINANCIALS: CAPEX, OPEX, LR, NLR)
        -- =========================================================================
        -- Ngân sách dự toán (Budget / Planned)
        cast(coalesce(plannedbudget, plannedamount, 0.0) as double) as planned_budget,
        cast(coalesce(budgetcostlr, 0.0) as double)                 as budget_cost_labor,
        cast(coalesce(budgetcostnlr, 0.0) as double)                as budget_cost_non_labor,
        cast(coalesce(budgetcostcapex, 0.0) as double)              as budget_cost_capex,
        cast(coalesce(budgetcostopex, 0.0) as double)               as budget_cost_opex,
        cast(coalesce(plannedrevenue, 0.0) as double)               as planned_revenue,

        -- Thực tế phát sinh (Actual Costs)
        cast(coalesce(actualcost, 0.0) as double)                   as actual_cost,
        cast(coalesce(actualcostlr, 0.0) as double)                 as actual_cost_labor,
        cast(coalesce(actualcostnlr, 0.0) as double)                as actual_cost_non_labor,
        cast(coalesce(actualcostcapex, 0.0) as double)              as actual_cost_capex,
        cast(coalesce(actualcostopex, 0.0) as double)               as actual_cost_opex,
        cast(coalesce(actualrevenue, 0.0) as double)                as actual_revenue,

        -- =========================================================================
        -- 5. EVM GỐC TỪ HỆ THỐNG NGUỒN (EARNED VALUE MANAGEMENT)
        -- =========================================================================
        cast(coalesce(earnedvalue, 0.0) as double)                  as source_earned_value,
        cast(coalesce(cpi, 0.0) as double)                          as source_cpi,
        cast(coalesce(spi, 0.0) as double)                          as source_spi,
        cast(coalesce(currencyeac, 0.0) as double)                  as source_currency_eac,
        cast(coalesce(currencyetc, 0.0) as double)                  as source_currency_etc,

        -- =========================================================================
        -- 6. TRẠNG THÁI & SỨC KHỎE DỰ ÁN (STATUS & HEALTH)
        -- =========================================================================
        cast(coalesce(state_id, json_extract_scalar(state, '$.id')) as varchar)                 as state_id,
        cast(coalesce(trackstatus, 'Unknown') as varchar)           as track_status,            -- On Track, At Risk, Off Track
        cast(coalesce(percentcompleted, 0.0) as double)             as percent_completed,
        cast(coalesce(laborcostpercentcomplete, 0.0) as double)     as labor_cost_percent_complete,
        cast(coalesce(expectedprogress, 0.0) as double)             as expected_progress,
        cast(coalesce(issuescount, 0) as integer)                   as issues_count,

        -- =========================================================================
        -- 7. NHÂN SỰ, PHÂN QUYỀN & AUDIT (COLLABORATION & AUDIT)
        -- =========================================================================
        cast(coalesce(manager_id, json_extract_scalar(manager, '$.id')) as varchar)             as manager_user_id,
        cast(coalesce(created_by_id, json_extract_scalar(createdby, '$.id')) as varchar)       as created_by_user_id,
        cast(coalesce(entity_owner_id, json_extract_scalar(entityowner, '$.id')) as varchar)   as entity_owner_user_id,
        cast(createdon as timestamp)                                as source_created_at,
        cast(lastupdatedon as timestamp)                            as source_last_updated_at,
        current_timestamp                                           as dbt_ingested_at

    from source_tasks
)

select * from renamed_and_cast
