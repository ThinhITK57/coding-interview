{{ config(
    materialized='table',
    unique_key='task_id'
) }}

/*
    Model: dim_tasks
    Mục đích: Bảng Chiều (Dimension) chứa toàn bộ thuộc tính mô tả, phân cấp WBS,
    và thông tin đường găng của từng Task.
*/

with intermediate_tasks as (
    select * from {{ ref('int_tasks__evm_metrics') }}
)

select
    task_id,
    task_code,
    task_name,
    task_description,
    project_id,
    parent_task_id,
    parent_project_id,
    phase_id,
    manager_user_id,
    created_by_user_id,
    state_id,
    track_status,
    is_milestone,
    is_on_critical_path,
    deliverable_name,
    deliverable_type,
    task_type,
    children_count,
    predecessors_count,
    successors_count,
    planned_start_date,
    planned_due_date,
    planned_duration_days,
    baseline_start_date,
    baseline_due_date,
    baseline_duration_days,
    budgeted_work_hours,
    planned_budget,
    budget_cost_labor,
    budget_cost_non_labor,
    budget_cost_capex,
    budget_cost_opex,
    source_last_updated_at,
    current_timestamp as dbt_updated_at

from intermediate_tasks
