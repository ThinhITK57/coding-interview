{{ config(
    materialized='incremental',
    unique_key=['snapshot_date', 'task_id']
) }}

/*
    Model: fct_task_daily_snapshot
    Mục đích: Bảng Fact chụp ảnh tiến độ (Periodic Snapshot Fact), phục vụ vẽ đồ thị
    hình chữ S (S-Curve), xu hướng EVM và quản lý rủi ro qua từng ngày.
*/

with current_metrics as (
    select * from {{ ref('int_tasks__evm_metrics') }}
)

select
    current_date                                as snapshot_date,
    task_id,
    task_code,
    project_id,
    parent_task_id,
    state_id,
    track_status,
    evm_health_status,
    is_milestone,
    is_on_critical_path,

    -- Tiến độ & Giờ công
    percent_completed,
    expected_progress,
    budgeted_work_hours,
    actual_effort_hours,
    remaining_effort_hours,
    effort_variance_hours,
    actual_billable_hours,
    actual_non_billable_hours,

    -- Các chỉ số EVM cốt lõi
    planned_value_pv,
    earned_value_ev,
    actual_cost_ac,
    cost_variance_cv,
    schedule_variance_sv,
    cpi,
    spi,
    estimate_at_completion_eac,
    estimate_to_complete_etc,

    -- Tài chính thực tế
    actual_cost_labor,
    actual_cost_non_labor,
    actual_cost_capex,
    actual_cost_opex,
    issues_count,
    source_last_updated_at,
    current_timestamp                           as snapshot_ingested_at

from current_metrics

{% if is_incremental() %}
    -- Incremental filter: Chỉ nạp ngày hiện tại nếu chưa có snapshot của ngày hôm nay
    where current_date > (select coalesce(max(snapshot_date), date '1970-01-01') from {{ this }})
{% endif %}
