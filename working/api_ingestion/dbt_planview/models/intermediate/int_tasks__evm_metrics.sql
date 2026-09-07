{{ config(materialized='view') }}

/*
    Model: int_tasks__evm_metrics
    Mục đích: 
    Thực hiện tính toán các chỉ số Earned Value Management (EVM) chuẩn quốc tế (PMI),
    chuẩn hóa các trường hợp chia cho 0, và đánh giá chỉ số rủi ro / sức khỏe task.
*/

with base_tasks as (
    select * from {{ ref('stg_planview__tasks') }}
),

calculated_metrics as (
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

        -- Ngày tháng
        planned_start_date,
        planned_due_date,
        planned_duration_days,
        baseline_start_date,
        baseline_due_date,
        baseline_duration_days,
        actual_start_date,
        actual_end_date,
        actual_duration_days,
        start_date_variance_days,
        due_date_variance_days,

        -- Tiến độ & Giờ công
        percent_completed,
        expected_progress,
        budgeted_work_hours,
        actual_effort_hours,
        remaining_effort_hours,
        actual_billable_hours,
        actual_non_billable_hours,
        (budgeted_work_hours - actual_effort_hours) as effort_variance_hours,

        -- =====================================================================
        -- EVM CORE VALUES (PV, EV, AC)
        -- =====================================================================
        -- Planned Value (PV): Giá trị kế hoạch phải đạt tới thời điểm hiện tại
        case
            when expected_progress > 0 then round(planned_budget * (expected_progress / 100.0), 2)
            else planned_budget
        end as planned_value_pv,

        -- Earned Value (EV): Giá trị thực tế đã làm xong tính bằng tiền
        case
            when source_earned_value > 0 then source_earned_value
            when percent_completed > 0 then round(planned_budget * (percent_completed / 100.0), 2)
            else 0.0
        end as earned_value_ev,

        -- Actual Cost (AC): Chi phí thực tế đã chi ra
        actual_cost as actual_cost_ac,
        planned_budget,

        -- Tài chính chi tiết
        budget_cost_labor,
        budget_cost_non_labor,
        budget_cost_capex,
        budget_cost_opex,
        actual_cost_labor,
        actual_cost_non_labor,
        actual_cost_capex,
        actual_cost_opex,
        issues_count,
        source_last_updated_at,
        dbt_ingested_at

    from base_tasks
),

variances_and_indices as (
    select
        *,
        -- 1. Cost Variance (CV) = EV - AC (> 0: Tiết kiệm / Dưới ngân sách, < 0: Bội chi)
        round(earned_value_ev - actual_cost_ac, 2) as cost_variance_cv,

        -- 2. Schedule Variance (SV) = EV - PV (> 0: Vượt tiến độ, < 0: Trễ hạn)
        round(earned_value_ev - planned_value_pv, 2) as schedule_variance_sv,

        -- 3. Cost Performance Index (CPI) = EV / AC (> 1.0: Tốt, < 1.0: Vượt ngân sách)
        case
            when actual_cost_ac > 0 then round(earned_value_ev / actual_cost_ac, 3)
            when earned_value_ev > 0 then 1.0
            else null
        end as cpi,

        -- 4. Schedule Performance Index (SPI) = EV / PV (> 1.0: Tốt, < 1.0: Chậm tiến độ)
        case
            when planned_value_pv > 0 then round(earned_value_ev / planned_value_pv, 3)
            when earned_value_ev > 0 then 1.0
            else null
        end as spi

    from calculated_metrics
),

forecast_and_status as (
    select
        *,
        -- Estimate at Completion (EAC): Dự toán tổng chi phí khi hoàn thành task
        case
            when cpi is not null and cpi > 0 then round(planned_budget / cpi, 2)
            when actual_cost_ac > 0 then round(actual_cost_ac + (planned_budget - earned_value_ev), 2)
            else planned_budget
        end as estimate_at_completion_eac,

        -- Estimate to Complete (ETC): Chi phí cần bỏ thêm từ bây giờ
        case
            when cpi is not null and cpi > 0 then round((planned_budget - earned_value_ev) / cpi, 2)
            else round(greatest(planned_budget - earned_value_ev, 0.0), 2)
        end as estimate_to_complete_etc,

        -- Phân loại sức khỏe tổng hợp (Health Status Tagging)
        case
            when percent_completed >= 100.0 then 'COMPLETED'
            when is_on_critical_path and (due_date_variance_days > 0 or spi < 0.9) then 'CRITICAL_DELAY'
            when spi < 0.85 or cpi < 0.85 then 'HIGH_RISK'
            when spi < 1.0 or cpi < 1.0 then 'AT_RISK'
            else 'ON_TRACK'
        end as evm_health_status

    from variances_and_indices
)

select * from forecast_and_status
