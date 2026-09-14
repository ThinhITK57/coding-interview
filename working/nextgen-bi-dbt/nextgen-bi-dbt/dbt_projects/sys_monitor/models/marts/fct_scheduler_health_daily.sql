select
    cast(created_at as date) as event_date,
    task,
    job_group,
    target_type,
    count(*) as scheduler_event_count,
    count(case when status in ('complete', 'completed', 'success', 'succeeded') then 1 end) as successful_scheduler_event_count,
    count(case when status in ('failed', 'error') then 1 end) as failed_scheduler_event_count
from {{ ref('stg_scheduler_log') }}
group by 1, 2, 3, 4
