{{ config(materialized='table') }}

{#
    Grain của model này:
    1 dòng / thread / user / ngày có prompt.

    Model này chỉ dùng character metrics, không tính token.
#}

with prompt_base as (

    select
        p.ai_prompt_uuid,
        p.ai_thread_uuid,
        p.created_by_user_uuid as user_uuid,
        cast(p.created_at as date) as prompt_date,
        p.created_at,

        coalesce(length(p.prompt), 0) as prompt_characters,
        coalesce(length(p.response), 0) as response_characters,
        coalesce(length(p.prompt), 0) + coalesce(length(p.response), 0) as total_characters,

        p.response_time_seconds

    from {{ ref('stg_ai_prompts') }} p

    where p.created_at is not null
      and p.prompt is not null

),

prompt_enriched as (

    select
        pb.*,
        t.agent_uuid,
        t.project_uuid,
        t.organization_uuid,
        t.created_from

    from prompt_base pb

    left join {{ ref('stg_ai_threads') }} t
        on t.ai_thread_uuid = pb.ai_thread_uuid

),

thread_all_time_metrics as (

    select
        ai_thread_uuid,
        count(*) as prompt_count,

        sum(prompt_characters) as total_prompt_characters,
        sum(response_characters) as total_response_characters,
        sum(total_characters) as total_characters,

        avg(prompt_characters) as avg_prompt_characters_per_prompt,
        avg(response_characters) as avg_response_characters_per_prompt,
        avg(total_characters) as avg_total_characters_per_prompt,

        min(created_at) as first_prompt_at,
        max(created_at) as last_prompt_at,

        extract(epoch from (max(created_at) - min(created_at))) as thread_duration_seconds,

        avg(response_time_seconds) as avg_response_time_seconds,
        max(response_time_seconds) as max_response_time_seconds

    from prompt_base

    group by 1

),

thread_response_p95 as (

    select
        ai_thread_uuid,

        percentile_cont(0.95) within group (
            order by response_time_seconds
        ) as p95_response_time_seconds

    from prompt_base

    where response_time_seconds is not null

    group by 1
),

thread_tool_calls as (
    select
        p.ai_thread_uuid,
        count(tc.ai_agent_tool_call_uuid) as tool_call_count
    from {{ ref('stg_ai_prompts') }} p
    left join {{ ref('stg_ai_tool_calls') }} tc
        on tc.ai_prompt_uuid = p.ai_prompt_uuid
    group by 1
),

thread_users as (
    select
        ai_thread_uuid,
        created_by_user_uuid as user_uuid
    from {{ ref('stg_ai_prompts') }}
    where created_by_user_uuid is not null
    group by 1, 2
),

user_thread_counts as (
    select
        user_uuid,
        count(distinct ai_thread_uuid) as total_ai_threads
    from thread_users
    group by 1
),

thread_day_metrics as (

    select
        prompt_date,
        ai_thread_uuid,
        user_uuid,

        count(*) as thread_day_prompt_count,

        sum(prompt_characters) as thread_day_prompt_characters,
        sum(response_characters) as thread_day_response_characters,
        sum(total_characters) as thread_day_total_characters,

        avg(prompt_characters) as thread_day_avg_prompt_characters_per_generation,
        avg(response_characters) as thread_day_avg_response_characters_per_generation,
        avg(total_characters) as thread_day_avg_total_characters_per_generation,

        min(created_at) as thread_day_first_prompt_at,
        max(created_at) as thread_day_last_prompt_at,

        extract(epoch from (max(created_at) - min(created_at))) as thread_day_duration_seconds,

        avg(response_time_seconds) as thread_day_avg_response_time_seconds,
        max(response_time_seconds) as thread_day_max_response_time_seconds

    from prompt_base

    group by 1, 2, 3

),

thread_day_response_p95 as (

    select
        prompt_date,
        ai_thread_uuid,
        user_uuid,

        percentile_cont(0.95) within group (
            order by response_time_seconds
        ) as thread_day_p95_response_time_seconds

    from prompt_base

    where response_time_seconds is not null

    group by 1, 2, 3

),

thread_day_tool_calls as (

    select
        cast(p.created_at as date) as prompt_date,
        p.ai_thread_uuid,
        p.created_by_user_uuid as user_uuid,

        count(tc.ai_agent_tool_call_uuid) as thread_day_tool_call_count

    from {{ ref('stg_ai_prompts') }} p

    left join {{ ref('stg_ai_tool_calls') }} tc
        on tc.ai_prompt_uuid = p.ai_prompt_uuid

    where p.created_at is not null
      and p.prompt is not null

    group by 1, 2, 3

),

daily_usage_by_context as (

    select
        prompt_date,
        agent_uuid,
        project_uuid,
        organization_uuid,
        created_from,

        count(*) as daily_generation_count,
        count(distinct ai_thread_uuid) as daily_active_thread_count,
        count(distinct user_uuid) as daily_active_user_count,

        sum(prompt_characters) as daily_total_prompt_characters,
        sum(response_characters) as daily_total_response_characters,
        sum(total_characters) as daily_total_characters,

        avg(prompt_characters) as daily_avg_prompt_characters_per_generation,
        avg(response_characters) as daily_avg_response_characters_per_generation,
        avg(total_characters) as daily_avg_total_characters_per_generation

    from prompt_enriched

    group by 1, 2, 3, 4, 5

),

daily_response_times_by_context as (

    select
        prompt_date,
        agent_uuid,
        project_uuid,
        organization_uuid,
        created_from,

        avg(response_time_seconds) as daily_avg_response_time_seconds,
        max(response_time_seconds) as daily_max_response_time_seconds,

        percentile_cont(0.95) within group (
            order by response_time_seconds
        ) as daily_p95_response_time_seconds

    from prompt_enriched

    where response_time_seconds is not null

    group by 1, 2, 3, 4, 5

),

daily_thread_usage_by_context as (

    select
        prompt_date,
        agent_uuid,
        project_uuid,
        organization_uuid,
        created_from,
        ai_thread_uuid,

        sum(prompt_characters) as thread_prompt_characters_in_day,
        sum(response_characters) as thread_response_characters_in_day,
        sum(total_characters) as thread_total_characters_in_day

    from prompt_enriched

    group by 1, 2, 3, 4, 5, 6

),

daily_thread_averages_by_context as (

    select
        prompt_date,
        agent_uuid,
        project_uuid,
        organization_uuid,
        created_from,

        avg(thread_prompt_characters_in_day) as daily_avg_prompt_characters_per_thread,
        avg(thread_response_characters_in_day) as daily_avg_response_characters_per_thread,
        avg(thread_total_characters_in_day) as daily_avg_total_characters_per_thread

    from daily_thread_usage_by_context

    group by 1, 2, 3, 4, 5

)

select
    tdm.prompt_date as metric_date,
    t.ai_thread_uuid,
    t.agent_uuid,
    t.agent_name,
    t.project_uuid,
    t.organization_uuid,
    t.created_from,
    t.title,
    t.created_at as thread_created_at,
    cast(t.created_at as date) as thread_date,
    date_trunc('month', t.created_at)::date as thread_month,
    extract(year from t.created_at) as thread_year,

    u.user_uuid,
    u.first_name,
    u.last_name,
    utc.total_ai_threads as user_total_ai_threads,

    -- Thread all-time metrics
    coalesce(tam.prompt_count, 0) as prompt_count,
    coalesce(tam.total_prompt_characters, 0) as total_prompt_characters,
    coalesce(tam.total_response_characters, 0) as total_response_characters,
    coalesce(tam.total_characters, 0) as total_characters,

    coalesce(tam.avg_prompt_characters_per_prompt, 0) as avg_prompt_characters_per_prompt,
    coalesce(tam.avg_response_characters_per_prompt, 0) as avg_response_characters_per_prompt,
    coalesce(tam.avg_total_characters_per_prompt, 0) as avg_total_characters_per_prompt,

    coalesce(ttc.tool_call_count, 0) as tool_call_count,

    tam.first_prompt_at,
    tam.last_prompt_at,
    coalesce(tam.thread_duration_seconds, 0) as thread_duration_seconds,

    coalesce(tam.avg_response_time_seconds, 0) as avg_response_time_seconds,
    coalesce(tam.max_response_time_seconds, 0) as max_response_time_seconds,
    coalesce(trp.p95_response_time_seconds, 0) as p95_response_time_seconds,

    -- Thread daily metrics
    coalesce(tdm.thread_day_prompt_count, 0) as thread_day_prompt_count,

    coalesce(tdm.thread_day_prompt_characters, 0) as thread_day_prompt_characters,
    coalesce(tdm.thread_day_response_characters, 0) as thread_day_response_characters,
    coalesce(tdm.thread_day_total_characters, 0) as thread_day_total_characters,

    coalesce(tdm.thread_day_avg_prompt_characters_per_generation, 0) as thread_day_avg_prompt_characters_per_generation,
    coalesce(tdm.thread_day_avg_response_characters_per_generation, 0) as thread_day_avg_response_characters_per_generation,
    coalesce(tdm.thread_day_avg_total_characters_per_generation, 0) as thread_day_avg_total_characters_per_generation,

    tdm.thread_day_first_prompt_at,
    tdm.thread_day_last_prompt_at,
    coalesce(tdm.thread_day_duration_seconds, 0) as thread_day_duration_seconds,

    coalesce(tdm.thread_day_avg_response_time_seconds, 0) as thread_day_avg_response_time_seconds,
    coalesce(tdm.thread_day_max_response_time_seconds, 0) as thread_day_max_response_time_seconds,
    coalesce(tdrp.thread_day_p95_response_time_seconds, 0) as thread_day_p95_response_time_seconds,

    coalesce(tdtc.thread_day_tool_call_count, 0) as thread_day_tool_call_count,

    -- Daily metrics by context
    coalesce(duc.daily_generation_count, 0) as daily_generation_count,
    coalesce(duc.daily_active_thread_count, 0) as daily_active_thread_count,
    coalesce(duc.daily_active_user_count, 0) as daily_active_user_count,

    coalesce(drtc.daily_avg_response_time_seconds, 0) as daily_avg_response_time_seconds,
    coalesce(drtc.daily_max_response_time_seconds, 0) as daily_max_response_time_seconds,
    coalesce(drtc.daily_p95_response_time_seconds, 0) as daily_p95_response_time_seconds,

    coalesce(duc.daily_total_prompt_characters, 0) as daily_total_prompt_characters,
    coalesce(duc.daily_total_response_characters, 0) as daily_total_response_characters,
    coalesce(duc.daily_total_characters, 0) as daily_total_characters,

    coalesce(duc.daily_avg_prompt_characters_per_generation, 0) as daily_avg_prompt_characters_per_generation,
    coalesce(duc.daily_avg_response_characters_per_generation, 0) as daily_avg_response_characters_per_generation,
    coalesce(duc.daily_avg_total_characters_per_generation, 0) as daily_avg_total_characters_per_generation,

    coalesce(dtac.daily_avg_prompt_characters_per_thread, 0) as daily_avg_prompt_characters_per_thread,
    coalesce(dtac.daily_avg_response_characters_per_thread, 0) as daily_avg_response_characters_per_thread,
    coalesce(dtac.daily_avg_total_characters_per_thread, 0) as daily_avg_total_characters_per_thread

from thread_day_metrics tdm

left join {{ ref('stg_ai_threads') }} t
    on t.ai_thread_uuid = tdm.ai_thread_uuid

left join {{ ref('stg_users') }} u
    on u.user_uuid = tdm.user_uuid

left join user_thread_counts utc
    on utc.user_uuid = tdm.user_uuid

left join thread_all_time_metrics tam
    on tam.ai_thread_uuid = tdm.ai_thread_uuid

left join thread_response_p95 trp
    on trp.ai_thread_uuid = tdm.ai_thread_uuid

left join thread_tool_calls ttc
    on ttc.ai_thread_uuid = tdm.ai_thread_uuid

left join thread_day_response_p95 tdrp
    on tdrp.prompt_date = tdm.prompt_date
   and tdrp.ai_thread_uuid = tdm.ai_thread_uuid
   and tdrp.user_uuid is not distinct from tdm.user_uuid

left join thread_day_tool_calls tdtc
    on tdtc.prompt_date = tdm.prompt_date
   and tdtc.ai_thread_uuid = tdm.ai_thread_uuid
   and tdtc.user_uuid is not distinct from tdm.user_uuid

left join daily_usage_by_context duc
    on duc.prompt_date = tdm.prompt_date
   and duc.agent_uuid is not distinct from t.agent_uuid
   and duc.project_uuid is not distinct from t.project_uuid
   and duc.organization_uuid is not distinct from t.organization_uuid
   and duc.created_from is not distinct from t.created_from

left join daily_response_times_by_context drtc
    on drtc.prompt_date = tdm.prompt_date
   and drtc.agent_uuid is not distinct from t.agent_uuid
   and drtc.project_uuid is not distinct from t.project_uuid
   and drtc.organization_uuid is not distinct from t.organization_uuid
   and drtc.created_from is not distinct from t.created_from

left join daily_thread_averages_by_context dtac
    on dtac.prompt_date = tdm.prompt_date
   and dtac.agent_uuid is not distinct from t.agent_uuid
   and dtac.project_uuid is not distinct from t.project_uuid
   and dtac.organization_uuid is not distinct from t.organization_uuid
   and dtac.created_from is not distinct from t.created_from
