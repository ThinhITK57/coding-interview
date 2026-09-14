with tool_calls as (
    select
        tc.ai_agent_tool_call_uuid,
        tc.ai_prompt_uuid,
        tc.tool_call_id,
        tc.tool_name,
        tc.created_at,
        p.ai_thread_uuid
    from {{ ref('stg_ai_tool_calls') }} tc
    left join {{ ref('stg_ai_prompts') }} p
        on p.ai_prompt_uuid = tc.ai_prompt_uuid
),

tool_results as (
    select distinct
        ai_prompt_uuid,
        tool_call_id,
        result
    from {{ ref('stg_ai_tool_results') }}
)

select
    cast(tc.created_at as date) as event_date,
    tc.tool_name,
    count(distinct tc.ai_agent_tool_call_uuid) as tool_call_count,
    count(distinct case
        when tr.tool_call_id is not null
            and lower(tr.result) not like '%error%'
            then tc.ai_agent_tool_call_uuid
    end) as successful_tool_call_count,
    count(distinct case
        when tr.tool_call_id is null then tc.ai_agent_tool_call_uuid
    end) as missing_result_tool_call_count,
    count(distinct case
        when tr.tool_call_id is not null
            and lower(tr.result) like '%error%'
            then tc.ai_agent_tool_call_uuid
    end) as error_tool_call_count,
    count(distinct tc.ai_thread_uuid) as ai_thread_count
from tool_calls tc
left join tool_results tr
    on tr.ai_prompt_uuid = tc.ai_prompt_uuid
    and tr.tool_call_id = tc.tool_call_id
group by 1, 2
