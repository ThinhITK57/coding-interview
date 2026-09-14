select
    cast(created_at as date) as event_date,
    count(*) as prompt_count,
    count(case when human_score is not null then 1 end) as feedback_count,
    count(case when human_score = 1 then 1 end) as upvote_count,
    count(case when human_score = -1 then 1 end) as downvote_count,
    count(case when human_score = 0 then 1 end) as neutral_feedback_count
from {{ ref('stg_ai_prompts') }}
group by 1
