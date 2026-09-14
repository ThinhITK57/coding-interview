select
    'core' as inventory_group,
    'users' as inventory_item,
    'users' as source_table,
    count(*) as entity_count,
    'All system users' as notes
from {{ ref('stg_users') }}

union all

select
    'core' as inventory_group,
    'active_users' as inventory_item,
    'users' as source_table,
    count(*) as entity_count,
    'Users with is_active = true' as notes
from {{ ref('stg_users') }}
where is_active

union all

select
    'core' as inventory_group,
    'projects' as inventory_item,
    'projects' as source_table,
    count(*) as entity_count,
    'All Lightdash projects' as notes
from {{ ref('stg_projects') }}

union all

select
    'core' as inventory_group,
    'charts' as inventory_item,
    'saved_queries' as source_table,
    count(*) as entity_count,
    'Saved charts' as notes
from {{ ref('stg_charts') }}
where deleted_at is null

union all

select
    'core' as inventory_group,
    'dashboards' as inventory_item,
    'dashboards' as source_table,
    count(*) as entity_count,
    'Active dashboards' as notes
from {{ ref('stg_dashboards') }}
where deleted_at is null

union all

select
    'ai' as inventory_group,
    'ai_agents' as inventory_item,
    'ai_agent' as source_table,
    count(*) as entity_count,
    'All AI agents' as notes
from {{ ref('stg_ai_agents') }}

union all

select
    'ai' as inventory_group,
    'ai_threads' as inventory_item,
    'ai_thread' as source_table,
    count(*) as entity_count,
    'All AI threads' as notes
from {{ ref('stg_ai_threads') }}

union all

select
    'ai' as inventory_group,
    'ai_prompts' as inventory_item,
    'ai_prompt' as source_table,
    count(*) as entity_count,
    'All AI prompts' as notes
from {{ ref('stg_ai_prompts') }}

union all

select
    'ai' as inventory_group,
    'ai_tool_calls' as inventory_item,
    'ai_agent_tool_call' as source_table,
    count(*) as entity_count,
    'All AI tool calls' as notes
from {{ ref('stg_ai_tool_calls') }}

union all

select
    'ai' as inventory_group,
    'ai_tool_results' as inventory_item,
    'ai_agent_tool_result' as source_table,
    count(*) as entity_count,
    'All AI tool results' as notes
from {{ ref('stg_ai_tool_results') }}

union all

select
    'ai' as inventory_group,
    'managed_agent_projects' as inventory_item,
    'managed_agent_settings' as source_table,
    count(*) as entity_count,
    'Projects with managed agent settings' as notes
from {{ ref('stg_managed_agent_settings') }}

union all

select
    'ops' as inventory_group,
    'managed_agent_actions' as inventory_item,
    'managed_agent_actions' as source_table,
    count(*) as entity_count,
    'All managed agent actions' as notes
from {{ ref('stg_managed_agent_actions') }}

union all

select
    'ops' as inventory_group,
    'scheduler_logs' as inventory_item,
    'scheduler_log' as source_table,
    count(*) as entity_count,
    'Scheduler execution logs' as notes
from {{ ref('stg_scheduler_log') }}

union all

select
    'ops' as inventory_group,
    'jobs' as inventory_item,
    'jobs' as source_table,
    count(*) as entity_count,
    'Background jobs' as notes
from {{ ref('stg_jobs') }}

union all

select
    'ops' as inventory_group,
    'job_steps' as inventory_item,
    'job_steps' as source_table,
    count(*) as entity_count,
    'Background job steps' as notes
from {{ ref('stg_job_steps') }}

union all

select
    'ops' as inventory_group,
    'chart_views' as inventory_item,
    'analytics_chart_views' as source_table,
    count(*) as entity_count,
    'Chart view events' as notes
from {{ ref('stg_analytics_chart_views') }}

union all

select
    'ops' as inventory_group,
    'dashboard_views' as inventory_item,
    'analytics_dashboard_views' as source_table,
    count(*) as entity_count,
    'Dashboard view events' as notes
from {{ ref('stg_analytics_dashboard_views') }}

union all

select
    'ops' as inventory_group,
    'app_views' as inventory_item,
    'analytics_app_views' as source_table,
    count(*) as entity_count,
    'App view events' as notes
from {{ ref('stg_analytics_app_views') }}
