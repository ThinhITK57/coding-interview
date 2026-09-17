
 CREATE view hive.jira_silver.jira_issue_effort_fct_vw AS
 SELECT
    i.issue_key,
    i.project_key,
    i.status,
    u.display_name,
    f.total_net_effort,
    f.resolved_time
FROM jira_silver.jira_issue_task_fct_vw i
LEFT JOIN jira_silver.estimated_effort f
    ON i.issue_key = f.issue_key
LEFT JOIN jira_silver.jira_user u
    ON i.assignee = u.username