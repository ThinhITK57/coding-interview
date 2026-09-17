CREATE VIEW hive.jira_silver.jira_worklog_fct_vw AS
SELECT
  i.issue_key,
  i.project_key,
  i.assignee,
  w.author,
  w.timespent_hours,
  w.started_time as work_start_time
FROM jira_silver.jira_issue_task_fct_vw i
LEFT JOIN jira_silver.worklog w
  ON i.issue_key = w.issue_key