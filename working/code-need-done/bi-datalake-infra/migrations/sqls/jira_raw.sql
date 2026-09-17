CREATE SCHEMA hive.jira_raw 
with (location='s3a://vcs-raw/jira-raw/');

-- hive.jira_raw.bug_prod definition

CREATE TABLE hive.jira_raw.bug_prod (
   key varchar,
   projectkey varchar,
   projectname varchar,
   summary varchar,
   issuetype varchar,
   status varchar,
   assignee varchar,
   resolved varchar,
   priority varchar,
   created varchar,
   updated varchar,
   duedate varchar,
   source_file varchar,
   created_ts bigint,
   updated_ts bigint,
   resolved_ts bigint,
   duedate_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_raw.defect_task definition

CREATE TABLE hive.jira_raw.defect_task (
   key varchar,
   summary varchar,
   status varchar,
   resolved varchar,
   priority varchar,
   updated varchar,
   created varchar,
   projectkey varchar,
   projectname varchar,
   source_file varchar,
   updated_ts bigint,
   created_ts bigint,
   resolved_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_raw.field_list definition

CREATE TABLE hive.jira_raw.field_list (
   id varchar,
   name varchar,
   custom boolean,
   orderable boolean,
   navigable boolean,
   searchable boolean,
   clausenames array(varchar),
   schema ROW(type varchar, custom varchar, customid bigint, system varchar, items varchar)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_raw.jira_user definition

CREATE TABLE hive.jira_raw.jira_user (
   user_id bigint,
   user_key varchar,
   username varchar,
   display_name varchar,
   email varchar,
   active boolean,
   directory_id bigint,
   source_file varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_raw.kpi_binh_staff definition

CREATE TABLE hive.jira_raw.kpi_binh_staff (
   key varchar,
   summary varchar,
   issuetype varchar,
   status varchar,
   updated varchar,
   created varchar,
   description varchar,
   agent_online double,
   agent_latest double,
   has_soc247 varchar,
   is_exception varchar,
   latest_version varchar,
   contract_type varchar,
   product_type varchar,
   exception_reason varchar,
   reporting_date varchar,
   update_start_date varchar,
   update_date varchar,
   product_name varchar,
   update_frequency varchar,
   version varchar,
   latency_delay double,
   source_file varchar,
   updated_ts bigint,
   created_ts bigint,
   reporting_date_ts bigint,
   update_start_date_ts bigint,
   update_date_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_raw.kpi_kqi definition

CREATE TABLE hive.jira_raw.kpi_kqi (
   key varchar,
   summary varchar,
   quarterly_kpi_result double,
   monthly_accumulated_result double,
   execution_result double,
   reporting_date varchar,
   unit_of_measure double,
   target varchar,
   service_product varchar,
   condition varchar,
   updated varchar,
   source_file varchar,
   reporting_date_ts bigint,
   updated_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_raw.log_work definition

CREATE TABLE hive.jira_raw.log_work (
   projectkey varchar,
   issueid bigint,
   issuekey varchar,
   summary varchar,
   worklogid bigint,
   author varchar,
   started varchar,
   timespent_hours double,
   comment varchar,
   created varchar,
   updated varchar,
   source_file varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_raw.msn_task definition

CREATE TABLE hive.jira_raw.msn_task (
   key varchar,
   summary varchar,
   issuetype varchar,
   status varchar,
   duedate varchar,
   updated varchar,
   created varchar,
   assignee varchar,
   projectkey varchar,
   projectname varchar,
   start_date varchar,
   task_type varchar,
   msn_type double,
   source_file varchar,
   duedate_ts bigint,
   updated_ts bigint,
   created_ts bigint,
   start_date_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_raw.os_project_uat_bugs definition

CREATE TABLE hive.jira_raw.os_project_uat_bugs (
   key varchar,
   summary varchar,
   issuetype varchar,
   status varchar,
   updated varchar,
   created varchar,
   projectkey varchar,
   projectname varchar,
   source_file varchar,
   updated_ts bigint,
   created_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_raw.process_compliance definition

CREATE TABLE hive.jira_raw.process_compliance (
   key varchar,
   summary varchar,
   issuetype varchar,
   duedate varchar,
   ttqt_product_name varchar,
   process_compliance_rate double,
   updated varchar,
   source_file varchar,
   updated_ts bigint,
   duedate_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_raw.project_list definition

CREATE TABLE hive.jira_raw.project_list (
   expand varchar,
   self varchar,
   id varchar,
   key varchar,
   description varchar,
   lead ROW(self varchar, key varchar, name varchar, avatarurls ROW("48x48" varchar, "24x24" varchar, "16x16" varchar, "32x32" varchar), displayname varchar, active boolean),
   name varchar,
   avatarurls ROW("48x48" varchar, "24x24" varchar, "16x16" varchar, "32x32" varchar),
   projectkeys array(varchar),
   projecttypekey varchar,
   archived boolean,
   projectcategory ROW(self varchar, id varchar, name varchar, description varchar),
   url varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_raw.project_management definition

CREATE TABLE hive.jira_raw.project_management (
   key varchar,
   summary varchar,
   assignee varchar,
   status varchar,
   updated varchar,
   project_source varchar,
   pm_sdm varchar,
   start_date varchar,
   contract_plan_start_date varchar,
   contract_plan_end_date varchar,
   project_score double,
   source_file varchar,
   updated_ts bigint,
   contract_plan_start_date_ts bigint,
   start_date_ts bigint,
   contract_plan_end_date_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_raw.task_operation definition

CREATE TABLE hive.jira_raw.task_operation (
   key varchar,
   summary varchar,
   issuetype varchar,
   status varchar,
   updated varchar,
   created varchar,
   assignee varchar,
   priority varchar,
   assignor varchar,
   work_group varchar,
   department_center varchar,
   start_date varchar,
   duedate varchar,
   source_file varchar,
   updated_ts bigint,
   created_ts bigint,
   start_date_ts bigint,
   duedate_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_raw.ulnl definition

CREATE TABLE hive.jira_raw.ulnl (
   key varchar,
   projectkey varchar,
   projectname varchar,
   summary varchar,
   issuetype varchar,
   status varchar,
   assignee varchar,
   resolved varchar,
   ttsx_product varchar,
   total_net_effort double,
   updated varchar,
   source_file varchar,
   resolved_ts bigint,
   updated_ts bigint
)
WITH (
   format = 'PARQUET'
);