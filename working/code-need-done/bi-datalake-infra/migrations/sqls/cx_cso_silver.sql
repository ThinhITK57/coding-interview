CREATE SCHEMA hive.cx_cso_silver
with (location='s3a://vcs-silver/cx-cso-silver/');


-- hive.cx_cso_silver.audit_contacts_to_clean definition

CREATE TABLE hive.cx_cso_silver.audit_contacts_to_clean (
   source_system varchar,
   contact_id varchar,
   company_id varchar,
   company_name varchar,
   original_name varchar,
   email varchar,
   suspect_reason varchar,
   updated_at timestamp(3),
   audit_detected_at timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_silver.audit_domain_suggestion definition

CREATE TABLE hive.cx_cso_silver.audit_domain_suggestion (
   email_domain varchar,
   contact_id varchar,
   email varchar,
   current_company_name varchar,
   suggested_company_name varchar,
   mapping_status varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_silver.companies definition

CREATE TABLE hive.cx_cso_silver.companies (
   id bigint,
   name varchar,
   description varchar,
   note varchar,
   created_at varchar,
   updated_at varchar,
   company_alias varchar,
   tax_code varchar,
   company_segment varchar,
   company_type varchar,
   health_score varchar,
   account_tier varchar,
   renewal_date varchar,
   industry varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_silver.companies_flat_view definition

CREATE TABLE hive.cx_cso_silver.companies_flat_view (
   id bigint,
   name varchar,
   description varchar,
   domains_list varchar,
   created_at timestamp(3),
   updated_at timestamp(3),
   alias varchar,
   tax_code varchar,
   company_segment varchar,
   company_type varchar,
   health_score varchar,
   account_tier varchar,
   industry varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_silver.company_domains_exploded definition

CREATE TABLE hive.cx_cso_silver.company_domains_exploded (
   company_id bigint,
   company_name varchar,
   domain_name varchar,
   updated_at timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_silver.comparison_company_mapping definition

CREATE TABLE hive.cx_cso_silver.comparison_company_mapping (
   contact_id varchar,
   contact_name varchar,
   email varchar,
   email_domain varchar,
   current_company_id varchar,
   current_company_name varchar,
   suggested_company_id bigint,
   suggested_company_name varchar,
   mapping_status varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_silver.cx_cso_support_tickets definition

CREATE TABLE hive.cx_cso_silver.cx_cso_support_tickets (
   ticket_id varchar,
   subject varchar,
   customer_group varchar,
   status_name varchar,
   priority_level varchar,
   created_at timestamp(3),
   updated_at timestamp(3),
   first_responded_at timestamp(3),
   resolved_at timestamp(3),
   closed_at timestamp(3),
   call_reminder varchar,
   issue_category varchar,
   is_vcs boolean,
   l1 boolean,
   l2 boolean,
   l3 boolean,
   l4 boolean,
   is_duplicated_ticket boolean,
   segment varchar,
   market_segment varchar,
   is_reopened_by_cx boolean,
   assigned_to varchar,
   action_program varchar,
   company_alias varchar,
   customer_satisfaction_rating bigint,
   customer_entry_channel varchar,
   customer_respond_date timestamp(3),
   communication_effectiveness varchar,
   imported_ticket_date timestamp(3),
   imported_ticket_due_date timestamp(3),
   incident_root_cause varchar,
   issues_type varchar,
   note varchar,
   number_of_due_date_changes bigint,
   old_due_date timestamp(3),
   is_one_day_before_due boolean,
   is_one_hour_before_due boolean,
   reason varchar,
   related_ticket varchar,
   is_reminder_update boolean,
   is_notification boolean,
   is_overdue boolean,
   response_time_minutes double,
   sentiment varchar,
   severity_level varchar,
   spam_type varchar,
   support_category varchar,
   is_third_four_time boolean,
   ttr_overdue varchar,
   urgency_level varchar,
   resolution_time_minutes double,
   first_response_time_minutes double,
   l1_time_actual_minutes double,
   l1_time_allowed_minutes double,
   l1_violated boolean,
   l2_time_actual_minutes double,
   l2_time_allowed_minutes double,
   l2_violated boolean,
   l3_time_actual_minutes double,
   l3_time_allowed_minutes double,
   l3_violated boolean,
   l4_time_actual_minutes double,
   l4_time_allowed_minutes double,
   l4_violated boolean,
   time_to_response_minutes double,
   assigned_agent_stage varchar,
   violated_level integer,
   requester_name varchar,
   company_name varchar,
   customer_segment_l1 varchar,
   customer_segment_l2 varchar,
   customer_segment_l3 varchar,
   agent_name varchar,
   resolution_time_in_business_hours double,
   agent_reply_count integer,
   first_response_date timestamp(3),
   due_date_change_count bigint,
   tickets_first_responded_within_sla varchar,
   tickets_resolved_within_sla varchar,
   ttr_time_minutes double
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_silver.ticket_tags definition

CREATE TABLE hive.cx_cso_silver.ticket_tags (
   ticket_id bigint,
   tag varchar,
   created_at varchar,
   updated_at varchar
)
WITH (
   format = 'PARQUET'
);