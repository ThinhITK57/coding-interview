CREATE SCHEMA hive.cx_survicate_bronze 
with (location='s3a://vcs-raw/cx-survicate-bronze/');


-- hive.cx_survicate_bronze.audit_question_types definition

CREATE TABLE hive.cx_survicate_bronze.audit_question_types (
   question_type varchar,
   total_questions bigint,
   distinct_question_ids bigint,
   distinct_surveys bigint,
   first_response_dt date,
   last_response_dt date
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_survicate_bronze.audit_question_types_by_survey definition

CREATE TABLE hive.cx_survicate_bronze.audit_question_types_by_survey (
   survey_id varchar,
   question_type varchar,
   total bigint,
   first_response_date date,
   last_response_date date
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_survicate_bronze.question_answer_choices definition

CREATE TABLE hive.cx_survicate_bronze.question_answer_choices (
   question_id varchar,
   survey_id varchar,
   question_type varchar,
   question_clean varchar,
   answer_choice_id varchar,
   answer_choice_content varchar,
   kpi_type varchar,
   journey varchar,
   customer_touchpoint varchar,
   product_category varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_survicate_bronze.questions_list definition

CREATE TABLE hive.cx_survicate_bronze.questions_list (
   survey_id varchar,
   id varchar,
   type varchar,
   question_clean varchar,
   introduction_clean varchar,
   answer_choices array(ROW(id bigint, content varchar))
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_survicate_bronze.responses_envelope definition

CREATE TABLE hive.cx_survicate_bronze.responses_envelope (
   survey_id varchar,
   uuid varchar,
   url varchar,
   device_type varchar,
   operating_system varchar,
   language varchar,
   platform varchar,
   cso_ticket_id varchar,
   cso_uid varchar,
   cso_email varchar,
   respondent ROW(uuid varchar, attributes array(varchar)),
   respondent_uuid varchar,
   answers array(ROW(question_id bigint, question_type varchar, action_performed boolean, disclaimer_accepted varchar, answers array(ROW(id bigint, content varchar, score varchar, comment varchar, translated_comment varchar, disclaimer_accepted boolean, insight varchar, rank bigint)), answer varchar, translated_answer varchar, insight varchar, ai_followups varchar, translated_ai_followups varchar, fields array(ROW(type varchar, content varchar)), comment varchar, translated_comment varchar)),
   collected_at date
)
WITH (
   format = 'PARQUET',
   partitioned_by = ARRAY['collected_at']
);


-- hive.cx_survicate_bronze.responses_envelope_parse definition

CREATE TABLE hive.cx_survicate_bronze.responses_envelope_parse (
   survey_id varchar,
   response_id varchar,
   respondent_uuid varchar,
   collected_date date,
   question_id bigint,
   question_type varchar,
   choice_id bigint,
   answer_string varchar,
   answer_number double,
   answer_tag varchar,
   comment varchar,
   choice_content varchar,
   item_content varchar,
   item_score varchar,
   position integer,
   raw_answer ROW(question_id bigint, question_type varchar, action_performed boolean, disclaimer_accepted varchar, answers array(ROW(id bigint, content varchar, score varchar, comment varchar, translated_comment varchar, disclaimer_accepted boolean, insight varchar, rank bigint)), answer varchar, translated_answer varchar, insight varchar, ai_followups varchar, translated_ai_followups varchar, fields array(ROW(type varchar, content varchar)), comment varchar, translated_comment varchar)
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_survicate_bronze.survey_details definition

CREATE TABLE hive.cx_survicate_bronze.survey_details (
   survey_detail_id varchar,
   survey_id varchar,
   type varchar,
   name varchar,
   enabled boolean,
   responses bigint,
   launch_start_at varchar,
   launch_end_at varchar,
   launch_responses_limit varchar,
   author_name varchar,
   author_email varchar,
   folder varchar,
   first_response_at varchar,
   last_response_at varchar,
   attribute varchar,
   created_at_ts bigint,
   first_response_at_ts bigint,
   last_response_at_ts bigint
)
WITH (
   format = 'PARQUET'
);