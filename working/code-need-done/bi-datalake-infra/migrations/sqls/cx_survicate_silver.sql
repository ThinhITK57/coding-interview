CREATE SCHEMA hive.cx_survicate_silver 
with (location='s3a://vcs-silver/cx-survicate-silver/');

-- hive.cx_survicate_silver.audit_new_question_types definition

CREATE TABLE hive.cx_survicate_silver.audit_new_question_types (
   question_type varchar,
   total_questions bigint,
   distinct_surveys bigint,
   first_response_dt date,
   last_response_dt date
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_survicate_silver.dim_question_types definition

CREATE TABLE hive.cx_survicate_silver.dim_question_types (
   question_type varchar,
   shape varchar,
   keep_in_fact_answers boolean
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_survicate_silver.fact_answers definition

CREATE TABLE hive.cx_survicate_silver.fact_answers (
   survey_id varchar,
   response_id varchar,
   respondent_uuid varchar,
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
   raw_answer ROW(question_id bigint, question_type varchar, action_performed boolean, disclaimer_accepted varchar, answers array(ROW(id bigint, content varchar, score varchar, comment varchar, translated_comment varchar, disclaimer_accepted boolean, insight varchar, rank bigint)), answer varchar, translated_answer varchar, insight varchar, ai_followups varchar, translated_ai_followups varchar, fields array(ROW(type varchar, content varchar)), comment varchar, translated_comment varchar),
   collected_date date
)
WITH (
   format = 'PARQUET',
   partitioned_by = ARRAY['collected_date']
);


-- hive.cx_survicate_silver.fact_events definition

CREATE TABLE hive.cx_survicate_silver.fact_events (
   survey_id varchar,
   response_id varchar,
   respondent_uuid varchar,
   question_id varchar,
   question_type varchar,
   action_performed boolean,
   raw_answer ROW(question_id bigint, question_type varchar, action_performed boolean, disclaimer_accepted varchar, answers array(ROW(id bigint, content varchar, score varchar, comment varchar, translated_comment varchar, disclaimer_accepted boolean, insight varchar, rank bigint)), answer varchar, translated_answer varchar, insight varchar, ai_followups varchar, translated_ai_followups varchar, fields array(ROW(type varchar, content varchar)), comment varchar, translated_comment varchar),
   collected_date date
)
WITH (
   format = 'PARQUET',
   partitioned_by = ARRAY['collected_date']
);


-- hive.cx_survicate_silver.question_answer_choices definition

CREATE TABLE hive.cx_survicate_silver.question_answer_choices (
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


-- hive.cx_survicate_silver.questions definition

CREATE TABLE hive.cx_survicate_silver.questions (
   survey_id varchar,
   id bigint,
   type varchar,
   question_clean varchar,
   introduction_clean varchar,
   answer_choices array(ROW(id bigint, content varchar))
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_survicate_silver.survey_details definition

CREATE TABLE hive.cx_survicate_silver.survey_details (
   survey_id varchar,
   survey_type varchar,
   survey_name varchar,
   folder varchar,
   created_at date,
   enabled boolean,
   journey varchar,
   customer_touchpoint varchar,
   product_category varchar
)
WITH (
   format = 'PARQUET'
);