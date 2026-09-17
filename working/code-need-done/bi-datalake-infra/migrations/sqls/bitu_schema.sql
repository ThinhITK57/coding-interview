CREATE SCHEMA hive.bitu_schema 
with (location='s3a://vcs-silver/bitu-schema/');

-- hive.bitu_schema.column_business_convention definition

CREATE TABLE hive.bitu_schema.column_business_convention (
   db_name varchar,
   table_name varchar,
   field_name varchar,
   data_type varchar,
   description_en varchar,
   description_vi varchar,
   pkey varchar,
   fkey double,
   nullable boolean
)
WITH (
   format = 'PARQUET'
);


-- hive.bitu_schema.column_schema definition

CREATE TABLE hive.bitu_schema.column_schema (
   schema varchar,
   "table" varchar,
   col varchar,
   data_type varchar,
   nullable varchar,
   pkey varchar,
   fkey varchar,
   description varchar,
   keep varchar,
   enum varchar
)
WITH (
   csv_escape = '\',
   csv_quote = '"',
   csv_separator = ',',
   format = 'CSV',
   skip_header_line_count = 1
);


-- hive.bitu_schema.column_schema_v2 definition

CREATE TABLE hive.bitu_schema.column_schema_v2 (
   layer varchar,
   "table" varchar,
   col varchar,
   data_type varchar,
   nullable varchar,
   pkey varchar,
   fkey varchar,
   description varchar,
   keep varchar,
   keep_in_rag varchar,
   enum varchar
)
WITH (
   csv_escape = '\',
   csv_quote = '"',
   csv_separator = ',',
   format = 'CSV',
   skip_header_line_count = 1
);


-- hive.bitu_schema.feedback definition

CREATE TABLE hive.bitu_schema.feedback (
   created_at varchar,
   username varchar,
   intent varchar,
   details varchar
)
WITH (
   format = 'TEXTFILE',
   textfile_field_separator = '	',
   textfile_field_separator_escape = '\'
);


-- hive.bitu_schema.role_table_permissions definition

CREATE TABLE hive.bitu_schema.role_table_permissions (
   role_name varchar,
   table_name varchar
)
WITH (
   csv_escape = '\',
   csv_quote = '"',
   csv_separator = ',',
   format = 'CSV',
   skip_header_line_count = 1
);


-- hive.bitu_schema.schema_description definition

CREATE TABLE hive.bitu_schema.schema_description (
   schema varchar,
   description varchar
)
WITH (
   csv_escape = '\',
   csv_quote = '"',
   csv_separator = ',',
   format = 'CSV',
   skip_header_line_count = 1
);


-- hive.bitu_schema.table_business_convention definition

CREATE TABLE hive.bitu_schema.table_business_convention (
   db_name varchar,
   table_name varchar,
   description_en varchar,
   description_vi varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bitu_schema.table_schema definition

CREATE TABLE hive.bitu_schema.table_schema (
   layer varchar,
   schema varchar,
   "table" varchar,
   source varchar,
   description varchar,
   keep varchar
)
WITH (
   csv_escape = '\',
   csv_quote = '"',
   csv_separator = ',',
   format = 'CSV',
   skip_header_line_count = 1
);


-- hive.bitu_schema.table_schema_v2 definition

CREATE TABLE hive.bitu_schema.table_schema_v2 (
   layer varchar,
   schema varchar,
   "table" varchar,
   source varchar,
   description varchar,
   keep varchar
)
WITH (
   csv_escape = '\',
   csv_quote = '"',
   csv_separator = ',',
   format = 'CSV',
   skip_header_line_count = 1
);


-- hive.bitu_schema.trino_sql definition

CREATE TABLE hive.bitu_schema.trino_sql (
   question_number bigint,
   question varchar,
   timestamp varchar,
   answer varchar,
   code varchar,
   source varchar,
   tables varchar,
   text_for_embedding varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.bitu_schema.user_role definition

CREATE TABLE hive.bitu_schema.user_role (
   username varchar,
   role_name varchar
)
WITH (
   csv_escape = '\',
   csv_quote = '"',
   csv_separator = ',',
   format = 'CSV',
   skip_header_line_count = 1
);


-- hive.bitu_schema.user_table_permissions definition

CREATE TABLE hive.bitu_schema.user_table_permissions (
   username varchar,
   table_name varchar
)
WITH (
   csv_escape = '\',
   csv_quote = '"',
   csv_separator = ',',
   format = 'CSV',
   skip_header_line_count = 1
);