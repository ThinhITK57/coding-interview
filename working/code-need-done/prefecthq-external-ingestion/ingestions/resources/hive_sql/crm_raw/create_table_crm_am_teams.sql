CREATE TABLE hive.crm_raw.teams (
   partial BOOLEAN,
    id BIGINT,
    name VARCHAR,
    created_at VARCHAR,
    updated_at VARCHAR,
    team_user_ids ARRAY<BIGINT>,
    creator_id BIGINT,
    updater_id BIGINT,
    crawled_at_ts BIGINT
)
WITH (
   external_location = 's3a://vcs-raw/crm-raw/teams',
   format = 'PARQUET'
);