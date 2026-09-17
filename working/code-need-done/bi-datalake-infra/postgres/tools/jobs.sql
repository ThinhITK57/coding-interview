psql -U admin -d bootstrap

-- 1. Create user
CREATE USER etl_service_user WITH PASSWORD 'StrongPassword123!';

-- 2. Create database
CREATE DATABASE datalake_ops;

-- 3. Grant quyền connect
GRANT ALL PRIVILEGES ON DATABASE datalake_ops TO etl_service_user;

\c datalake_ops

CREATE SCHEMA job_control AUTHORIZATION etl_service_user;


CREATE TABLE job_control.job_runs (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(255),
    run_id VARCHAR(255),
    status VARCHAR(50),  -- RUNNING / SUCCESS / FAILED

    start_time TIMESTAMP,
    end_time TIMESTAMP,

    duration_seconds INT,

    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE job_control.task_runs (
    id BIGSERIAL PRIMARY KEY,
    job_run_id BIGINT REFERENCES job_control.job_runs(id),

    task_name VARCHAR(255),
    status VARCHAR(50),

    start_time TIMESTAMP,
    end_time TIMESTAMP,

    error_message TEXT
);

CREATE TABLE job_control.datasets (
    id BIGSERIAL PRIMARY KEY,
    dataset_name VARCHAR(255),
    layer VARCHAR(50), -- bronze/silver/gold

    path TEXT,
    format VARCHAR(50),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE job_control.job_logs (
    id BIGSERIAL PRIMARY KEY,
    job_run_id BIGINT,

    log_level VARCHAR(20),
    message TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

GRANT USAGE ON SCHEMA job_control TO etl_service_user;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA job_control TO etl_service_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA job_control GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_service_user;

CREATE INDEX idx_job_runs_job_name ON job_control.job_runs(job_name);
CREATE INDEX idx_job_runs_status ON job_control.job_runs(status);

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA job_control TO etl_service_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA job_control GRANT USAGE, SELECT ON SEQUENCES TO etl_service_user;


GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA job_control TO etl_service_user;
