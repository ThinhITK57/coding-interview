# metadata-agent

Cron-based container to run dbt build/upload flow for OpenMetadata artifacts.

## Included
- Docker image with `dbt-core`, `dbt-trino`, `boto3`, `openmetadata-ingestion[dbt]`
- Cron runner that executes your existing script:
  - `/workspace/dbt_projects/scripts/s3-build-upload-openmetadata-local.sh`
- Optional `git pull --ff-only` on branch `master` before each run
- Optional trigger for OpenMetadata DBT ingestion pipelines (by Database Service)
- Optional stateful run gate: run job only when a new remote commit exists
- Retry with exponential backoff for OpenMetadata timeout and HTTP 5xx

## Quick start
1. Create env file:
   - `cp metadata-agent/.env.example metadata-agent/.env`
2. Edit values in `metadata-agent/.env` (at least `ENV_FILE`, `DBT_DOMAINS`).
3. Start agent from `metadata-agent` folder:
   - `docker compose -f docker-compose.yml up -d --build`
4. Check logs:
   - `docker logs -f metadata-agent`
   - `tail -f metadata-agent/logs/cron.log`

## Useful env
- `CRON_SCHEDULE` default `*/30 * * * *`
- `RUN_ON_STARTUP` default `true`
- `DBT_DOMAINS` default `crm` (comma-separated)
- `DBT_RUN_ARGS` optional extra args, e.g. `--skip-tests`
- `ENV_FILE` default `/workspace/dbt_projects/.env.s3.local`
- `ENABLE_GIT_PULL` default `false`
- `GIT_BRANCH` default `master`
- `GIT_LAST_SHA_FILE` default `/var/lib/metadata-agent/last_master.sha`
- `ENABLE_OM_DBT_TRIGGER` default `false`
- `OM_JWT` required when trigger is enabled
- `OM_DATABASE_SERVICE_NAME` required when trigger is enabled
- `OM_DBT_TRIGGER_MODE` values: `all`, `first`, `name`
- `OM_DBT_PIPELINE_NAME` required when mode is `name`
- `OM_RETRY_MAX_ATTEMPTS` default `4`
- `OM_RETRY_INITIAL_DELAY_SECONDS` default `1`
- `OM_RETRY_MAX_DELAY_SECONDS` default `8`

## Behavior
- When `ENABLE_GIT_PULL=true`, agent fetches `origin/<GIT_BRANCH>` and compares with `GIT_LAST_SHA_FILE`.
- If SHA is unchanged, it skips dbt job and OpenMetadata trigger.
- If SHA is new, it runs dbt upload flow, then triggers OpenMetadata.
- `GIT_LAST_SHA_FILE` is updated only after all steps succeed.

## Compose files
- Only use: `metadata-agent/docker-compose.yml`
