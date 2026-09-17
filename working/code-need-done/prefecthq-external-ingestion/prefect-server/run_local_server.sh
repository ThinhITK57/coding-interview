#!/bin/bash

#192.168.6.158
export PREFECT_API_URL=http://127.0.0.1:4200/api
export PREFECT_API_DATABASE_CONNECTION_URL=sqlite+aiosqlite:///./prefect_data/prefect.db
export PREFECT_API_ANALYTICS_ENABLED=false
export PREFECT_SERVER_ANALYTICS_ENABLED=false

prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
# prefect config set PREFECT_API_ANALYTICS_ENABLED=false
prefect config set PREFECT_SERVER_ANALYTICS_ENABLED=false

prefect server start --host 0.0.0.0
