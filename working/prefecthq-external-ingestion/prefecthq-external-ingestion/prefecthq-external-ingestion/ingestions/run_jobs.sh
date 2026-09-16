#!/bin/bash

docker-compose up -d test_minio_flow
docker-compose up -d test_hdfs_flow
docker-compose up -d kpi_kpq_nocodb_flow