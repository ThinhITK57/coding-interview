#!/bin/bash

docker-compose -f docker-compose.ambari.yaml stop hdfs-jira-crawler
docker-compose -f docker-compose.ambari.yaml stop  hdfs-noc-metrics-crawler
# docker-compose -f docker-compose.ambari.yaml down   kpi_kpq_nocodb_flow