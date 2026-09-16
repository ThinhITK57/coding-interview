#!/bin/bash

docker-compose -f docker-compose.ambari.yaml up  -d hdfs-jira-crawler
docker-compose -f docker-compose.ambari.yaml up  -d  hdfs-noc-metrics-crawler
docker-compose -f docker-compose.ambari.yaml up  -d  ambari-freshworks-crawler
docker-compose -f docker-compose.ambari.yaml up  -d  ambari-cx-survicate-crawler
docker-compose -f docker-compose.ambari.yaml up  -d  ambari-cx-cso-crawler
# docker-compose -f docker-compose.ambari.yaml up  -d  kpi_kpq_nocodb_flow