#!/bin/bash

docker-compose -f docker-compose.ambari.yaml up -d
# docker-compose -f docker-compose.ambari.yaml up -d ambari-cx-survicate-crawler
# docker-compose -f docker-compose.ambari.yaml up -d ambari-freshworks-crawler
# docker-compose -f docker-compose.ambari.yaml up -d ambari-vcs-raw-backup-table
# docker-compose -f docker-compose.ambari.yaml up -d mixpanel-crawler-flow-ambari