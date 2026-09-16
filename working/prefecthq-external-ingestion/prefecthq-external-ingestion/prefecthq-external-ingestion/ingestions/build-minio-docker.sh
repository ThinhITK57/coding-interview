#!/bin/bash

docker-compose -f docker-compose.minio.yaml up -d
# docker-compose -f docker-compose.minio.yaml up -d minio-freshworks-crawler
# docker-compose -f docker-compose.minio.yaml up -d minio-vcs-raw-backup-table