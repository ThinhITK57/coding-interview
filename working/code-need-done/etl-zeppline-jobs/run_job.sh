#!/bin/bash

JOB_FILE=$1

if [ -z "$JOB_FILE" ]; then
  echo "Usage: sh submit.sh <job_file.py>"
  exit 1
fi

echo "Submitting job: $JOB_FILE"

spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 1 \
  --executor-cores 1 \
  --executor-memory 1G \
  --driver-memory 512M \
  --conf spark.executor.memoryOverhead=256 \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.pyspark.python=python3 \
  --principal zeppelin-datamining@SDM.VIETTELCYBER.COM \
  --keytab /u01/trino-server-479/etc/zeppelin.server.kerberos.keytab \
  main_submit.py $JOB_FILE