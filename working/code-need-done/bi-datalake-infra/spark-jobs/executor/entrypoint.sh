#!/bin/bash
set -e

SPARK_HOME=/opt/spark

# Add extra jars
export SPARK_CLASSPATH=$SPARK_HOME/jars/*
export PATH=$SPARK_HOME/bin:$PATH
export HADOOP_USER_NAME=admin

CMD=$1
shift || true

case "$CMD" in

  run)
    # Usage:
    # docker exec -it spark-client entrypoint.sh run jobs/a.py '{"date":"2025"}'

    START=$(date +%s)
    JOB_FILE=$1
    JOB_FILE=${JOB_FILE#/workspace/}
    JOB_FILE=/workspace/$JOB_FILE
    shift || true

    PARAMS=${1:-"{}"}
    shift || true
    EXTRA_CONF="$@"  # optional extra Spark configs

    echo "🚀 Running job: $JOB_FILE"
    echo "📦 Params: $PARAMS"
    echo "⚙️ Extra Spark conf: $EXTRA_CONF"

    RUNNER_TEMPLATE=/workspace/framework/runner.py

    # --- Spark default configs ---
    JARS=$(ls $SPARK_HOME/jars_extra/*.jar | tr '\n' ',' | sed 's/,$//')

    # JAR_LIST=$(echo /workspace/jars/*.jar | tr ' ' ',')
# --conf spark.driver.extraJavaOptions='-XX:+UseG1GC -XX:+ExplicitGCInvokesConcurrent -Xms128m -Xmx512m' 
# --conf spark.executor.extraJavaOptions='-XX:+UseG1GC -Xms128m -Xmx512m' 

    SPARK_CONF="
--conf spark.driver.extraClassPath=${JARS}
--conf spark.executor.extraClassPath=${JARS}
--conf spark.driver.memory=512m 
--conf spark.executor.memory=512m 
--conf spark.sql.shuffle.partitions=1 
--conf spark.default.parallelism=1 
--conf spark.sql.adaptive.enabled=true 
--conf spark.ui.enabled=false 
--conf spark.eventLog.enabled=false 
--conf spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT} 
--conf spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY} 
--conf spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY} 
--conf spark.hadoop.fs.s3a.path.style.access=true 
--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false 
--conf spark.sql.catalogImplementation=hive 
--conf hive.metastore.uris=${HIVE_ENDPOINT:-thrift://localhost:9083}
"
      # --jars "$JARS_EXTRA" \
      # --driver-class-path "/opt/spark/jars_extra/*" \

    # --- Chạy spark-submit trên file tạm ---
    spark-submit \
      --master local[3] \
      --conf "spark.executor.extraJavaOptions=-Dlog4j.rootCategory=WARN,console" \
      --conf "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=WARN,console" \
      $SPARK_CONF \
      $EXTRA_CONF \
      $RUNNER_TEMPLATE $JOB_FILE "$PARAMS"
      


    END=$(date +%s)
    echo "⏱️ Duration: $((END-START))s"
    ;;

  shell)
    echo "🧪 Opening pyspark shell..."
    pyspark
    ;;

  bash)
    echo "🐚 Opening bash..."
    /bin/bash
    ;;

  run_single)
    echo "🐚 Run Jobs"
    python3 /workspace/jobs/cron.py run_single
    ;;

  *)
    # echo "📌 Default: Cron Job mode"
    # python3 /workspace/jobs/cron.py
    echo "📌 Default: PrefectHQ Job mode"
    python3 /workspace/jobs/prefecthq.py
    ;;

esac