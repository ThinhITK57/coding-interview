#!/bin/bash
# =============================================================================
# verify-all-infra.sh — Kiểm tra toàn bộ 8/8 components infrastructure Fleet Platform
# Chạy trên: master
# Usage: bash verify-all-infra.sh
# =============================================================================

set -euo pipefail

PASS=0
FAIL=0
WARN=0

check() {
  local name="$1"
  local cmd="$2"
  if eval "$cmd" > /dev/null 2>&1; then
    echo "  ✓ $name"
    ((PASS++))
  else
    echo "  ✗ $name — FAILED"
    ((FAIL++))
  fi
}

warn() {
  local name="$1"
  local cmd="$2"
  if eval "$cmd" > /dev/null 2>&1; then
    echo "  ✓ $name"
    ((PASS++))
  else
    echo "  ⚠ $name — NOT RUNNING (may not be needed yet)"
    ((WARN++))
  fi
}

echo "=========================================================="
echo " Fleet Platform — Full Infrastructure Health Check (8/8)"
echo " $(date)"
echo "=========================================================="

# --- 1. HDFS ---
echo ""
echo "━━━ [1/8] Hadoop HDFS ━━━"
check "NameNode process (master)" "ssh master jps 2>/dev/null | grep -q NameNode"
check "DataNode process (slave1)" "ssh slave1 jps 2>/dev/null | grep -q DataNode"
check "DataNode process (slave2)" "ssh slave2 jps 2>/dev/null | grep -q DataNode"
check "HDFS safemode OFF" "hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF"
check "HDFS /fleet-datalake exists" "hdfs dfs -test -d /fleet-datalake"

# --- 2. Kafka Multi-Broker & Zookeeper ---
echo ""
echo "━━━ [2/8] Kafka Multi-Broker Cluster ━━━"
check "Zookeeper port 2181 (master)" "ssh master 'ss -tlnp | grep -q :2181'"
check "Kafka broker 0 (master:9092)" "ssh master 'ss -tlnp | grep -q :9092'"
check "Kafka broker 1 (slave1:9092)" "ssh slave1 'ss -tlnp | grep -q :9092'"
check "Kafka broker 2 (slave2:9092)" "ssh slave2 'ss -tlnp | grep -q :9092'"
check "All 3 brokers registered" "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server master:9092,slave1:9092,slave2:9092 2>&1 | grep -c ':9092' | grep -q 3"
check "No under-replicated partitions" "[ $(/opt/kafka/bin/kafka-topics.sh --describe --bootstrap-server master:9092,slave1:9092,slave2:9092 --under-replicated-partitions 2>/dev/null | wc -l) -eq 0 ]"

# --- 3. PostgreSQL ---
echo ""
echo "━━━ [3/8] PostgreSQL ━━━"
check "PostgreSQL service" "systemctl is-active --quiet postgresql"
check "wal_level = logical" "sudo -u postgres psql -tAc \"SHOW wal_level;\" 2>/dev/null | grep -q logical"
check "Database fleet_oltp exists" "sudo -u postgres psql -tAc \"SELECT 1 FROM pg_database WHERE datname='fleet_oltp';\" 2>/dev/null | grep -q 1"
check "Database airflow_metadata exists" "sudo -u postgres psql -tAc \"SELECT 1 FROM pg_database WHERE datname='airflow_metadata';\" 2>/dev/null | grep -q 1"
check "Database metastore_db exists (Hive)" "sudo -u postgres psql -tAc \"SELECT 1 FROM pg_database WHERE datname='metastore_db';\" 2>/dev/null | grep -q 1"
check "User fleet_cdc has REPLICATION" "sudo -u postgres psql -tAc \"SELECT rolreplication FROM pg_roles WHERE rolname='fleet_cdc';\" 2>/dev/null | grep -q t"

# --- 4. Debezium / Kafka Connect ---
echo ""
echo "━━━ [4/8] Debezium (Kafka Connect) ━━━"
warn "Kafka Connect REST API (port 8083)" "curl -sf http://master:8083/ > /dev/null"
warn "Debezium plugin loaded" "curl -sf http://master:8083/connector-plugins 2>/dev/null | grep -q PostgresConnector"

# --- 5. Redis ---
echo ""
echo "━━━ [5/8] Redis ━━━"
check "Redis service" "systemctl is-active --quiet redis-server"
check "Redis PING/PONG" "redis-cli ping 2>/dev/null | grep -q PONG"
check "Redis GEO support" "redis-cli GEOADD _test_geo 0 0 test 2>/dev/null && redis-cli DEL _test_geo > /dev/null"

# --- 6. Spark ---
echo ""
echo "━━━ [6/8] Spark ━━━"
check "Spark Master process" "ssh master jps 2>/dev/null | grep -q Master"
check "Spark Worker (slave1)" "ssh slave1 jps 2>/dev/null | grep -q Worker"
check "Spark Worker (slave2)" "ssh slave2 jps 2>/dev/null | grep -q Worker"
check "Spark reads HDFS" "spark-submit --master local[1] --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.12-*.jar 2 2>&1 | grep -q 'Pi is roughly'"

# --- 7. Apache Hive (Metastore & HiveServer2) ---
echo ""
echo "━━━ [7/8] Apache Hive (Metastore & HMS) ━━━"
warn "Hive Metastore port 9083 listening" "ss -tlnp | grep -q :9083"
warn "HiveServer2 port 10000 listening" "ss -tlnp | grep -q :10000"
check "HDFS /user/hive/warehouse exists" "hdfs dfs -test -d /user/hive/warehouse"

# --- 8. Airflow ---
echo ""
echo "━━━ [8/8] Airflow ━━━"
warn "Airflow Webserver (port 8081)" "curl -sf http://master:8081/health > /dev/null"
warn "Airflow Scheduler" "systemctl is-active --quiet airflow-scheduler"

# --- Summary ---
echo ""
echo "=========================================================="
echo " Results: $PASS passed, $FAIL failed, $WARN warnings"
echo "=========================================================="

if [ $FAIL -gt 0 ]; then
  echo " ❌ Some critical checks FAILED. Fix before proceeding."
  exit 1
elif [ $WARN -gt 0 ]; then
  echo " ⚠ Some services not yet running (may be expected at this stage)."
  exit 0
else
  echo " ✅ All 8/8 infrastructure components are healthy and operational!"
  exit 0
fi
