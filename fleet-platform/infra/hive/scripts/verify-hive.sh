#!/bin/bash
# =============================================================================
# verify-hive.sh — Kiểm tra sức khỏe của Apache Hive (Metastore & HiveServer2)
# Chạy trên: master
# Usage: bash verify-hive.sh
# =============================================================================

set -euo pipefail

PASS=0
FAIL=0

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

echo "=========================================================="
echo " Hive Metastore & HiveServer2 — Health Check"
echo " $(date)"
echo "=========================================================="

echo ""
echo "━━━ [1/4] PostgreSQL Metastore DB ━━━"
check "Database metastore_db exists" "sudo -u postgres psql -tAc \"SELECT 1 FROM pg_database WHERE datname='metastore_db';\" 2>/dev/null | grep -q 1"
check "User hive exists" "sudo -u postgres psql -tAc \"SELECT 1 FROM pg_roles WHERE rolname='hive';\" 2>/dev/null | grep -q 1"
check "Hive metastore schema initialized" "sudo -u postgres psql -d metastore_db -tAc \"SELECT 1 FROM \\\"VERSION\\\";\" 2>/dev/null | grep -q 1"

echo ""
echo "━━━ [2/4] HDFS Warehouse Permissions ━━━"
check "HDFS /user/hive/warehouse exists" "hdfs dfs -test -d /user/hive/warehouse"
check "HDFS /tmp/hive exists" "hdfs dfs -test -d /tmp/hive"

echo ""
echo "━━━ [3/4] Hive Daemons & Ports ━━━"
check "Hive Metastore port 9083 listening" "ss -tlnp | grep -q :9083"
check "HiveServer2 port 10000 listening" "ss -tlnp | grep -q :10000"

echo ""
echo "━━━ [4/4] Hive Query via Beeline ━━━"
check "Beeline test query SHOW DATABASES" "/usr/local/hive/bin/beeline -u 'jdbc:hive2://master:10000/default;auth=noSasl' -n hive -e 'SHOW DATABASES;' 2>/dev/null | grep -q default"

echo ""
echo "=========================================================="
echo " Results: $PASS passed, $FAIL failed"
echo "=========================================================="

if [ $FAIL -gt 0 ]; then
  echo " ❌ Some Hive checks FAILED."
  exit 1
else
  echo " ✅ Hive Metastore & HiveServer2 are healthy and operational."
  exit 0
fi
