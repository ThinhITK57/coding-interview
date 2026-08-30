#!/bin/bash
# =============================================================================
# verify-kafka.sh — Kiểm tra Kafka Multi-Broker Cluster
# 3 brokers: master (broker.id=0), slave1 (broker.id=1), slave2 (broker.id=2)
# Chạy trên: master
# Usage: bash verify-kafka.sh
# =============================================================================

set -euo pipefail

KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP="master:9092,slave1:9092,slave2:9092"
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

echo "======================================"
echo " Fleet Platform — Kafka Multi-Broker Health Check"
echo "======================================"
echo ""

# 1. Services
echo "[Services]"
check "Zookeeper (master)" "ssh master 'ss -tlnp | grep -q :2181'"
check "Kafka broker (master:9092)" "ssh master 'ss -tlnp | grep -q :9092'"
check "Kafka broker (slave1:9092)" "ssh slave1 'ss -tlnp | grep -q :9092'"
check "Kafka broker (slave2:9092)" "ssh slave2 'ss -tlnp | grep -q :9092'"

# 2. Broker metadata — tất cả 3 brokers phải register
echo ""
echo "[Cluster Metadata]"
BROKER_OUTPUT=$($KAFKA_BIN/kafka-broker-api-versions.sh --bootstrap-server $BOOTSTRAP 2>&1 || true)
check "Broker 0 (master) registered" "echo '$BROKER_OUTPUT' | grep -q 'master:9092'"
check "Broker 1 (slave1) registered" "echo '$BROKER_OUTPUT' | grep -q 'slave1:9092'"
check "Broker 2 (slave2) registered" "echo '$BROKER_OUTPUT' | grep -q 'slave2:9092'"

# 3. Topic replication
echo ""
echo "[Topic Replication]"
EXPECTED_TOPICS=(
  "truck-telemetry"
  "repair-request"
  "fleet-cdc.public.heads"
  "fleet-cdc.public.parts_inventory"
  "fleet-cdc.public.customers"
  "fleet-cdc.public.work_orders"
  "fleet-cdc.public.invoices"
)

for topic in "${EXPECTED_TOPICS[@]}"; do
  TOPIC_DESC=$($KAFKA_BIN/kafka-topics.sh --describe --bootstrap-server $BOOTSTRAP --topic "$topic" 2>/dev/null || echo "")
  if [ -z "$TOPIC_DESC" ]; then
    echo "  ✗ Topic MISSING: $topic"
    ((FAIL++))
  else
    RF=$(echo "$TOPIC_DESC" | grep "ReplicationFactor" | head -1 | grep -oP 'ReplicationFactor:\s*\K\d+' || echo "0")
    if [ "$RF" -ge 3 ] 2>/dev/null; then
      echo "  ✓ $topic (RF=$RF)"
      ((PASS++))
    else
      echo "  ⚠ $topic exists but RF=$RF (expected 3)"
      ((FAIL++))
    fi
  fi
done

# 4. ISR check — tất cả partition phải có ISR = 3 (khi healthy)
echo ""
echo "[In-Sync Replicas]"
UNDER_REPLICATED=$($KAFKA_BIN/kafka-topics.sh --describe --bootstrap-server $BOOTSTRAP \
  --under-replicated-partitions 2>/dev/null | wc -l)
if [ "$UNDER_REPLICATED" -eq 0 ]; then
  echo "  ✓ All partitions fully in-sync (ISR = RF)"
  ((PASS++))
else
  echo "  ✗ $UNDER_REPLICATED under-replicated partitions detected"
  ((FAIL++))
fi

# 5. Produce/Consume round-trip test (acks=all → cần ISR ghi thành công)
echo ""
echo "[Produce/Consume Test (acks=all)]"
TEST_MSG="fleet-multibroker-healthcheck-$(date +%s)"
echo "$TEST_MSG" | $KAFKA_BIN/kafka-console-producer.sh \
  --bootstrap-server $BOOTSTRAP \
  --topic truck-telemetry \
  --producer-property acks=all 2>/dev/null

RECEIVED=$($KAFKA_BIN/kafka-console-consumer.sh \
  --bootstrap-server $BOOTSTRAP \
  --topic truck-telemetry \
  --from-beginning \
  --timeout-ms 10000 2>/dev/null | tail -1)

if [ "$RECEIVED" = "$TEST_MSG" ]; then
  echo "  ✓ Produce (acks=all) → Consume round-trip OK"
  ((PASS++))
else
  echo "  ✗ Produce/Consume round-trip FAILED"
  ((FAIL++))
fi

# Summary
echo ""
echo "======================================"
echo " Results: $PASS passed, $FAIL failed"
echo "======================================"

if [ $FAIL -gt 0 ]; then
  echo " ⚠ Some checks failed. Review output above."
  exit 1
else
  echo " ✅ Kafka multi-broker cluster is healthy."
  exit 0
fi
