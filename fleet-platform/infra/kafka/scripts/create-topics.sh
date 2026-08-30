#!/bin/bash
# =============================================================================
# create-topics.sh — Tạo tất cả Kafka topics cho Fleet Platform
# Multi-broker cluster: replication-factor=3, min.insync.replicas=2
# Chạy trên: master (sau khi cả 3 brokers đã start)
# Usage: bash create-topics.sh
# =============================================================================

set -euo pipefail

KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP="master:9092,slave1:9092,slave2:9092"
RF=3    # replication-factor = số brokers
ISR=2   # min.insync.replicas = RF - 1 (chịu 1 broker down)

echo "======================================"
echo " Fleet Platform — Kafka Topic Setup"
echo " Cluster: 3 brokers, RF=$RF, ISR=$ISR"
echo "======================================"

# --- Kiểm tra đủ 3 brokers online ---
echo ""
echo "[Pre-check] Verifying broker count..."
BROKER_COUNT=$($KAFKA_BIN/kafka-metadata.sh --snapshot /opt/kafka/kafka-logs/__cluster_metadata-0/00000000000000000000.log --broker-count 2>/dev/null || \
  $KAFKA_BIN/kafka-broker-api-versions.sh --bootstrap-server $BOOTSTRAP 2>/dev/null | grep -c "^master\|^slave1\|^slave2" || echo "0")

if [ "$BROKER_COUNT" -lt 3 ] 2>/dev/null; then
  echo "  ⚠ Warning: Expected 3 brokers but detected fewer. Topics will still be created."
  echo "  Make sure all brokers are running before producing data."
fi

# --- Topics từ App (mock producers) ---
echo ""
echo "[1/3] Creating App topics (RF=$RF, ISR=$ISR)..."

$KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic truck-telemetry \
  --partitions 3 \
  --replication-factor $RF \
  --config retention.ms=604800000 \
  --config min.insync.replicas=$ISR

$KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic repair-request \
  --partitions 3 \
  --replication-factor $RF \
  --config retention.ms=604800000 \
  --config min.insync.replicas=$ISR

echo "  ✓ truck-telemetry (3 partitions, RF=$RF)"
echo "  ✓ repair-request  (3 partitions, RF=$RF)"

# --- CDC topics (cleanup.policy=compact) ---
echo ""
echo "[2/3] Creating CDC topics (compact, RF=$RF, ISR=$ISR)..."

CDC_TABLES=("heads" "parts_inventory" "customers" "work_orders" "invoices")

for table in "${CDC_TABLES[@]}"; do
  $KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $BOOTSTRAP \
    --topic "fleet-cdc.public.${table}" \
    --partitions 3 \
    --replication-factor $RF \
    --config cleanup.policy=compact \
    --config min.compaction.lag.ms=3600000 \
    --config min.insync.replicas=$ISR
  echo "  ✓ fleet-cdc.public.${table}"
done

# --- Debezium internal topics ---
echo ""
echo "[3/3] Creating Debezium internal topics (compact, RF=$RF)..."

$KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic fleet-cdc-connect-configs \
  --partitions 1 \
  --replication-factor $RF \
  --config cleanup.policy=compact

$KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic fleet-cdc-connect-offsets \
  --partitions 25 \
  --replication-factor $RF \
  --config cleanup.policy=compact

$KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server $BOOTSTRAP \
  --topic fleet-cdc-connect-status \
  --partitions 5 \
  --replication-factor $RF \
  --config cleanup.policy=compact

echo "  ✓ fleet-cdc-connect-configs"
echo "  ✓ fleet-cdc-connect-offsets"
echo "  ✓ fleet-cdc-connect-status"

# --- Liệt kê tất cả topics + verify replication ---
echo ""
echo "======================================"
echo " All topics created. Verifying replication:"
echo "======================================"
$KAFKA_BIN/kafka-topics.sh --describe --bootstrap-server $BOOTSTRAP \
  --topics-with-overrides 2>/dev/null || \
$KAFKA_BIN/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP

echo ""
echo "Done. Kafka multi-broker cluster is ready for Fleet Platform."
