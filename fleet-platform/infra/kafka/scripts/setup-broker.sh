#!/bin/bash
# =============================================================================
# setup-broker.sh — Generate server.properties cho từng node từ template
# Chạy trên: master (sẽ scp config sang slave1, slave2)
# Usage: bash setup-broker.sh
# =============================================================================

set -euo pipefail

KAFKA_HOME="/opt/kafka"
TEMPLATE="$KAFKA_HOME/config/server.properties.template"
NODES=("master" "slave1" "slave2")
USER="aiguystory"

echo "======================================"
echo " Kafka Multi-Broker — Config Generator"
echo "======================================"

# Copy template trước
cp $KAFKA_HOME/config/server.properties $TEMPLATE

for i in "${!NODES[@]}"; do
  NODE="${NODES[$i]}"
  BROKER_ID=$i
  CONFIG="/tmp/server-${NODE}.properties"

  echo ""
  echo "[Broker $BROKER_ID] Generating config for $NODE..."

  sed -e "s/__BROKER_ID__/$BROKER_ID/g" \
      -e "s/__HOSTNAME__/$NODE/g" \
      $TEMPLATE > $CONFIG

  if [ "$NODE" == "$(hostname)" ]; then
    echo "  → Local node, copying to $KAFKA_HOME/config/server.properties"
    cp $CONFIG $KAFKA_HOME/config/server.properties
  else
    echo "  → Remote node, scp to $NODE"
    scp $CONFIG ${USER}@${NODE}:$KAFKA_HOME/config/server.properties
  fi

  rm $CONFIG
  echo "  ✓ Broker $BROKER_ID ($NODE) configured"
done

# Tạo thư mục kafka-logs trên mỗi node
echo ""
echo "Creating kafka-logs directories..."
for NODE in "${NODES[@]}"; do
  if [ "$NODE" == "$(hostname)" ]; then
    mkdir -p $KAFKA_HOME/kafka-logs
  else
    ssh ${USER}@${NODE} "mkdir -p $KAFKA_HOME/kafka-logs"
  fi
  echo "  ✓ $NODE: $KAFKA_HOME/kafka-logs"
done

echo ""
echo "======================================"
echo " Done. Start Kafka on each node:"
echo "   master:  $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties"
echo "   slave1:  ssh slave1 '$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties'"
echo "   slave2:  ssh slave2 '$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties'"
echo "======================================"
