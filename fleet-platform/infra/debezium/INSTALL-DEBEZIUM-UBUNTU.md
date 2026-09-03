# Cài đặt Debezium Connect cho Fleet Platform

> **Vai trò**: Debezium chạy như 1 Kafka Connect worker, đọc WAL từ PostgreSQL
> và publish CDC events vào Kafka topics (`fleet-cdc.public.*`).
> Đây là component kết nối giữa Odoo OLTP và toàn bộ hạ nguồn (Redis sync + Spark batch).

---

## 1. Cài đặt Kafka Connect (đi kèm Kafka)

Kafka Connect đã có sẵn trong Kafka distribution (`/opt/kafka/`).
Chỉ cần tải **Debezium PostgreSQL Connector plugin**.

```bash
ssh aiguystory@master

# Tạo thư mục plugin cho Kafka Connect
mkdir -p /opt/kafka/connect-plugins

# Tải Debezium PostgreSQL connector
# Kiểm tra bản mới nhất: https://debezium.io/releases/
cd /tmp
wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.7.1.Final/debezium-connector-postgres-2.7.1.Final-plugin.tar.gz
tar -xzf debezium-connector-postgres-2.7.1.Final-plugin.tar.gz -C /opt/kafka/connect-plugins/
rm debezium-connector-postgres-2.7.1.Final-plugin.tar.gz

# Verify
ls /opt/kafka/connect-plugins/debezium-connector-postgres/
# Phải thấy các .jar files
```

---

## 2. Cấu hình Kafka Connect Distributed

File config: `fleet-platform/infra/debezium/config/connect-distributed.properties`

Copy sang master:
```bash
cp /path/to/connect-distributed.properties /opt/kafka/config/connect-distributed.properties
```

---

## 3. Tạo systemd service

```bash
sudo tee /etc/systemd/system/kafka-connect.service << 'EOF'
[Unit]
Description=Kafka Connect (Debezium)
Documentation=https://debezium.io
Requires=kafka.service
After=kafka.service

[Service]
Type=simple
User=aiguystory
Group=aiguystory
Environment="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"
ExecStart=/opt/kafka/bin/connect-distributed.sh /opt/kafka/config/connect-distributed.properties
ExecStop=/bin/kill -TERM $MAINPID
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl start kafka-connect
sudo systemctl enable kafka-connect
```

---

## 4. Verify Kafka Connect đang chạy

```bash
# Đợi ~30s cho Connect khởi động xong

# Check REST API
curl -s http://master:8083/ | python3 -m json.tool
# Phải thấy: version, commit, kafka_cluster_id

# Check connector plugins đã load
curl -s http://master:8083/connector-plugins | python3 -m json.tool
# Phải thấy: io.debezium.connector.postgresql.PostgresConnector
```

---

## 5. Đăng ký Debezium CDC Connector

> **LƯU Ý**: Bước này chạy SAU KHI đã tạo schema OLTP (Ticket 2).
> Nhưng file config chuẩn bị sẵn ở đây.

File JSON: `fleet-platform/infra/debezium/config/fleet-cdc-connector.json`

Đăng ký:
```bash
curl -X POST http://master:8083/connectors \
  -H "Content-Type: application/json" \
  -d @fleet-cdc-connector.json

# Verify connector status
curl -s http://master:8083/connectors/fleet-cdc-connector/status | python3 -m json.tool
# Phải thấy: "state": "RUNNING"
```
