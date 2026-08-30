# Kafka Multi-Broker — Hướng dẫn Step-by-Step trên từng máy

> **Mục tiêu cuối cùng:**
> ```
> master  → Zookeeper + Kafka Broker 0
> slave1  → Kafka Broker 1
> slave2  → Kafka Broker 2
> ```
>
> Sau khi tải Kafka từ Apache về, bên trong thư mục Kafka đã có sẵn
> thư mục `config/` chứa các file mẫu. Bạn chỉ cần **sửa** những file
> đó cho đúng — không cần tạo thêm file hay thư mục config nào mới.

---

## Cấu trúc thư mục Kafka sau khi giải nén

Khi bạn tải `kafka_2.13-3.7.1.tgz` và giải nén vào `/opt/kafka/`, cấu trúc có sẵn là:

```
/opt/kafka/
├── bin/                          ← Các script khởi động/quản lý
│   ├── kafka-server-start.sh
│   ├── kafka-server-stop.sh
│   ├── zookeeper-server-start.sh
│   ├── zookeeper-server-stop.sh
│   ├── kafka-topics.sh
│   ├── kafka-console-producer.sh
│   ├── kafka-console-consumer.sh
│   └── ...
├── config/                       ← File cấu hình (bạn sẽ SỬA ở đây)
│   ├── server.properties         ← ★ Config Kafka broker (sửa trên MỖI máy)
│   ├── zookeeper.properties      ← ★ Config Zookeeper (chỉ sửa trên master)
│   ├── consumer.properties
│   ├── producer.properties
│   └── ...
├── libs/                         ← JAR files
└── logs/                         ← Log runtime (tự tạo khi chạy)
```

**Bạn không cần tạo thêm thư mục `config/` hay file mới.** Chỉ cần sửa 2 file có sẵn:
- `server.properties` — sửa trên **cả 3 máy** (mỗi máy khác nhau đôi chút)
- `zookeeper.properties` — sửa trên **master** thôi

---

# BƯỚC 1: Tải và cài Kafka trên cả 3 máy

## 1.1 Trên master

```bash
ssh aiguystory@master

sudo mkdir -p /opt/kafka
cd /opt
sudo wget https://downloads.apache.org/kafka/3.7.1/kafka_2.13-3.7.1.tgz
sudo tar -xzf kafka_2.13-3.7.1.tgz
sudo mv kafka_2.13-3.7.1/* /opt/kafka/
sudo rmdir kafka_2.13-3.7.1
sudo rm kafka_2.13-3.7.1.tgz
sudo chown -R aiguystory:aiguystory /opt/kafka

# Tạo thư mục data
mkdir -p /opt/kafka/kafka-logs
mkdir -p /opt/kafka/zookeeper-data
```

## 1.2 Trên slave1

```bash
ssh aiguystory@slave1

sudo mkdir -p /opt/kafka
cd /opt
sudo wget https://downloads.apache.org/kafka/3.7.1/kafka_2.13-3.7.1.tgz
sudo tar -xzf kafka_2.13-3.7.1.tgz
sudo mv kafka_2.13-3.7.1/* /opt/kafka/
sudo rmdir kafka_2.13-3.7.1
sudo rm kafka_2.13-3.7.1.tgz
sudo chown -R aiguystory:aiguystory /opt/kafka

mkdir -p /opt/kafka/kafka-logs
```

## 1.3 Trên slave2

```bash
ssh aiguystory@slave2

# (Giống hệt slave1)
sudo mkdir -p /opt/kafka
cd /opt
sudo wget https://downloads.apache.org/kafka/3.7.1/kafka_2.13-3.7.1.tgz
sudo tar -xzf kafka_2.13-3.7.1.tgz
sudo mv kafka_2.13-3.7.1/* /opt/kafka/
sudo rmdir kafka_2.13-3.7.1
sudo rm kafka_2.13-3.7.1.tgz
sudo chown -R aiguystory:aiguystory /opt/kafka

mkdir -p /opt/kafka/kafka-logs
```

### Verify trên cả 3 máy

```bash
# Chạy trên mỗi máy:
ls /opt/kafka/bin/kafka-server-start.sh
ls /opt/kafka/config/server.properties
# Cả 2 phải tồn tại
```

---

# BƯỚC 2: Cấu hình Zookeeper (CHỈ trên master)

```bash
ssh aiguystory@master
nano /opt/kafka/config/zookeeper.properties
```

**Xóa hết nội dung cũ**, paste nội dung sau:

```properties
# Thư mục lưu data Zookeeper
dataDir=/opt/kafka/zookeeper-data

# Port client kết nối (Kafka broker sẽ connect vào port này)
clientPort=2181

# Heartbeat interval (ms)
tickTime=2000

# Giới hạn kết nối từ 1 IP (0 = unlimited)
maxClientCnxns=0

# Tắt AdminServer (không cần cho cluster này)
admin.enableServer=false
```

Lưu và thoát (`Ctrl+O`, `Enter`, `Ctrl+X`).

---

# BƯỚC 3: Cấu hình Kafka Broker trên TỪNG máy

Mỗi máy đều sửa file `/opt/kafka/config/server.properties`.
**3 máy dùng cùng 1 nội dung, CHỈ KHÁC 2 DÒNG** được đánh dấu `★`.

## 3.1 Trên master

```bash
ssh aiguystory@master
nano /opt/kafka/config/server.properties
```

**Xóa hết nội dung cũ**, paste:

```properties
# ===================== THAY ĐỔI THEO NODE =====================
# ★ Mỗi broker cần ID khác nhau: master=0, slave1=1, slave2=2
broker.id=0

# ★ Hostname của chính máy này (client sẽ dùng hostname này để kết nối)
advertised.listeners=PLAINTEXT://master:9092

# ===================== GIỐNG NHAU TRÊN CẢ 3 MÁY =====================
listeners=PLAINTEXT://0.0.0.0:9092

num.network.threads=3
num.io.threads=8

socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Thư mục lưu message
log.dirs=/opt/kafka/kafka-logs

# Số partitions mặc định
num.partitions=3
num.recovery.threads.per.data.dir=1

# ===================== REPLICATION (PRODUCTION) =====================
# 3 brokers → mỗi partition có 3 bản copy
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
default.replication.factor=3

# Producer phải được ≥2 brokers xác nhận → chịu 1 broker down
min.insync.replicas=2
transaction.state.log.min.isr=2

# ===================== RETENTION =====================
# Giữ message 7 ngày
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

# ===================== ZOOKEEPER =====================
# Zookeeper chạy trên master
zookeeper.connect=master:2181
zookeeper.connection.timeout.ms=18000

# ===================== PRODUCTION SETTINGS =====================
# Delay 3s khi consumer group rebalance
group.initial.rebalance.delay.ms=3000

# Tắt auto-create topics (production: phải tạo thủ công)
auto.create.topics.enable=false

log.cleanup.policy=delete
```

## 3.2 Trên slave1

```bash
ssh aiguystory@slave1
nano /opt/kafka/config/server.properties
```

**Xóa hết nội dung cũ**, paste nội dung **GIỐNG HỆT master**, nhưng đổi 2 dòng có dấu ★:

```properties
# ★ ĐỔI: 0 → 1
broker.id=1

# ★ ĐỔI: master → slave1
advertised.listeners=PLAINTEXT://slave1:9092

# ===================== PHẦN CÒN LẠI GIỐNG HỆT MASTER =====================
listeners=PLAINTEXT://0.0.0.0:9092

num.network.threads=3
num.io.threads=8

socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

log.dirs=/opt/kafka/kafka-logs

num.partitions=3
num.recovery.threads.per.data.dir=1

offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
default.replication.factor=3

min.insync.replicas=2
transaction.state.log.min.isr=2

log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

zookeeper.connect=master:2181
zookeeper.connection.timeout.ms=18000

group.initial.rebalance.delay.ms=3000

auto.create.topics.enable=false

log.cleanup.policy=delete
```

## 3.3 Trên slave2

```bash
ssh aiguystory@slave2
nano /opt/kafka/config/server.properties
```

**Giống hệt slave1**, chỉ đổi 2 dòng ★:

```properties
# ★ ĐỔI: 1 → 2
broker.id=2

# ★ ĐỔI: slave1 → slave2
advertised.listeners=PLAINTEXT://slave2:9092

# ===================== PHẦN CÒN LẠI GIỐNG HỆT =====================
listeners=PLAINTEXT://0.0.0.0:9092

num.network.threads=3
num.io.threads=8

socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

log.dirs=/opt/kafka/kafka-logs

num.partitions=3
num.recovery.threads.per.data.dir=1

offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
default.replication.factor=3

min.insync.replicas=2
transaction.state.log.min.isr=2

log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

zookeeper.connect=master:2181
zookeeper.connection.timeout.ms=18000

group.initial.rebalance.delay.ms=3000

auto.create.topics.enable=false

log.cleanup.policy=delete
```

### Tóm tắt: chỉ 2 dòng khác nhau

| Máy | `broker.id` | `advertised.listeners` |
|---|---|---|
| master | `0` | `PLAINTEXT://master:9092` |
| slave1 | `1` | `PLAINTEXT://slave1:9092` |
| slave2 | `2` | `PLAINTEXT://slave2:9092` |

Tất cả dòng còn lại **copy y hệt**.

---

# BƯỚC 4: Tạo systemd services

## 4.1 Zookeeper service (CHỈ trên master)

```bash
ssh aiguystory@master
```

Trước tiên kiểm tra đường dẫn Java:
```bash
readlink -f $(which java) | sed 's|/bin/java||'
# Ghi nhớ kết quả, ví dụ: /usr/lib/jvm/java-11-openjdk-amd64
# Thay vào JAVA_HOME bên dưới nếu khác
```

```bash
sudo tee /etc/systemd/system/zookeeper.service << 'EOF'
[Unit]
Description=Apache Zookeeper
Requires=network.target
After=network.target

[Service]
Type=simple
User=aiguystory
Group=aiguystory
Environment="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"
ExecStart=/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
ExecStop=/opt/kafka/bin/zookeeper-server-stop.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
```

## 4.2 Kafka service (TRÊN CẢ 3 MÁY — nội dung giống nhau)

### Trên master:

```bash
ssh aiguystory@master

sudo tee /etc/systemd/system/kafka.service << 'EOF'
[Unit]
Description=Apache Kafka Broker
Requires=network.target
After=network.target zookeeper.service

[Service]
Type=simple
User=aiguystory
Group=aiguystory
Environment="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
```

### Trên slave1:

```bash
ssh aiguystory@slave1

sudo tee /etc/systemd/system/kafka.service << 'EOF'
[Unit]
Description=Apache Kafka Broker
Requires=network.target
After=network.target

[Service]
Type=simple
User=aiguystory
Group=aiguystory
Environment="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
```

### Trên slave2:

```bash
ssh aiguystory@slave2

# Giống hệt slave1:
sudo tee /etc/systemd/system/kafka.service << 'EOF'
[Unit]
Description=Apache Kafka Broker
Requires=network.target
After=network.target

[Service]
Type=simple
User=aiguystory
Group=aiguystory
Environment="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
```

> **Chú ý**: Trên master, Kafka service có `After=... zookeeper.service` vì Zookeeper
> cũng nằm trên master và phải khởi động trước. Trên slave1/slave2 không có Zookeeper
> nên không cần dòng đó.

---

# BƯỚC 5: Khởi động cluster (THỨ TỰ QUAN TRỌNG)

```
Zookeeper (master) → Kafka broker 0 (master) → Kafka broker 1 (slave1) → Kafka broker 2 (slave2)
```

```bash
# ---- Bước 5.1: Start Zookeeper trên master ----
ssh aiguystory@master
sudo systemctl start zookeeper
sudo systemctl status zookeeper
# Phải thấy: active (running)

# ---- Bước 5.2: Start Kafka trên master ----
sudo systemctl start kafka
sudo systemctl status kafka
# Phải thấy: active (running)

# ---- Bước 5.3: Start Kafka trên slave1 ----
ssh aiguystory@slave1
sudo systemctl start kafka
sudo systemctl status kafka

# ---- Bước 5.4: Start Kafka trên slave2 ----
ssh aiguystory@slave2
sudo systemctl start kafka
sudo systemctl status kafka

# ---- Bước 5.5: Enable auto-start khi reboot ----
ssh aiguystory@master "sudo systemctl enable zookeeper && sudo systemctl enable kafka"
ssh aiguystory@slave1 "sudo systemctl enable kafka"
ssh aiguystory@slave2 "sudo systemctl enable kafka"
```

---

# BƯỚC 6: Kiểm tra cluster hoạt động

Chạy tất cả lệnh dưới đây **trên master**:

```bash
ssh aiguystory@master
```

## 6.1 Kiểm tra 3 brokers đã register

```bash
/opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server master:9092,slave1:9092,slave2:9092 2>&1 | grep "^master\|^slave"
```

**Kết quả mong đợi** — phải thấy cả 3 dòng:
```
master:9092 (id: 0 rack: null) -> (...)
slave1:9092 (id: 1 rack: null) -> (...)
slave2:9092 (id: 2 rack: null) -> (...)
```

## 6.2 Tạo 1 topic test với replication-factor=3

```bash
/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server master:9092,slave1:9092,slave2:9092 \
  --topic test-multi-broker \
  --partitions 3 \
  --replication-factor 3
```

## 6.3 Kiểm tra topic vừa tạo

```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --bootstrap-server master:9092,slave1:9092,slave2:9092 \
  --topic test-multi-broker
```

**Kết quả mong đợi:**
```
Topic: test-multi-broker  PartitionCount: 3  ReplicationFactor: 3  ...
  Partition: 0  Leader: X  Replicas: 0,1,2  Isr: 0,1,2
  Partition: 1  Leader: Y  Replicas: 0,1,2  Isr: 0,1,2
  Partition: 2  Leader: Z  Replicas: 0,1,2  Isr: 0,1,2
```

Quan trọng: **Isr: 0,1,2** → cả 3 brokers đều in-sync.

## 6.4 Test produce & consume

```bash
# Terminal 1 trên master: Producer
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server master:9092,slave1:9092,slave2:9092 \
  --topic test-multi-broker \
  --producer-property acks=all
```

Gõ vài dòng text rồi `Ctrl+C`.

```bash
# Terminal 2 trên master: Consumer
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server master:9092,slave1:9092,slave2:9092 \
  --topic test-multi-broker \
  --from-beginning
```

Phải thấy tất cả text đã gõ.

## 6.5 Cleanup topic test

```bash
/opt/kafka/bin/kafka-topics.sh --delete \
  --bootstrap-server master:9092,slave1:9092,slave2:9092 \
  --topic test-multi-broker
```

---

# BƯỚC 7: Tạo topics cho dự án Fleet Platform

```bash
ssh aiguystory@master

BOOTSTRAP="master:9092,slave1:9092,slave2:9092"

# --- App topics ---
/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP \
  --topic truck-telemetry \
  --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000

/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP \
  --topic repair-request \
  --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000

# --- CDC topics (compact — giữ bản ghi mới nhất của mỗi key) ---
for table in heads parts_inventory customers work_orders invoices; do
  /opt/kafka/bin/kafka-topics.sh --create \
    --bootstrap-server $BOOTSTRAP \
    --topic "fleet-cdc.public.${table}" \
    --partitions 3 --replication-factor 3 \
    --config cleanup.policy=compact \
    --config min.insync.replicas=2 \
    --config min.compaction.lag.ms=3600000
done

# --- Debezium internal topics ---
/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP \
  --topic fleet-cdc-connect-configs \
  --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact

/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP \
  --topic fleet-cdc-connect-offsets \
  --partitions 25 --replication-factor 3 \
  --config cleanup.policy=compact

/opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP \
  --topic fleet-cdc-connect-status \
  --partitions 5 --replication-factor 3 \
  --config cleanup.policy=compact

# --- Liệt kê tất cả topics ---
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP
```

**Kết quả mong đợi** — 10 topics:
```
fleet-cdc-connect-configs
fleet-cdc-connect-offsets
fleet-cdc-connect-status
fleet-cdc.public.customers
fleet-cdc.public.heads
fleet-cdc.public.invoices
fleet-cdc.public.parts_inventory
fleet-cdc.public.work_orders
repair-request
truck-telemetry
```

---

# Tổng kết: File nào ở đâu trên mỗi máy

```
master:
  /opt/kafka/config/zookeeper.properties   ← Bước 2 (chỉ master có)
  /opt/kafka/config/server.properties      ← Bước 3.1 (broker.id=0)
  /opt/kafka/zookeeper-data/               ← Thư mục data Zookeeper
  /opt/kafka/kafka-logs/                   ← Thư mục data Kafka
  /etc/systemd/system/zookeeper.service    ← Bước 4.1
  /etc/systemd/system/kafka.service        ← Bước 4.2

slave1:
  /opt/kafka/config/server.properties      ← Bước 3.2 (broker.id=1)
  /opt/kafka/kafka-logs/                   ← Thư mục data Kafka
  /etc/systemd/system/kafka.service        ← Bước 4.2

slave2:
  /opt/kafka/config/server.properties      ← Bước 3.3 (broker.id=2)
  /opt/kafka/kafka-logs/                   ← Thư mục data Kafka
  /etc/systemd/system/kafka.service        ← Bước 4.2
```

> Slave1 và slave2 **KHÔNG CÓ** file `zookeeper.properties` và
> **KHÔNG CÓ** thư mục `zookeeper-data/` — vì Zookeeper chỉ chạy trên master.
