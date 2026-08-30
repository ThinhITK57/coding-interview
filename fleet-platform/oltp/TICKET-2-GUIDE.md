# Ticket 2 — OLTP Schema & Log-Based CDC Pipeline

> **Mục tiêu**: Tạo schema OLTP, nạp dữ liệu mẫu, đăng ký Debezium CDC connector,
> chạy data producer mô phỏng, verify CDC events trên Kafka.
>
> **Pre-requisites**: Ticket 1 hoàn thành (Kafka, Postgres, Debezium đã cài và chạy).

---

## Cấu trúc file Ticket 2

```
fleet-platform/
├── oltp/
│   ├── sql/
│   │   ├── 01_schema.sql           ← DDL: 7 bảng + indexes + triggers
│   │   ├── 02_seed_data.sql         ← Dữ liệu mẫu thực tế (Việt Nam)
│   │   └── 03_cdc_permissions.sql   ← GRANT + PUBLICATION cho Debezium
│   └── scripts/
│       ├── data_producer.py         ← Mô phỏng hoạt động OLTP liên tục
│       └── verify_cdc.py            ← Verify CDC events trên Kafka
├── infra/
│   └── debezium/
│       └── config/
│           └── fleet-cdc-connector.json  ← Connector config (đã có, đã update)
```

---

## Bước 1: Tạo schema (trên master)

```bash
ssh aiguystory@master

# Chạy DDL
psql -h master -U fleet_app -d fleet_oltp -f /path/to/fleet-platform/oltp/sql/01_schema.sql
# Password: fleet_app_2024

# Verify bảng đã tạo
psql -h master -U fleet_app -d fleet_oltp -c "\dt public.*"
```

**Kết quả mong đợi** — 7 bảng:
```
            List of relations
 Schema |       Name        | Type  |  Owner
--------+-------------------+-------+----------
 public | components        | table | fleet_app
 public | customers         | table | fleet_app
 public | heads             | table | fleet_app
 public | invoices          | table | fleet_app
 public | parts_inventory   | table | fleet_app
 public | work_order_items  | table | fleet_app
 public | work_orders       | table | fleet_app
```

Verify REPLICA IDENTITY FULL:
```bash
psql -h master -U fleet_app -d fleet_oltp -c \
  "SELECT relname, relreplident FROM pg_class
   WHERE relname IN ('customers','heads','parts_inventory','work_orders','invoices','components','work_order_items');"
```

Tất cả phải hiện `relreplident = f` (f = FULL).

---

## Bước 2: Nạp dữ liệu mẫu

```bash
psql -h master -U fleet_app -d fleet_oltp -f /path/to/fleet-platform/oltp/sql/02_seed_data.sql
```

**Kết quả mong đợi** (cuối output):
```
 table_name      | row_count
-----------------+-----------
 components      |        15
 customers       |         8
 heads           |        10
 invoices        |         8
 parts_inventory |        45
 work_orders     |        15
```

---

## Bước 3: Cấp quyền CDC cho Debezium

```bash
# Chạy bằng postgres superuser (không phải fleet_app)
sudo -u postgres psql -d fleet_oltp -f /path/to/fleet-platform/oltp/sql/03_cdc_permissions.sql
```

---

## Bước 4: Tạo CDC topics trên Kafka (nếu chưa tạo)

Debezium sẽ tạo topic khi `auto.create.topics.enable=true`. Nhưng chúng ta đã **tắt**
auto-create → cần tạo thủ công cho 2 bảng mới:

```bash
ssh aiguystory@master

BOOTSTRAP="master:9092,slave1:9092,slave2:9092"

# Topics cho 2 bảng mới (components, work_order_items)
for table in components work_order_items; do
  /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $BOOTSTRAP \
    --topic "fleet-cdc.public.${table}" \
    --partitions 3 --replication-factor 3 \
    --config cleanup.policy=compact \
    --config min.insync.replicas=2 \
    --config min.compaction.lag.ms=3600000
  echo "✓ fleet-cdc.public.${table}"
done

# Verify tất cả 7 CDC topics + 2 app topics + 3 internal = 12 topics
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP
```

---

## Bước 5: Đăng ký Debezium CDC Connector

```bash
# Debezium Connect worker phải đang chạy trên port 8083
curl -s http://master:8083/ | python3 -m json.tool
# Phải thấy: {"version":"...", "commit":"...", ...}

# Đăng ký connector
curl -X POST http://master:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/path/to/fleet-platform/infra/debezium/config/fleet-cdc-connector.json

# Kiểm tra trạng thái
curl -s http://master:8083/connectors/fleet-cdc-connector/status | python3 -m json.tool
```

**Kết quả mong đợi**:
```json
{
  "name": "fleet-cdc-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "master:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "master:8083"
    }
  ],
  "type": "source"
}
```

> **Nếu task state = FAILED**: xem log
> ```bash
> curl -s http://master:8083/connectors/fleet-cdc-connector/tasks/0/status | python3 -m json.tool
> ```
> Lỗi phổ biến: quên chạy `03_cdc_permissions.sql` hoặc `wal_level` chưa là `logical`.

---

## Bước 6: Verify initial snapshot trên Kafka

Sau khi connector chạy, Debezium sẽ đọc **toàn bộ dữ liệu hiện có** (snapshot) và publish
lên Kafka. Kiểm tra:

```bash
# Đọc 5 messages đầu tiên từ topic heads
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server master:9092,slave1:9092,slave2:9092 \
  --topic fleet-cdc.public.heads \
  --from-beginning \
  --max-messages 5 | python3 -m json.tool
```

Phải thấy CDC event với `"op": "r"` (r = read/snapshot):
```json
{
  "payload": {
    "op": "r",
    "before": null,
    "after": {
      "id": 1,
      "name": "Trạm Bình Tân",
      "latitude": 10.7513900,
      ...
    }
  }
}
```

---

## Bước 7: Chạy Data Producer (mô phỏng nghiệp vụ)

```bash
ssh aiguystory@master

# Cài dependency
pip install psycopg2-binary

# Chạy 20 rounds, mỗi round cách 3 giây
python3 /path/to/fleet-platform/oltp/scripts/data_producer.py --rounds 20 --interval 3
```

Output sẽ hiện từng action:
```
--- Round 1/20 [21:45:03] ---
  [CREATE] Work Order #16: 51C-999.01 → Head #4 | Phanh kêu — kiểm tra và thay má phanh...
  [UPDATE] Inventory: Head #1, Component #7 → qty=28 (-2)
  [SCD] Customer #4 (Chủ xe Nguyễn Văn Tám): mới → cũ

--- Round 2/20 [21:45:06] ---
  [UPDATE] Work Order #11: scheduled → in_progress
  [PAYMENT] Invoice #6 (HD-2024-000006): paid — 3,091,000đ
  ...
```

---

## Bước 8: Verify CDC Events (end-to-end)

Trong terminal khác (trong khi data producer đang chạy):

```bash
# Cài dependencies
pip install kafka-python requests

# Chạy verify
python3 /path/to/fleet-platform/oltp/scripts/verify_cdc.py --timeout 30
```

**Kết quả mong đợi**:
```
[1/4] Debezium Connector Status
  ✓ Connector state: RUNNING
  ✓ Task 0 state: RUNNING

[2/4] CDC Topics on Kafka
  ✓ Topic exists: fleet-cdc.public.heads
  ✓ Topic exists: fleet-cdc.public.customers
  ...

[3/4] CDC Event Format (consuming for 30s)
  ✓ Total CDC events received: 127
  ✓ REPLICA IDENTITY FULL — before image has 11 fields (topic: fleet-cdc.public.customers)
  ✓ After image has 11 fields

[4/4] Event Count Summary
  Topic                                   Total  INSERT  UPDATE  DELETE
  ----------------------------------------------------------------------
  fleet-cdc.public.heads                     10      10       0       0
  fleet-cdc.public.customers                 12       8       4       0
  fleet-cdc.public.parts_inventory           53      45       8       0
  fleet-cdc.public.work_orders               20      15       5       0
  fleet-cdc.public.invoices                  12       8       4       0
  ...

Results: 9 passed, 0 failed
✅ CDC pipeline is working correctly.
```

---

## Troubleshooting

| Vấn đề | Nguyên nhân | Cách sửa |
|---|---|---|
| Connector FAILED | `wal_level` chưa `logical` | `sudo nano /etc/postgresql/15/main/postgresql.conf` → set `wal_level = logical` → restart |
| Connector FAILED | Thiếu quyền CDC | Chạy lại `03_cdc_permissions.sql` |
| Topic trống | `auto.create.topics.enable=false` nhưng chưa tạo topic | Bước 4 |
| before image chỉ có PK | Chưa set REPLICA IDENTITY FULL | Chạy lại phần REPLICA IDENTITY trong `01_schema.sql` |
| `psycopg2` import error | Chưa cài | `pip install psycopg2-binary` |
| `kafka-python` import error | Chưa cài | `pip install kafka-python` |
