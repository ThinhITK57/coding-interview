# P8 — ROLLS ROYCE PROJECT PRESENTATION
## Kịch bản luyện tập: Chứng minh tôi THẬT SỰ hiểu công nghệ, không phải học vẹt

> **Triết lý**: Interviewer senior không cần biết bạn *cài* Hadoop. Họ muốn biết: bạn *chọn* Hadoop vì gì, *cấu hình* gì, *khi nào sẽ bỏ* Hadoop, và *sự cố xảy ra* bạn xử lý thế nào.

---

## 🎯 DỰ ÁN FLAGSHIP: FLEET MAINTENANCE & REPAIR DATA PLATFORM

### BỐI CẢNH TÔI ĐÓNG GÓP VÀO DỰ ÁN

> **Script khai mở — 30 giây trước khi vào kỹ thuật:**
>
> *"Dự án này ban đầu chỉ có báo cáo Excel cuối tháng — Ban Giám đốc phải đợi 30 ngày để thấy doanh thu tháng trước. Phần em chịu trách nhiệm là thiết kế và triển khai toàn bộ data pipeline từ nguồn (Odoo/PostgreSQL) đến dashboard real-time, bao gồm việc đưa ra quyết định chọn từng technology trong stack và vận hành chúng. Kết quả: từ báo cáo T+30 ngày xuống còn T+15 phút, và 0% query load lên hệ thống vận hành."*

---

## MODULE 1: HADOOP/HDFS MULTI-NODE CLUSTER — CẤU HÌNH THẬT VÀ TƯ DUY VẬN HÀNH

### 1.1 Tại sao HDFS trong dự án này?

**Câu hỏi interviewer có thể hỏi (3 biến thể):**
- *"Tại sao em dùng Hadoop mà không dùng S3?"*
- *"Có công nghệ nào lưu trữ tương tự HDFS không?"*
- *"Em có biết HDFS có nhược điểm gì không?"*

**Đáp án "Rolls Royce" — không phải học vẹt:**

> *"Em chọn HDFS vì 2 lý do thực tế của dự án Fleet, không phải vì HDFS 'tốt' chung chung:*
>
> *Thứ nhất — **data locality**: Spark batch job của em đọc hàng chục GB Parquet mỗi đêm để tổng hợp DWH. Khi Spark chạy trên cùng cụm multi-node với HDFS, nó tận dụng data locality — đọc data từ chính DataNode đang chứa nó trên mạng LAN nội bộ, không phải truyền qua Internet. Với S3, mọi đọc đều là network I/O từ ngoài cloud về. Với cụm hạ tầng bare-metal / on-premise sẵn có của công ty — HDFS là lựa chọn tối ưu chi phí và kiểm soát băng thông tốt nhất.*
>
> *Thứ hai — **mature ecosystem với Spark & YARN**: Spark + HDFS + YARN là bộ ba production-proven hơn một thập kỷ. HDFS InputFormat, committer protocol, YARN resource scheduling, speculative execution — tất cả tích hợp sẵn, không phải debug xung đột driver storage.*
>
> *Công nghệ tương tự HDFS: **MinIO** (S3-compatible, on-prem), **Ceph** (distributed object store), **GCS/Azure ADLS** (cloud). Nếu dự án chuyển sang cloud, em sẽ migrate sang S3/GCS + Spark on EMR/Databricks — kiến trúc logic không đổi, chỉ đổi storage layer."*

---

### 1.2 Kiến trúc Cụm Multi-Node thực tế đã triển khai

**Cụm gồm 3 nodes độc lập (Ubuntu 22.04 LTS / OpenJDK 11 / Hadoop 3.3.6):**

```
                  ┌────────────────────────────────────────────────────────┐
                  │                 MASTER NODE (192.168.x.101)            │
                  │  • NameNode (Port 9870 Web UI, Port 9000 RPC)          │
                  │  • SecondaryNameNode (Port 9868)                       │
                  │  • YARN ResourceManager (Port 8088 Web UI, 8032 RPC)   │
                  └───────────────────────────┬────────────────────────────┘
                                              │
                     Passwordless SSH (1-chiều từ Master) + RPC LAN Network
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
     ┌─────────────────────────────────┐             ┌─────────────────────────────────┐
     │    SLAVE 1 (192.168.x.102)      │             │    SLAVE 2 (192.168.x.103)      │
     │  • DataNode (Port 9864 Web)     │             │  • DataNode (Port 9864 Web)     │
     │  • YARN NodeManager (Port 8042) │             │  • YARN NodeManager (Port 8042) │
     │  • Local Storage: /hdfs/datanode│             │  • Local Storage: /hdfs/datanode│
     └─────────────────────────────────┘             └─────────────────────────────────┘
```

---

### 1.3 📌 8 ĐIỂM CỐT TỬ CẦN "GHIM CHẶT TRONG ĐẦU" VỀ HADOOP MULTI-NODE

Khi interviewer hỏi: *"Em cài đặt multi-node thật chưa? Những vấn đề xương máu khi setup cụm phân tán là gì?"* — đây là 8 điểm chứng minh bạn đã trực tiếp cấu hình và fix bug thực tế:

#### 📌 Ghim 1: Bẫy Loopback `127.0.1.1` của Ubuntu trong `/etc/hosts` (Lỗi kinh điển)
- **Vấn đề**: Ubuntu mặc định tự sinh dòng `127.0.1.1 slave1` trong `/etc/hosts`.
- **Hậu quả**: Khi DataNode khởi động, nó bind IP vào `127.0.1.1` và thông báo địa chỉ này về NameNode. Khi Master hoặc Client muốn ghi block, NameNode bảo *"hãy kết nối tới 127.0.1.1"* → Client kết nối vào chính loopback của mình → **Lỗi `Connection refused` hoặc DataNode không thể giao tiếp với NameNode**.
- **Cách xử lý chuẩn**: Bắt buộc comment hoặc xóa dòng `127.0.1.1` trên tất cả các node. Chỉ map static IP thật:
  ```
  192.168.x.101   master
  192.168.x.102   slave1
  192.168.x.103   slave2
  ```

#### 📌 Ghim 2: Mô hình SSH không mật khẩu (Passwordless SSH) — Chỉ cần 1 chiều từ Master
- Master cần `ssh-keygen -t rsa` và dùng `ssh-copy-id` đẩy public key tới `master`, `slave1`, `slave2`.
- **Tại sao?** Các script khởi động tập trung (`start-dfs.sh`, `start-yarn.sh`) chạy từ Master, nó sẽ SSH ngầm vào từng slave liệt kê trong file `workers` để gọi lệnh khởi chạy daemon `datanode` và `nodemanager`.
- Slaves **không cần** SSH ngược lại Master. Tuy nhiên, toàn bộ các port RPC (9000, 8032, 9866, 8042...) phải thông suốt giữa các node (tắt ufw hoặc mở đúng port range).

#### 📌 Ghim 3: Quy tắc bất di bất dịch của `hdfs namenode -format`
- **CHỈ FORMAT 1 LẦN DUY NHẤT** khi dựng mới cụm.
- **Hiểu sâu nguyên lý**: Lệnh format tạo ra một `clusterID` ngẫu nhiên lưu trong thư mục NameNode metadata (`dfs.namenode.name.dir`).
- **Tai nạn hay gặp**: Nếu format lại NameNode khi cluster đã từng chạy, NameNode sinh `clusterID` mới, nhưng các DataNode trên `slave1`, `slave2` vẫn lưu `clusterID` cũ trong `dfs.datanode.data.dir` → DataNode start lên bị NameNode từ chối với lỗi **`java.io.IOException: InconsistentFSStateException`** → Cụm mất sạch worker.
- **Nếu bắt buộc format lại**: Phải xóa sạch dữ liệu trong `dfs.datanode.data.dir` và `hadoop.tmp.dir` trên **CẢ 3 MÁY** rồi mới format lại.

#### 📌 Ghim 4: Phải chỉnh `dfs.replication = 2` cho cụm 2 Slaves
- Mặc định Hadoop là `dfs.replication = 3`. Nếu cụm chỉ có 2 worker (`slave1`, `slave2`) mà để mặc định 3, NameNode sẽ liên tục báo trạng thái **`Under-replicated blocks`** trong Web UI (9870) vì nó không tìm được DataNode thứ 3 để đặt bản sao.
- Phải set `dfs.replication = 2` trong `hdfs-site.xml` để cluster ở trạng thái 100% Healthy.

#### 📌 Ghim 5: Bảo vệ đĩa OS với `dfs.datanode.du.reserved`
- Cấu hình `dfs.datanode.du.reserved = 2147483648` (2GB) trong `hdfs-site.xml`.
- **Ý nghĩa**: HDFS DataNode sẽ luôn chừa lại tối thiểu 2GB dung lượng trống trên ổ đĩa vật lý của slave cho hệ điều hành. Tránh tình trạng HDFS ghi tràn 100% disk làm treo OS hoặc crash các tiến trình hệ thống khác.

#### 📌 Ghim 6: YARN Log Aggregation (`yarn.log-aggregation-enable = true`) — Sống còn trong Multi-Node
- Trong môi trường phân tán, các Container của Spark/MapReduce chạy rải rác trên `slave1` và `slave2`.
- Nếu `log-aggregation` = `false`: Khi job lỗi, log nằm trên local disk của slave đã chạy container đó. Bạn không thể xem log từ Web UI ResourceManager (`http://master:8088`) mà phải SSH vào từng slave mò thư mục `/usercache/...`.
- Khi bật `yarn.log-aggregation-enable = true`: Ngay khi application kết thúc, NodeManager tự động gom toàn bộ container logs đẩy lên HDFS tại `/tmp/logs/<user>/logs/`. Nhờ đó, từ bất kỳ đâu chỉ cần gõ `yarn logs -applicationId <app_id>` hoặc click trực tiếp trên Web UI 8088 là đọc được log tức thì.

#### 📌 Ghim 7: 3 biến môi trường MapReduce bắt buộc trong `mapred-site.xml`
- Lỗi kinh điển khi chạy MapReduce/Spark on YARN trên Hadoop 3.x:
  `Could not find or load main class org.apache.hadoop.mapreduce.v2.app.MRAppMaster`
- **Nguyên nhân**: YARN Container không nhận được biến `HADOOP_MAPRED_HOME` từ môi trường host.
- **Fix triệt để**: Cấu hình 3 property trong `mapred-site.xml`:
  `yarn.app.mapreduce.am.env`, `mapreduce.map.env`, `mapreduce.reduce.env` cùng trỏ về `HADOOP_MAPRED_HOME=/usr/local/hadoop`.

#### 📌 Ghim 8: Cầu nối Spark ↔ HDFS Multi-Node (`spark-env.sh`)
- Để Spark submit job lên cụm YARN và đọc ghi HDFS `hdfs://master:9000`, cần export classpath trong `spark-env.sh`:
  ```bash
  export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
  export YARN_CONF_DIR=/usr/local/hadoop/etc/hadoop
  export SPARK_DIST_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
  ```

---

### 1.4 Bộ Cấu Hình Multi-Node Chuẩn Xác Từng Dòng

```xml
<!-- 1. core-site.xml (Cấu hình trên CẢ 3 MÁY) -->
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://master:9000</value>
    <!-- Trỏ về NameNode trên máy master cổng RPC 9000 -->
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/usr/local/hadoop/tmpdata</value>
    <!-- Tuyệt đối không để mặc định /tmp vì Linux OS sẽ xóa khi reboot -->
  </property>
</configuration>
```

```xml
<!-- 2. hdfs-site.xml (Cấu hình trên CẢ 3 MÁY) -->
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
    <!-- Cụm có 2 DataNodes (slave1, slave2) -> replication = 2 -->
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///usr/local/hadoop/hdfs/namenode</value>
    <!-- Thư mục lưu fsimage và edits log trên Master -->
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///usr/local/hadoop/hdfs/datanode</value>
    <!-- Thư mục lưu raw blocks trên Slaves -->
  </property>
  <property>
    <name>dfs.datanode.du.reserved</name>
    <value>2147483648</value>
    <!-- Dành riêng 2GB cho OS, chống ghi tràn ổ cứng -->
  </property>
</configuration>
```

```xml
<!-- 3. mapred-site.xml (Cấu hình trên CẢ 3 MÁY) -->
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
    <!-- Giao quyền quản lý tài nguyên tính toán cho YARN -->
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
  </property>
  <!-- 3 biến môi trường chống lỗi ClassNotFound MRAppMaster -->
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
  </property>
</configuration>
```

```xml
<!-- 4. yarn-site.xml (Cấu hình trên CẢ 3 MÁY) -->
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>master</value>
    <!-- Chỉ định Master quản lý tài nguyên tập trung -->
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
    <!-- Bật shuffle service cho các stage Map/Reduce/Spark -->
  </property>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>3072</value>
    <!-- RAM cấp cho YARN chạy Container trên mỗi Slave node -->
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>512</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>3072</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>2</value>
    <!-- 2 virtual cores cho mỗi worker -->
  </property>
  <property>
    <name>yarn.log-aggregation-enable</name>
    <value>true</value>
    <!-- Gom log container từ slaves về HDFS để debug tập trung -->
  </property>
  <property>
    <name>yarn.nodemanager.delete.debug-delay-sec</name>
    <value>600</value>
    <!-- Giữ lại thư mục container 10 phút sau khi chạy xong để debug nếu cần -->
  </property>
</configuration>
```

```ini
# 5. workers (Trên Master: $HADOOP_HOME/etc/hadoop/workers)
# Xóa localhost, khai báo danh sách slaves:
slave1
slave2
```

---

### 1.5 Quy Trình Vận Hành & Kiểm Tra Trạng Thái Cụm (Verification)

**1. Kiểm tra tiến trình phân tán bằng `jps`:**
- Trên **Master**: Phải thấy 3 tiến trình:
  - `NameNode`
  - `SecondaryNameNode`
  - `ResourceManager`
- Trên **Slave1 & Slave2** (`ssh slave1 jps`, `ssh slave2 jps`): Phải thấy 2 tiến trình:
  - `DataNode`
  - `NodeManager`

**2. Giám sát qua Web UI:**
- **HDFS NameNode UI** (`http://master:9870`):
  - Tab *Datanodes*: Số lượng **Live Nodes = 2** (`slave1`, `slave2`). Dead Nodes = 0.
  - DFS Used%: Dung lượng lưu trữ phân tán thực tế.
- **YARN ResourceManager UI** (`http://master:8088`):
  - Tab *Nodes of the cluster*: **Active Nodes = 2**.
  - VCores Total: 4 (2 nodes × 2 vcores).
  - Memory Total: 6144 MB (2 nodes × 3072 MB).

---

### 1.6 Sự cố thật em đã gặp với HDFS Multi-Node — "Rolls Royce" Storytelling

**Câu hỏi interviewer:**
- *"Bạn đã gặp sự cố gì khi vận hành cụm Hadoop multi-node chưa? Xử lý thế nào?"*

**Câu chuyện thật (STAR format):**

> *"Trong quá trình vận hành cụm multi-node cho pipeline Fleet, em đã trực tiếp xử lý 2 sự cố đặc thù của cụm phân tán:*
>
> ***Sự cố 1 — Mất kết nối DataNode do Ubuntu loopback resolution:***
> *Khi mới deploy slave2 vào cụm, lệnh `jps` trên slave2 báo DataNode đang chạy, nhưng NameNode Web UI (port 9870) chỉ hiển thị 1 Live Node (chỉ nhận slave1). Kiểm tra log `hadoop-*-datanode-slave2.log`, em thấy DataNode liên tục gửi heartbeat với IP `127.0.1.1` và bị Master từ chối. Em phát hiện file `/etc/hosts` trên slave2 bị Ubuntu tự chèn `127.0.1.1 slave2`. Em comment dòng này, restart DataNode, và ngay lập tức NameNode nhận đủ 2 Live Nodes.*
>
> ***Sự cố 2 — Small File Problem & NameNode Heap GC:***
> *Sau khi Spark Streaming chạy được 3 tuần, em nhận thấy NameNode Web UI báo heap usage tăng dần không ngừng.*
> *Nguyên nhân: Spark Streaming trigger mỗi 10 giây ghi trực tiếp ra HDFS → mỗi trigger tạo ra các file Parquet nhỏ phân tán trên cả 2 slaves → sau 3 tuần sinh ra hơn 150,000 files → NameNode phải giữ metadata của 150,000 files trong RAM (mỗi file/block tốn ~150 bytes heap) → JVM heap bắt đầu dính GC liên tục.*
> *Giải pháp em implement:*
> *1. Tăng trigger interval của Spark Streaming từ 10s lên 5 phút.*
> *2. Viết Airflow DAG chạy nightly compaction: đọc toàn bộ file nhỏ trong partition ngày hôm trước, dùng `coalesce(4)` để merge thành 4 file Parquet lớn (~128MB-256MB), ghi đè lại partition.*
> *3. Tăng heap size cho NameNode trong `hadoop-env.sh`: `export HADOOP_HEAPSIZE=4096`.*
> *Kết quả: Số lượng file giảm 98%, NameNode heap giải phóng về mức an toàn 40%, cluster vận hành trơn tru."*

---

## MODULE 2: DEBEZIUM CDC — CHỨng MINH HIỂU SÂU

### 2.1 Cấu hình Debezium em phải biết

**Câu hỏi interviewer (4 biến thể):**
- *"Debezium hoạt động như thế nào? Em cấu hình những gì?"*
- *"Nếu Debezium crash, dữ liệu có bị mất không?"*
- *"Replication slot là gì? Tại sao nguy hiểm nếu không monitor?"*
- *"Tại sao phải `REPLICA IDENTITY FULL`?"*

**Đáp án kỹ thuật — từng config có lý do:**

```json
// connector-fleet-cdc.json — Config thật với giải thích

{
  "name": "fleet-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

    // Kết nối PostgreSQL
    "database.hostname": "10.0.1.10",
    "database.port": "5432",
    "database.user": "debezium_user",
    "database.password": "...",
    "database.dbname": "odoo_production",
    "database.server.name": "fleet_odoo",

    // Plugin WAL — QUAN TRỌNG
    "plugin.name": "pgoutput",
    // pgoutput: built-in PostgreSQL 10+, không cần cài thêm
    // wal2json: cần cài extension riêng, đã deprecated
    // decoderbufs: cần cài, ít maintained

    // Replication slot — nguồn gốc bảo toàn dữ liệu
    "slot.name": "debezium_fleet_slot",
    // Slot này "ghim" WAL log — Postgres KHÔNG được xóa WAL
    // đến khi Debezium đã đọc xong
    // NGUY HIỂM: Nếu Debezium down lâu → WAL tích lũy → Disk đầy!

    // Bảng cần capture
    "table.include.list": "public.invoices,public.work_orders,public.customers,public.heads,public.parts_inventory",

    // Snapshot strategy — lần đầu kết nối
    "snapshot.mode": "initial",
    // initial: chụp toàn bộ table hiện tại, rồi mới stream changes
    // never: bỏ qua snapshot, chỉ stream từ thời điểm connector start
    // always: snapshot mỗi lần restart (nguy hiểm với bảng lớn!)

    // Offset lưu ở đâu
    "offset.storage": "org.apache.kafka.connect.storage.KafkaOffsetBackingStore",
    "offset.storage.topic": "__debezium_offsets",
    // Lưu offset trong Kafka → restart an toàn, resume từ đúng vị trí

    // Heartbeat — phát hiện connector "giả chết"
    "heartbeat.interval.ms": "30000",
    // Mỗi 30s gửi 1 heartbeat message
    // Tại sao cần? Nếu không có change event trong thời gian dài,
    // consumer lag không được update → monitoring giả báo "healthy"

    // Tombstone cho DELETE
    "tombstones.on.delete": "true",
    // Khi DELETE: gửi 2 message — change event (before image) + null (tombstone)
    // Tombstone cho phép Kafka compact topic xóa key đã delete

    // Transform — làm sạch message
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false"
  }
}
```

**Tại sao `REPLICA IDENTITY FULL` — câu hỏi hay bị hỏi:**

```sql
-- Mặc định PostgreSQL chỉ log PRIMARY KEY trong UPDATE event
-- Ví dụ: UPDATE customers SET status='old' WHERE id=1
-- Default REPLICA IDENTITY DEFAULT → WAL chỉ có: {id: 1, status: 'old'}
-- KHÔNG có before image → Debezium không biết status trước là gì → SCD2 không hoạt động!

ALTER TABLE customers REPLICA IDENTITY FULL;
-- Bây giờ WAL log TẤT CẢ cột trước và sau UPDATE:
-- before: {id:1, status:'new', company:'ABC'}
-- after:  {id:1, status:'old', company:'ABC'}
-- SCD2 merge mới có thể so sánh before/after để tạo version mới
```

**Câu hỏi vặn: "Replication slot nguy hiểm thế nào?"**

> *"Đây là một điểm em phải monitor rất chặt. Replication slot 'ghim' WAL log — PostgreSQL sẽ KHÔNG bao giờ xóa WAL cho đến khi Debezium confirm đã đọc xong.*
>
> *Scenario nguy hiểm: Debezium crash hoặc mất kết nối 24 giờ. Trong 24 giờ đó, Odoo tiếp tục ghi hàng nghìn transactions. PostgreSQL giữ tất cả WAL logs này lại → pg_wal directory phình ra → Disk đầy → PostgreSQL crash → Toàn bộ hệ thống OLTP down.*
>
> *Em monitor 2 metric này hàng ngày:*
>
> ```sql
> -- Xem lag của replication slot (bytes WAL chưa được Debezium đọc)
> SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
> FROM pg_replication_slots;
>
> -- Alert nếu lag > 5GB
> ```
>
> *Nếu lag > 5GB → alert ngay, không đợi đến khi disk đầy."*

---

## MODULE 3: KAFKA — CHỨNG MINH KHÔNG HỌC VẸT

### 3.1 Config Kafka thật sự trong Fleet

**Câu hỏi biến thể:**
- *"Kafka bạn cấu hình replication factor bao nhiêu? Tại sao?"*
- *"`acks=all` nghĩa là gì? Khi nào KHÔNG dùng acks=all?"*
- *"Consumer group là gì? Fleet có bao nhiêu consumer group?"*
- *"Kafka topic partition bạn chọn bao nhiêu? Công thức tính?"*

**Đáp án — config thật với lý do:**

```properties
# broker config — server.properties

# Replication
default.replication.factor=3
min.insync.replicas=2
# Nghĩa: 3 brokers giữ bản sao, nhưng chỉ cần 2 broker confirm = write thành công
# Tại sao không 3/3? Nếu 1 broker slow → write timeout. 2/3 là cân bằng đủ durability
# Tại sao không 1/3? Nếu leader crash trước khi 2 followers kịp replicate → data loss

# Retention — quan trọng cho replay
log.retention.hours=168      # 7 ngày
log.retention.bytes=-1       # Không giới hạn theo size (chỉ theo thời gian)
# 7 ngày vì: nếu Spark batch job cần backfill 7 ngày → đủ dữ liệu để replay

# Topic cdc-invoices: partition = 6
# Công thức: max(target_throughput / partition_throughput, số_consumer_max)
# Fleet CDC ~ 100 messages/s, mỗi partition handle ~10MB/s
# → 1-2 partitions đủ throughput, nhưng em dùng 6 để có thể scale consumer sau
```

```python
# Producer config — Debezium viết ra Kafka
producer.config = {
    "acks": "all",           # Chờ TẤT CẢ ISR replicas confirm
    "retries": 3,            # Retry 3 lần nếu lỗi transient
    "delivery.timeout.ms": 120000,  # 2 phút total timeout
    "compression.type": "lz4",      # LZ4: nén nhanh, tốt cho JSON CDC events
}
# Khi nào KHÔNG dùng acks=all? Analytics logging (metrics, click events)
# → chấp nhận mất 1 vài events, ưu tiên throughput cao
```

**Consumer groups trong Fleet:**

> *"Fleet có 3 consumer groups đọc từ cùng Kafka CDC topics:*
>
> *1. `group-id: spark-streaming-etl` — Spark Structured Streaming đọc để ingest vào HDFS (xử lý mỗi 5 phút)*
>
> *2. `group-id: redis-sync-service` — Service đồng bộ dữ liệu Head/tồn kho vào Redis (xử lý gần real-time)*
>
> *3. `group-id: airflow-sensor` — Airflow ExternalTaskSensor kiểm tra partition đã available chưa trước khi trigger batch job*
>
> *3 groups đọc cùng 1 topic nhưng maintain offset riêng — đây là lợi ích cốt lõi của Kafka vs RabbitMQ."*

---

## MODULE 4: SPARK — CHỨNG MINH HIỂU PRODUCTION

### 4.1 Cấu hình Spark thật sự

**Câu hỏi biến thể:**
- *"Em cấu hình executor memory bao nhiêu? Tại sao?"*
- *"Spark job lần đầu chạy chậm, em debug thế nào?"*
- *"AQE là gì? Em có bật không?"*
- *"Dynamic Allocation là gì? Khi nào bật/tắt?"*

**Config Spark với lý do:**

```python
# spark-submit config cho Fleet batch job

spark = SparkSession.builder \
    .appName("fleet-dwh-aggregation") \

    # Memory tuning — QUAN TRỌNG
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.memoryOverhead", "2g") \
    # memoryOverhead: RAM cho JVM overhead, off-heap, Python workers
    # Không cấu hình → OOM do JVM overhead, rất khó debug
    .config("spark.driver.memory", "4g") \

    # Parallelism
    .config("spark.executor.cores", "4") \
    # 4 cores/executor: balance giữa parallelism và GC pressure
    # 1 core/executor: ít parallelism, nhiều JVM overhead
    # 16 cores/executor: GC stop-the-world dài hơn

    # AQE — Adaptive Query Execution (Spark 3.0+)
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    # Auto gộp partition nhỏ sau shuffle — giảm small task overhead
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    # Auto phát hiện data skew trong JOIN, split partition skewed

    # Shuffle
    .config("spark.sql.shuffle.partitions", "auto") \
    # Với AQE enabled → để "auto", Spark tự tính số partition tối ưu
    # Không AQE: default 200, phải tune thủ công

    # Dynamic Allocation
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    # Tự scale lên khi có nhiều tasks, scale xuống khi idle
    # Quan trọng: phải bật External Shuffle Service cùng lúc
    .config("spark.shuffle.service.enabled", "true") \
    .getOrCreate()
```

**Khi nào TẮT Dynamic Allocation:**

> *"Streaming jobs: Spark Streaming cần số executor ổn định để maintain micro-batch SLA. Dynamic Allocation scale xuống giữa micro-batch → scale lên lại → latency spike. Fleet Streaming job em tắt Dynamic Allocation, fix cứng 4 executors."*

---

## MODULE 5: REDIS — CHỨNG MINH HIỂU 3 VAI TRÒ KHÁC NHAU

**Câu hỏi biến thể:**
- *"Redis trong dự án của bạn làm gì? Tại sao dùng Redis cho 3 việc khác nhau?"*
- *"GEOSEARCH là gì? Hoạt động như thế nào?"*
- *"Lua Script atomic nghĩa là gì?"*
- *"Redis có persistence không? Nếu Redis crash thì sao?"*

**Đáp án "Rolls Royce":**

```
Redis Fleet có 3 vai trò hoàn toàn khác nhau:

VAI TRÒ 1: Geo Spatial Search (tìm trạm gần nhất)
────────────────────────────────────────────────
# Lưu vị trí 50 trạm (Heads)
GEOADD fleet:heads:locations 106.6297 10.8231 "head:001"  # HCM
GEOADD fleet:heads:locations 108.2022 16.0544 "head:002"  # Da Nang

# Khi tài xế ở tọa độ (106.65, 10.80) cần tìm trạm trong 20km:
GEOSEARCH fleet:heads:locations FROMLONLAT 106.65 10.80
          BYRADIUS 20 km ASC COUNT 5
# Kết quả: ["head:001", "head:003"] — trả về trong < 1ms
# So với PostGIS: cần index, query 20-100ms (disk I/O)

Cơ chế: GEOSEARCH dùng Geohash encode tọa độ → lưu trong Sorted Set
→ Range query trên Sorted Set = O(log N + M)


VAI TRÒ 2: Atomic Slot Reservation (chống overbooking đặt lịch)
─────────────────────────────────────────────────────────────────
-- Lua Script đảm bảo CHECK + SET là atomic (single-threaded Redis)
local key = KEYS[1]  -- "head:001:slot:2026-08-13:09:00"
local truck_id = ARGV[1]
local ttl = tonumber(ARGV[2])  -- 300 giây (5 phút hold)

local current = redis.call('GET', key)
if current == false then
    -- Slot trống → đặt giữ chỗ với TTL
    redis.call('SET', key, truck_id, 'EX', ttl)
    return 1  -- Thành công
else
    return 0  -- Đã có người giữ
end

-- Tại sao Lua Script?
-- Giải pháp naive: GET rồi SET → 2 commands → race condition
-- 2 tài xế cùng GET thấy "trống" → cả 2 SET thành công → overbooking!
-- Lua Script: Redis chạy toàn bộ script trong 1 single thread → atomic


VAI TRÒ 3: Pub/Sub Notification + KV Store cho Dashboard
──────────────────────────────────────────────────────────
-- Airflow/Spark ghi kết quả tổng hợp vào Redis:
SET dashboard:revenue:weekly:2026-W32  '{"service": 450000000, "parts": 280000000}'
PUBLISH dashboard:updates "weekly:2026-W32"

-- Backend server subscribe:
SUBSCRIBE dashboard:updates
-- Khi nhận message → đọc data từ Redis → push xuống WebSocket clients
```

**Redis persistence — câu hỏi hay bị hỏi:**

> *"Em cấu hình cả RDB và AOF:*
>
> *RDB (Redis Database Backup): snapshot mỗi 1 giờ vào disk. Nếu Redis crash → mất tối đa 1 giờ data. Chấp nhận được cho vai trò serving layer vì data gốc vẫn còn trong HDFS + PostgreSQL, có thể re-sync.*
>
> *AOF (Append Only File): log mọi write operation. Nếu crash → replay AOF, mất tối đa vài giây. Em bật AOF cho vai trò slot reservation (tránh mất booking state khi restart).*
>
> *Quan trọng: Redis không phải source of truth. Nếu Redis chết và mất hết data → chạy lại Spark job để re-populate dashboard cache, chạy lại sync từ Kafka CDC để re-populate Head data. Redis là cache layer, không phải primary storage."*

---

## MODULE 6: KỊCH BẢN LUYỆN TẬP VỚI BIẾN THIÊN CÁCH HỎI

### Kịch bản A: Interviewer tấn công từ góc độ "Có lựa chọn tốt hơn không?"

```
INT: "Em dùng HDFS, nhưng bây giờ nhiều công ty chuyển sang Data Lakehouse 
      với Delta Lake/Iceberg trên S3. Em nghĩ sao?"

CAND LEVEL 1 (học vẹt): "Em cũng biết Delta Lake, nó có ACID..."
                        → FAIL: không connect với bài toán cụ thể

CAND LEVEL 2 (hiểu nguyên lý):
"Đúng — Delta Lake/Iceberg giải quyết vấn đề mà HDFS + Parquet thuần không có:
ACID transactions, schema evolution tự động, time travel query.
Nếu em làm lại từ đầu với cloud budget, em sẽ dùng Delta Lake trên S3.

Nhưng trong bối cảnh Fleet — bare-metal cluster, không có cloud budget,
và use case là append-only telemetry + batch aggregation (không cần ACID UPDATE) —
HDFS + Parquet là đủ và không cần pay overhead của Delta Lake transaction log.

Em chọn đơn giản nhất giải quyết được bài toán, không phải công nghệ 'hot' nhất."
```

### Kịch bản B: Interviewer kiểm tra "bạn có biết failure mode không?"

```
INT: "Nếu Kafka broker số 2 trong cluster 3 brokers crash lúc 2 giờ sáng,
     hệ thống Fleet sẽ bị ảnh hưởng gì?"

CAND phân tích từng layer:

"Với RF=3, min.insync.replicas=2 — đây là kịch bản cần phân tích từng phần:

Kafka producer (Debezium → Kafka):
→ Debezium tiếp tục ghi được. RF=3 → còn 2 broker ISR, đủ min.insync.replicas=2
→ Không bị ảnh hưởng. Write vẫn thành công.

Kafka consumer (Spark Streaming đọc từ Kafka):
→ Partition leader trên broker 2 sẽ failover sang broker 1 hoặc 3
→ Failover mất khoảng 10-30 giây (controller election)
→ Spark Streaming sẽ có 1-2 micro-batch trễ hơn bình thường, sau đó resume
→ Không mất data vì offset đã được commit.

Redis matching service:
→ Không bị ảnh hưởng — Redis không phụ thuộc Kafka

OLTP (Odoo):
→ Không bị ảnh hưởng — Odoo không biết Kafka có vấn đề

Dashboard:
→ Nếu crash xảy ra TRONG LÚC Airflow batch job đang ghi kết quả vào Kafka 
   để Spark consume: micro-batch đó có thể bị delay, dashboard refresh trễ
→ Sau khi Kafka tự recover (2-5 phút): mọi thứ hoạt động bình thường

Recovery hoàn toàn tự động. Em sẽ nhận alert từ Kafka monitoring (consumer lag spike)
và check lại Airflow DAG không bị stuck."
```

### Kịch bản C: Interviewer hỏi về đóng góp cụ thể của bạn

```
INT: "Bạn nói bạn 'xây dựng' hệ thống này. Phần nào là của bạn,
     phần nào team làm? Đóng góp cụ thể nhất của bạn là gì?"

CAND: "Em muốn transparent về điều này:

Phần em thiết kế và implement end-to-end:
1. Toàn bộ CDC pipeline: config Debezium connector, Kafka topics,
   Spark Streaming job đọc CDC và ghi vào HDFS.
   Cụ thể: em viết code PySpark transform CDC events (before/after) 
   thành SCD2 merge logic — đây là phần phức tạp nhất.

2. Data Quality Gate 5 tầng — em build cái này SAU một sự cố thật:
   Debezium connector âm thầm chết 2 ngày, không ai biết, dashboard
   vẫn hiển thị data cũ. Em build monitoring framework phát hiện sự cố
   trong 5 phút: check replication slot lag, Kafka consumer lag,
   freshness check, volume anomaly, và row count reconciliation.

3. Redis Lua Script cho slot reservation — em research và implement
   atomic pattern sau khi phát hiện race condition trong testing:
   2 test requests cùng lúc đều được accept vào cùng 1 slot.

Phần team khác làm:
- Odoo ERP setup và configuration
- PowerBI dashboard visual design
- App mobile cho tài xế
- Infrastructure setup (máy chủ, network)

Em chịu trách nhiệm 100% lớp data engineering: từ ingestion đến serving."
```

### Kịch bản D: Câu hỏi về outcome — interviewer muốn CON SỐ

```
INT: "Outcome của dự án là gì? Bạn đo lường thế nào?"

CAND: "Em track 5 metrics cụ thể:

1. Query load trên OLTP DB:
   Trước: DBA phàn nàn DB bị chậm vào cuối tháng vì reporting queries
   Sau: 0% additional load. Debezium chỉ đọc WAL, không query tables.
   Đo bằng: pg_stat_activity — không còn long-running SELECT từ reporting tools

2. Dashboard data freshness:
   Trước: Báo cáo Excel cập nhật T+30 ngày
   Sau: Streaming data fresh trong < 5 phút, batch aggregation trong < 30 phút
   Đo bằng: MAX(loaded_at) - MAX(event_timestamp) per table

3. Tìm trạm gần nhất:
   Trước: Service query PostGIS trực tiếp, p99 latency 85ms
   Sau: Redis GEOSEARCH, p99 latency < 1ms
   Đo bằng: Application logs, percentile calculation

4. Booking overbooking:
   Trước: Không đo được (không có system, dùng điện thoại)
   Sau: 0 overbooking events trong 3 tháng vận hành
   Đo bằng: Application audit log

5. Data Quality incidents:
   Trước: Phát hiện sự cố sau 2 ngày (CDC connector chết)
   Sau: Alert trong < 5 phút
   Đo bằng: Airflow alert log timestamps"
```

### Kịch bản E: Câu hỏi cải thiện — "bạn đã cải thiện gì?"

```
INT: "Trong quá trình vận hành, bạn đã phát hiện và cải thiện điều gì?"

CAND: "3 lần cải thiện đáng kể, theo thứ tự thời gian:

Cải thiện #1 — Small File Problem (tuần 3):
Phát hiện: NameNode Web UI báo heap 80%, GC logs cho thấy GC mỗi 30 giây
Root cause: 150,000 file nhỏ từ Spark Streaming trigger 10s
Fix: Tăng trigger lên 5 phút + Airflow nightly compaction job
Result: NameNode heap xuống 40%, GC giảm 95%

Cải thiện #2 — Data Quality Gate (sau sự cố CDC chết):
Phát hiện: 2 ngày dashboard hiển thị data cũ, không ai phát hiện
Root cause: Không có monitoring cho CDC pipeline health
Fix: Build 5-layer quality gate, tích hợp vào Airflow
Result: Phát hiện mọi sự cố trong < 5 phút

Cải thiện #3 — SCD2 Merge Performance (tháng 2):
Phát hiện: Airflow batch job tổng hợp DWH chạy 4 giờ thay vì 45 phút
Root cause: SCD2 merge dùng LEFT JOIN trên full table thay vì chỉ join
           với records đã thay đổi (CDC delta)
Fix: Filter chỉ CDC events trong batch window → join chỉ những records
     thay đổi → broadcast dim bảng nhỏ
Result: 4 giờ → 45 phút"
```

---

## 🎬 TEMPLATE KẾT HỢP — "ROLLS ROYCE" 90 GIÂY CHO FLEET PROJECT

> **[10 giây — Business Problem]**
> *"Fleet vận hành 50 trạm sửa chữa xe tải với 2 nguồn doanh thu: dịch vụ và linh kiện. Ban Giám đốc cần real-time visibility để ra quyết định — nhưng chỉ có báo cáo Excel T+30 ngày."*
>
> **[15 giây — My Role]**
> *"Em thiết kế và build toàn bộ data pipeline — từ CDC ingestion đến real-time dashboard, bao gồm việc lựa chọn công nghệ và vận hành production."*
>
> **[30 giây — Key Technical Decisions]**
> *"Quyết định quan trọng nhất: Debezium CDC thay vì query polling — 0% load lên OLTP DB, bắt được cả DELETE events. HDFS Parquet thay vì relational DB cho data lake — columnar storage cho Spark batch đọc 10x nhanh hơn. Redis GEOSEARCH + Lua atomic cho matching và slot reservation < 1ms."*
>
> **[20 giây — Outcomes với số liệu]**
> *"Dashboard latency: từ T+30 ngày xuống T+15 phút. OLTP DB load: từ có ảnh hưởng xuống 0%. Tìm trạm: từ 85ms xuống < 1ms. Overbooking: 0 incidents trong 3 tháng."*
>
> **[15 giây — What I improved + What I'd change]**
> *"Cải thiện đáng kể nhất: build Data Quality Gate sau sự cố CDC chết 2 ngày — alert trong 5 phút. Nếu làm lại: thêm Schema Registry để xử lý schema evolution tự động."*

---

## 📋 CHECKLIST LUYỆN TẬP

- [ ] Nói trôi chảy kiến trúc Cụm Multi-Node (Master, Slave1, Slave2), 8 điểm cốt tử (Ubuntu 127.0.1.1 gotcha, replication=2, format 1 lần, log-aggregation, du.reserved) và lý do cấu hình từng file XML
- [ ] Giải thích replication slot nguy hiểm như thế nào + cách monitor
- [ ] Giải thích Lua Script atomic + race condition scenario
- [ ] Trả lời "Kafka broker crash" scenario phân tích từng layer
- [ ] Kể 3 lần cải thiện dự án với số liệu cụ thể
- [ ] Trả lời "Delta Lake vs HDFS" với context cụ thể của dự án
- [ ] Phân biệt rõ phần MÌNH làm vs phần team làm
- [ ] Kể Data Quality Gate story (sự cố → phát hiện → fix → kết quả)

