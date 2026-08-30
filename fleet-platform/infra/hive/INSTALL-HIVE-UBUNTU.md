# HƯỚNG DẪN CÀI ĐẶT & CẤU HÌNH APACHE HIVE 3.1.3 TRÊN UBUNTU BARE-METAL (NO DOCKER)
## Tích Hợp PostgreSQL Metastore, HDFS Warehouse, HiveServer2 & Cầu Nối Spark SQL Catalog

> **Vai Trò Kiến Trúc của Apache Hive trong Fleet Platform:**
> *"Trong hệ sinh thái Data Platform, Apache Hive đóng vai trò là **Metadata Catalog tập trung (Hive Metastore - HMS)**. Nó quản trị toàn bộ cấu trúc Schema, thông tin phân vùng (Partition locations) và kiểu dữ liệu của các bảng Data Lake trên HDFS, giúp **Spark SQL, Trino, Airflow, DBeaver và PowerBI** truy vấn trực tiếp dữ liệu Parquet bằng ngôn ngữ SQL chuẩn mà không cần nạp lại schema từ đầu."*

---

# MỤC LỤC BÀI HƯỚNG DẪN

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ PHẦN 1: KIẾN TRÚC TỔNG THỂ & PHÂN BỔ DỊCH VỤ HIVE TRÊN MÁY MASTER                                      │
│ PHẦN 2: CHUẨN BỊ MÔI TRƯỜNG & KHỞI TẠO METASTORE DATABASE TRÊN POSTGRESQL                              │
│ PHẦN 3: TẢI & CÀI ĐẶT BINARY APACHE HIVE 3.1.3 VÀO /usr/local/hive                                    │
│ PHẦN 4: 2 BẪY LỖI CHẾT NGƯỜI BẮT BUỘC PHẢI FIX (GUAVA CONFLICT & HDFS PERMISSIONS)                    │
│ PHẦN 5: CẤU HÌNH HIVE-SITE.XML & KHỞI TẠO SCHEMATOOL                                                   │
│ PHẦN 6: TÍCH HỢP HIVE METASTORE VỚI APACHE SPARK SQL (ĐỌC CHUNG CATALOG)                               │
│ PHẦN 7: TẠO 2 SYSTEMD SERVICES QUẢN LÝ TỰ ĐỘNG (METASTORE & HIVESERVER2)                               │
│ PHẦN 8: KIỂM THỬ TRUY VẤN VỚI BEELINE CLI & TẠO BẢNG EXTERNAL PARQUET                                  │
│ PHẦN 9: BẢNG TỔNG HỢP CÁC BẪY CẤU HÌNH & THAM SỐ SỐNG CÒN CỦA HIVE                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 1: KIẾN TRÚC TỔNG THỂ HIVE TRÊN MÁY CHỦ BARE-METAL

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ KIẾN TRÚC HIVE METASTORE & HIVESERVER2 TRÊN MÁY CHỦ MASTER (192.168.1.10):                             │
│                                                                                                        │
│  [Beeline / DBeaver / BI] ──(Port 10000 JDBC)──► [HiveServer2]                                         │
│                                                        │                                               │
│  [Spark SQL / PySpark] ───(Port 9083 Thrift)───► [Hive Metastore (HMS)] ──► [PostgreSQL: metastore_db]│
│                                                        │                    (Schema & Partitions)      │
│                                                        ▼                                               │
│                                            [Hadoop HDFS Storage]                                       │
│                                            (/user/hive/warehouse & /fleet-datalake)                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

* **Dịch vụ 1: Hive Metastore (Cổng 9083)**: Service chạy ngầm phục vụ giao thức Thrift, kết nối trực tiếp vào PostgreSQL `metastore_db` để quản lý metadata.
* **Dịch vụ 2: HiveServer2 (Cổng 10000)**: Service lắng nghe kết nối JDBC/ODBC từ các client bên ngoài (Beeline, DBeaver, PowerBI).
* **Vị trí cài đặt**: Máy chủ `master` (192.168.1.10) tại `/usr/local/hive` (hoặc `/opt/hive`).

---

# PHẦN 2: CHUẨN BỊ MÔI TRƯỜNG & KHỞI TẠO DATABASE TRÊN POSTGRESQL

### 1. Khởi tạo Database `metastore_db` trên PostgreSQL 14+ (Máy `master`):
```bash
sudo -u postgres psql
```

Chạy các câu lệnh SQL sau:
```sql
-- 1. Tạo Database riêng lưu Metadata của Hive
CREATE DATABASE metastore_db;

-- 2. Tạo User riêng cho Hive
CREATE USER hive WITH PASSWORD 'HivePass_VCS_2026';

-- 3. Cấp toàn quyền cho user hive
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO hive;

\c metastore_db
GRANT ALL ON SCHEMA public TO hive;
ALTER SCHEMA public OWNER TO hive;
\q
```

### 2. Cấu hình xác thực mạng trong `/etc/postgresql/14/main/pg_hba.conf`:
Thêm dòng sau để cho phép user `hive` kết nối từ local và từ các máy trong cụm LAN:
```text
host    metastore_db    hive            192.168.1.0/24          scram-sha-256
host    metastore_db    hive            127.0.0.1/32            scram-sha-256
```
Reload lại PostgreSQL:
```bash
sudo systemctl reload postgresql
```

---

# PHẦN 3: TẢI & CÀI ĐẶT BINARY APACHE HIVE 3.1.3

Thực hiện trên máy `master`:

```bash
# 1. Tải Apache Hive 3.1.3 Binary
cd /tmp
wget https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz

# 2. Giải nén và chuyển vào thư mục chuẩn
tar -xzvf apache-hive-3.1.3-bin.tar.gz
sudo mv apache-hive-3.1.3-bin /usr/local/hive

# 3. Phân quyền sở hữu cho user chạy hệ thống (ví dụ: aiguystory hoặc hadoop)
sudo chown -R $USER:$USER /usr/local/hive

# 4. Tải PostgreSQL JDBC Driver (Bắt buộc để Hive kết nối tới PostgreSQL)
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
cp postgresql-42.6.0.jar /usr/local/hive/lib/

# 5. Khai báo biến môi trường vào ~/.bashrc
cat << 'EOF' >> ~/.bashrc

# --- APACHE HIVE ENVIRONMENT ---
export HIVE_HOME=/usr/local/hive
export HIVE_CONF_DIR=$HIVE_HOME/conf
export PATH=$PATH:$HIVE_HOME/bin
EOF

source ~/.bashrc
```

---

# PHẦN 4: 2 BẪY LỖI CHẾT NGƯỜI BẮT BUỘC PHẢI FIX TRƯỚC KHI CHẠY

---

### 🚨 BẪY 1: XUNG ĐỘT THƯ VIỆN GUAVA JAR (BẪY KINH ĐIỂN HADOOP 3.X VỚI HIVE 3.X)
* **Hiện tượng**: Khi chạy `schematool` hoặc `hive`, hệ thống văng lỗi: **`java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument`**.
* **Nguyên nhân**: Hive 3.1.3 đi kèm `guava-19.0.jar` (quá cũ), trong khi Hadoop 3.3.6 sử dụng `guava-27.0-jre.jar` $\implies$ Xung đột phiên bản bytecode JVM!
* **Cách xử lý triệt để**:
  ```bash
  # 1. Xóa file Guava cũ trong thư mục thư viện của Hive
  rm /usr/local/hive/lib/guava-19.0.jar

  # 2. Copy file Guava chuẩn từ Hadoop sang Hive
  cp /usr/local/hadoop/share/hadoop/common/lib/guava-*.jar /usr/local/hive/lib/
  ```

---

### 🚨 BẪY 2: PHÂN QUYỀN THƯ MỤC WAREHOUSE & TẠM TRÊN HDFS
* **Hiện tượng**: Hive không tạo được bảng hoặc báo lỗi quyền truy cập `Permission denied` trên `/tmp/hive` hoặc `/user/hive/warehouse`.
* **Cách xử lý triệt để**:
  ```bash
  # 1. Tạo các thư mục lưu trữ cốt lõi trên HDFS
  hdfs dfs -mkdir -p /tmp/hive
  hdfs dfs -mkdir -p /user/hive/warehouse

  # 2. Phân quyền đầy đủ cho các dịch vụ đọc ghi
  hdfs dfs -chmod -R 777 /tmp
  hdfs dfs -chmod -R 777 /tmp/hive
  hdfs dfs -chmod -R 775 /user/hive/warehouse
  ```

---

# PHẦN 5: CẤU HÌNH HIVE-SITE.XML & KHỞI TẠO SCHEMATOOL

### 1. Tạo file cấu hình `/usr/local/hive/conf/hive-site.xml`:
```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <!-- 1. Kết nối PostgreSQL Metastore -->
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:postgresql://master:5432/metastore_db</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>org.postgresql.Driver</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>hive</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>HivePass_VCS_2026</value>
    </property>

    <!-- 2. Thư mục HDFS Warehouse -->
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>/user/hive/warehouse</value>
    </property>
    <property>
        <name>hive.exec.scratchdir</name>
        <value>/tmp/hive</value>
    </property>

    <!-- 3. Thrift Metastore URI (Port 9083) cho Spark/Trino -->
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://master:9083</value>
    </property>
    <property>
        <name>hive.metastore.schema.verification</name>
        <value>false</value>
    </property>

    <!-- 4. HiveServer2 Port (Port 10000) -->
    <property>
        <name>hive.server2.thrift.port</name>
        <value>10000</value>
    </property>
    <property>
        <name>hive.server2.thrift.bind.host</name>
        <value>master</value>
    </property>
    <property>
        <name>hive.server2.enable.doAs</name>
        <value>false</value>
    </property>
</configuration>
```

### 2. Khởi tạo Schema với `schematool` (Chỉ chạy DUY NHẤT 1 LẦN):
```bash
schematool -dbType postgres -initSchema
```
* **Dấu hiệu thành công**: Dòng cuối cùng in ra: **`schemaTool completed`**.
* **Kiểm tra trong PostgreSQL**:
  ```bash
  sudo -u postgres psql -d metastore_db -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public';"
  ```
  $\implies$ Trả về **$\sim 74$ bảng metadata** (như `TBLS`, `DBS`, `PARTITIONS`, `SDS`...).

---

# PHẦN 6: TÍCH HỢP HIVE METASTORE VỚI APACHE SPARK SQL

Để Spark có thể đọc/ghi trực tiếp vào Catalog của Hive mà không cần nạp lại schema:

```bash
# 1. Copy file hive-site.xml sang thư mục cấu hình của Spark
cp /usr/local/hive/conf/hive-site.xml /usr/local/spark/conf/

# 2. Copy PostgreSQL JDBC Driver sang thư mục jars của Spark
cp /usr/local/hive/lib/postgresql-42.6.0.jar /usr/local/spark/jars/
```

Thêm cấu hình vào `/usr/local/spark/conf/spark-defaults.conf`:
```properties
spark.sql.catalogImplementation    hive
spark.sql.warehouse.dir            hdfs://master:9000/user/hive/warehouse
hive.metastore.uris                thrift://master:9083
```

---

# PHẦN 7: TẠO 2 SYSTEMD SERVICES QUẢN LÝ TỰ ĐỘNG

Tạo 2 file service trên Ubuntu để Hive tự khởi động cùng hệ điều hành:

### 1. File `/etc/systemd/system/hive-metastore.service`:
```ini
[Unit]
Description=Apache Hive Metastore Service
After=network.target postgresql.service hadoop.service

[Service]
Type=simple
User=aiguystory
Environment="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"
Environment="HADOOP_HOME=/usr/local/hadoop"
Environment="HIVE_HOME=/usr/local/hive"
ExecStart=/usr/local/hive/bin/hive --service metastore
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

### 2. File `/etc/systemd/system/hive-server2.service`:
```ini
[Unit]
Description=Apache HiveServer2 Thrift Service
After=network.target hive-metastore.service

[Service]
Type=simple
User=aiguystory
Environment="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"
Environment="HADOOP_HOME=/usr/local/hadoop"
Environment="HIVE_HOME=/usr/local/hive"
ExecStart=/usr/local/hive/bin/hive --service hiveserver2
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

### 3. Kích hoạt & Khởi động Services:
```bash
sudo systemctl daemon-reload
sudo systemctl enable hive-metastore hive-server2
sudo systemctl start hive-metastore hive-server2
```

---

# PHẦN 8: KIỂM THỬ TRUY VẤN VỚI BEELINE CLI & TẠO BẢNG EXTERNAL

### 1. Kết nối qua Beeline CLI:
```bash
beeline -u "jdbc:hive2://master:10000/default;auth=noSasl" -n hive
```

### 2. Tạo Database và Bảng External Parquet (Trỏ trực tiếp vào Data Lake HDFS):
```sql
-- Tạo Database Fleet DWH
CREATE DATABASE IF NOT EXISTS fleet_dwh;
USE fleet_dwh;

-- Tạo Bảng External đọc trực tiếp file Parquet từ Data Lake
CREATE EXTERNAL TABLE IF NOT EXISTS fact_repair_revenue (
    work_order_id INT,
    date_sk INT,
    head_sk INT,
    customer_sk STRING,
    labor_amount DECIMAL(15, 2),
    service_duration_hours DECIMAL(6, 2)
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://master:9000/fleet-datalake/gold/fact_repair_revenue';

-- Đồng bộ phân vùng mới
MSCK REPAIR TABLE fact_repair_revenue;

-- Truy vấn kiểm tra
SELECT * FROM fact_repair_revenue LIMIT 5;
```

---

# PHẦN 9: BẢNG TỔNG HỢP CÁC BẪY CẤU HÌNH SỐNG CÒN CỦA HIVE

```
┌──────────────────────────┬──────────────────────────────────────────┬──────────────────────────────────────────┐
│ Bẫy Cấu Hình             │ Hiện Tượng Lỗi                           │ Cách Xử Lý Chuẩn Xác                     │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 1. Guava Version Mismatch│ `NoSuchMethodError: checkArgument`       │ Xóa `guava-19.0.jar` trong Hive, copy    │
│                          │ khi chạy `schematool`                    │ `guava-27.0-jre.jar` từ Hadoop sang.     │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 2. Missing JDBC Driver   │ `ClassNotFoundException: Driver`         │ Tải `postgresql-42.x.jar` bỏ vào         │
│                          │ khi kết nối PostgreSQL metastore         │ `/usr/local/hive/lib/`.                  │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 3. HDFS Permissions      │ `Permission denied on /tmp/hive`         │ `hdfs dfs -chmod -R 777 /tmp/hive`       │
│                          │ hoặc `/user/hive/warehouse`              │ `hdfs dfs -chmod -R 775 /user/hive/...`  │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 4. Spark Integration     │ Spark SQL không nhìn thấy bảng của Hive  │ Copy `hive-site.xml` sang `spark/conf/`  │
│                          │                                          │ và set `spark.sql.catalogImplementation=hive│
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 5. Schema Auto-Create    │ Lỗi schema không đồng bộ khi chạy query  │ Set `datanucleus.schema.autoCreateAll=false│
│                          │                                          │ và bắt buộc dùng `schematool -initSchema`│
└──────────────────────────┴──────────────────────────────────────────┴──────────────────────────────────────────┘
```

---

Toàn bộ hướng dẫn trên hoàn thiện **mảnh ghép thứ 8 (Apache Hive)** trong hạ tầng Bare-Metal của bạn, biến cụm máy chủ thành một **Enterprise Data Lakehouse hoàn chỉnh**!
