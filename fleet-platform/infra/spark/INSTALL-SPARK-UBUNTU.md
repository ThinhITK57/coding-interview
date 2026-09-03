# Cài đặt Apache Spark cho Fleet Platform

> **Vai trò**: Spark chạy cả Structured Streaming (ELT telemetry) và Batch (DWH aggregation).
> Spark cần kết nối tới **HDFS** (đọc/ghi Parquet) và **Kafka** (consume topics).
> Chạy Spark Standalone mode — Master trên `master`, Workers trên `slave1` + `slave2`.

---

## 0. Tiền điều kiện

- ✅ Java đã cài (cùng bản với Hadoop)
- ✅ Hadoop HDFS multinode đang chạy
- ✅ SSH passwordless từ master → slave1, slave2

---

## 1. Cài đặt Spark trên TẤT CẢ 3 nodes

```bash
# Chạy trên master, slave1, slave2 (hoặc dùng script loop)
# Kiểm tra bản mới nhất: https://spark.apache.org/downloads.html

cd /opt
sudo wget https://downloads.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
sudo tar -xzf spark-3.5.3-bin-hadoop3.tgz
sudo mv spark-3.5.3-bin-hadoop3 spark
sudo chown -R aiguystory:aiguystory /opt/spark
sudo rm spark-3.5.3-bin-hadoop3.tgz
```

---

## 2. Thiết lập biến môi trường (TẤT CẢ 3 nodes)

```bash
# Thêm vào ~/.bashrc
cat >> ~/.bashrc << 'EOF'

# Spark
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Hadoop (Spark cần biết HADOOP_CONF_DIR để đọc/ghi HDFS)
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
EOF

source ~/.bashrc
```

---

## 3. Cấu hình Spark Master & Workers

### 3.1 spark-env.sh (trên master)

```bash
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
nano $SPARK_HOME/conf/spark-env.sh
```

Thêm:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_MASTER_HOST=master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Memory cho mỗi Worker (tùy RAM máy slave)
export SPARK_WORKER_MEMORY=2g
export SPARK_WORKER_CORES=2

# Hadoop config để Spark đọc/ghi HDFS
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
```

### 3.2 workers file

```bash
nano $SPARK_HOME/conf/workers
```

Nội dung (xóa localhost nếu có):
```
slave1
slave2
```

### 3.3 Copy config sang slaves

```bash
scp $SPARK_HOME/conf/spark-env.sh aiguystory@slave1:$SPARK_HOME/conf/
scp $SPARK_HOME/conf/spark-env.sh aiguystory@slave2:$SPARK_HOME/conf/
scp $SPARK_HOME/conf/workers aiguystory@slave1:$SPARK_HOME/conf/
scp $SPARK_HOME/conf/workers aiguystory@slave2:$SPARK_HOME/conf/
```

---

## 4. Cấu hình spark-defaults.conf

```bash
cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
```

File config đã chuẩn bị: `fleet-platform/infra/spark/config/spark-defaults.conf`

---

## 5. Tải Kafka connector JARs cho Spark

Spark cần JARs để đọc Kafka topics (Structured Streaming):

```bash
cd $SPARK_HOME/jars

# Spark-Kafka connector
wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar

# Kafka clients
wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.1/kafka-clients-3.7.1.jar

# Spark token provider
wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar

# Commons pool (dependency)
wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

# Copy sang slaves
for jar in spark-sql-kafka-0-10_2.12-3.5.3.jar kafka-clients-3.7.1.jar spark-token-provider-kafka-0-10_2.12-3.5.3.jar commons-pool2-2.12.0.jar; do
  scp $SPARK_HOME/jars/$jar aiguystory@slave1:$SPARK_HOME/jars/
  scp $SPARK_HOME/jars/$jar aiguystory@slave2:$SPARK_HOME/jars/
done
```

---

## 6. Khởi động Spark Cluster

```bash
# Trên master — start cả Master + Workers (qua SSH)
$SPARK_HOME/sbin/start-all.sh

# Verify
# Web UI: http://master:8080 — phải thấy 2 workers (slave1, slave2)
jps  # Trên master: phải thấy Master
ssh slave1 jps  # Phải thấy Worker
ssh slave2 jps  # Phải thấy Worker
```

---

## 7. Test: Spark đọc HDFS

```bash
# Tạo file test trên HDFS
hdfs dfs -mkdir -p /fleet-datalake/test
echo "hello fleet platform" | hdfs dfs -put - /fleet-datalake/test/hello.txt

# Chạy PySpark shell
pyspark --master spark://master:7077

# Trong PySpark shell:
>>> df = spark.read.text("hdfs:///fleet-datalake/test/hello.txt")
>>> df.show()
# Phải thấy: hello fleet platform

>>> exit()

# Cleanup
hdfs dfs -rm -r /fleet-datalake/test
```

---

## 8. Tạo HDFS directories cho dự án

```bash
hdfs dfs -mkdir -p /fleet-datalake/raw/truck-telemetry
hdfs dfs -mkdir -p /fleet-datalake/raw/repair-request
hdfs dfs -mkdir -p /fleet-datalake/curated/cdc
hdfs dfs -mkdir -p /fleet-datalake/warehouse/facts
hdfs dfs -mkdir -p /fleet-datalake/warehouse/dimensions
hdfs dfs -mkdir -p /fleet-datalake/checkpoints/streaming
hdfs dfs -mkdir -p /fleet-datalake/checkpoints/batch

# Verify
hdfs dfs -ls -R /fleet-datalake/
```
