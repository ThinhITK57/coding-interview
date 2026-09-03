# =============================================================================
# hive-env.sh — Khai báo biến môi trường cho Apache Hive
# Vị trí: /opt/hive/conf/hive-env.sh (hoặc /usr/local/hive/conf/hive-env.sh)
# =============================================================================

# 1. Đường dẫn Java & Hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# 2. Thư mục cấu hình Hive
export HIVE_HOME=/usr/local/hive
export HIVE_CONF_DIR=$HIVE_HOME/conf

# 3. Nạp thư viện bổ sung (PostgreSQL JDBC driver)
export HIVE_AUX_JARS_PATH=$HIVE_HOME/lib/postgresql-42.6.0.jar
