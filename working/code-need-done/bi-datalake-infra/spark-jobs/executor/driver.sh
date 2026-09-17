#!/bin/bash
set -e

export JAVA_HOME=/u01/platform/jdk-21.0.9
export MAVEN_HOME=/u01/platform/apache-maven-3.9.14
export M2_HOME=/u01/platform/apache-maven-3.9.14
export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$PATH


DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# ===== Versions =====
export TRINO_VERSION=438
# 3.5.1
export SPARK_VERSION=3.5.1
export HADOOP_VERSION=3
# 3.3.4, 3.4.1
export HADOOP_FULL_VERSION=3.3.4
# 2.25.16, 
export AWS_VERSION=1.12.262
export POSGRESQL_VERSION=42.7.2


FILENAME="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${FILENAME}"

mkdir -p ./downloads

if [ ! -f "./downloads/${FILENAME}" ]; then
    echo "--- File ${FILENAME} not found. Starting download... ---"
    curl -k -L -o "./downloads/${FILENAME}" "$URL"
    if [ $? -eq 0 ]; then
        echo "--- Download successful! ---"
    else
        echo "--- Error: Download failed! Check your proxy or internet connection. ---"
        exit 1
    fi
else
    echo "--- File ${FILENAME} already exists. Skipping download. ---"
fi

echo "rm -rf downloads/spark-3.5.1-bin-hadoop3/"
rm -rf downloads/spark-3.5.1-bin-hadoop3/

echo "rm -rf jars/*.jar"
rm -rf jars/*.jar

echo "tar -xzf ./downloads/${FILENAME} -C downloads/"
tar -xzf "./downloads/${FILENAME}" -C downloads/

echo "mvn clean package -DskipTests"
mvn clean package -DskipTests

echo "Copying built JARs to the jars directory..."
# rm -f ./downloads/spark-3.5.1-bin-hadoop3/jars/hive-*.jar
# cp -f ./jars/hive-*.jar ./downloads/spark-3.5.1-bin-hadoop3/jars/


echo "Copying AWS and Hadoop JARs to Spark's jars directory..."
cp ./jars/aws-java-sdk-bundle-1.12.262.jar downloads/spark-3.5.1-bin-hadoop3/jars/
cp ./jars/hadoop-aws-3.3.4.jar downloads/spark-3.5.1-bin-hadoop3/jars/