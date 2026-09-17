# pip uninstall pyspark -y 
# pip install pyspark==3.3.2 --proxy http://192.168.5.8:3128
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar -o hadoop-aws-3.4.1.jar
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.16/bundle-2.25.16.jar -o aws-bundle-2.25.16.jar

# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar -o hadoop-aws-3.4.0.jar
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.767/aws-java-sdk-bundle-1.12.767.jar -o aws-java-sdk-bundle-1.12.767.jar

# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar -o hadoop-aws-3.4.1.jar
# AWS SDK S3
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.25.16/s3-2.25.16.jar -o aws-sdk-s3-2.25.16.jar
# AWS SDK Core
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/core/2.25.16/core-2.25.16.jar -o aws-sdk-core-2.25.16.jar
# AWS SDK Auth
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/auth/2.25.16/auth-2.25.16.jar -o aws-sdk-auth-2.25.16.jar
# AWS SDK Regions
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/regions/2.25.16/regions-2.25.16.jar -o aws-sdk-regions-2.25.16.jar


# 1. Hadoop-AWS (Bạn đã có, nhưng nên dùng bản này cho đồng bộ)
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar -o hadoop-aws-3.4.0.jar
# 2. AWS SDK V2 Core (Đây là file chứa class bị thiếu của bạn)
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/sdk-core/2.25.27/sdk-core-2.25.27.jar -o sdk-core-2.25.27.jar
# 3. AWS SDK V2 S3
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.25.27/s3-2.25.27.jar -o s3-2.25.27.jar
# 4. AWS SDK V2 HTTP Client (Cần thiết để SDK hoạt động)
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/apache-client/2.25.27/apache-client-2.25.27.jar -o apache-client-2.25.27.jar
# 5. AWS SDK V2 Auth
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/auth/2.25.27/auth-2.25.27.jar -o auth-2.25.27.jar
# 6. AWS SDK V2 Regions
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/regions/2.25.27/regions-2.25.27.jar -o regions-2.25.27.jar

#  curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/s3-transfer-manager/2.25.27/s3-transfer-manager-2.25.27.jar -o s3-transfer-manager-2.25.27.jar
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/crt/0.29.11/crt-0.29.11.jar -o crt-0.29.11.jar
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/netty-nio-client/2.25.27/netty-nio-client-2.25.27.jar -o netty-nio-client-2.25.27.jar
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.25.27/url-connection-client-2.25.27.jar -o url-connection-client-2.25.27.jar
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/metrics-spi/2.25.27/metrics-spi-2.25.27.jar -o metrics-spi-2.25.27.jar
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/profiles/2.25.27/profiles-2.25.27.jar -o profiles-2.25.27.jar
# curl -k -x http://192.168.5.8:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/json-utils/2.25.27/json-utils-2.25.27.jar -o json-utils-2.25.27.jar


# mkdir -p "C:\Users\namtv40\Projects\bi-datalake-infra\migrations\tmp\spark\bin"
# curl -k -x http://192.168.5.8:3128 -L https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe -o "C:\Users\namtv40\Projects\bi-datalake-infra\migrations\tmp\spark\bin\winutils.exe"



# copy libs to C:\Users\namtv40\appdata\local\anaconda3\lib\site-packages\pyspark\jars


from dotenv import load_dotenv
load_dotenv()
import os
import warnings
from urllib3.exceptions import InsecureRequestWarning
from trino.auth import BasicAuthentication
import sys

warnings.simplefilter('ignore', InsecureRequestWarning)

import socketserver

if not hasattr(socketserver, "UnixStreamServer"):
    class DummyUnixStreamServer:
        pass
    socketserver.UnixStreamServer = DummyUnixStreamServer

from pyspark.sql import SparkSession

import os
import uuid
import pyarrow as pa
import pyarrow.parquet as pq


#    .config(
#         "spark.jars.excludes",
#         "io.netty:netty-buffer,"
#         "io.netty:netty-codec,"
#         "io.netty:netty-common,"
#         "io.netty:netty-handler,"
#         "io.netty:netty-transport"
#     ) \

current_dir = os.path.dirname(os.path.abspath(__file__))


jar_dir = r"C:\Users\namtv40\Projects\bi-datalake-infra\migrations\tmp\aws_jars"
all_jars = [os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith(".jar")]

print(all_jars)

HADOOP_HOME = os.path.join(current_dir, "tmp/spark") 
os.makedirs(HADOOP_HOME, exist_ok=True)

os.environ["HADOOP_HOME"] =HADOOP_HOME

spark = SparkSession.builder \
    .appName("DeduplicateTables") \
    .master("local[5]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://10.255.245.150:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY") ) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.driver.memory", "8g")\
    .config("spark.executor.memory", "8g")\
    .config("spark.driver.maxResultSize", "2g")\
    .config("spark.sql.shuffle.partitions", "8")\
    .config(
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
        "2"
    )\
    .config(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
    )\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraJavaOptions", "-Dhttp.proxyHost=192.168.5.8 -Dhttp.proxyPort=3128 -Dhttps.proxyHost=192.168.5.8 -Dhttps.proxyPort=3128 -Dhttp.nonProxyHosts=10.*,localhost,127.0.0.1,10.255.245.150 -Dhttps.nonProxyHosts=10.*,localhost,127.0.0.1,10.255.245.150")\
    .config("spark.executor.extraJavaOptions", "-Dhttp.proxyHost=192.168.5.8 -Dhttp.proxyPort=3128 -Dhttps.proxyHost=192.168.5.8 -Dhttps.proxyPort=3128 -Dhttp.nonProxyHosts=10.*,localhost,127.0.0.1,10.255.245.150 -Dhttps.nonProxyHosts=10.*,localhost,127.0.0.1,10.255.245.150")\
    .config( "spark.jars.packages",
"org.apache.hadoop:hadoop-aws:3.4.1,"
"software.amazon.awssdk:bundle:2.29.52"
)\
    .getOrCreate()
    # .config("spark.driver.extraJavaOptions", "-Dhttp.proxyHost=192.168.5.8 -Dhttp.proxyPort=3128 -Dhttps.proxyHost=192.168.5.8 -Dhttps.proxyPort=3128")\
    # .config("spark.executor.extraJavaOptions", "-Dhttp.proxyHost=192.168.5.8 -Dhttp.proxyPort=3128 -Dhttps.proxyHost=192.168.5.8 -Dhttps.proxyPort=3128")\
    # .config("spark.jars", ','.join(all_jars)) \
    
# spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
spark.conf.set("spark.sql.debug.maxToStringFields", 10000)
print(spark.version)

print(
    spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
)

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, desc

input_path = r"C:\Users\namtv40\Projects\bi-datalake-infra\migrations\tmp\cx_cso_raw\fact_cso_tickets_20260722_064738/*.parquet"
# output_path = r"C:\Users\namtv40\Projects\bi-datalake-infra\migrations\tmp\cx_cso_raw\fact_cso_tickets_dedup"
output_path = "C:/Users/namtv40/Projects/bi-datalake-infra/migrations/tmp/cx_cso_raw/fact_cso_tickets_dedup"
output_path = "file:///C:/Users/namtv40/Projects/bi-datalake-infra/migrations/tmp/cx_cso_raw/fact_cso_tickets_dedup"

# Đọc toàn bộ parquet
from glob import glob

files = glob(
    r"C:\Users\namtv40\Projects\bi-datalake-infra\migrations\tmp\cx_cso_raw\fact_cso_tickets_20260722_064738\*.parquet"
)
print(len(files))
print(files[:3])

df = spark.read.parquet(*files)
df = df.repartition(20, "id")

# Deduplicate
window = (
    Window.partitionBy("id")
          .orderBy(desc("updated_at_ts"))
)

df_dedup = (
    df.withColumn("rn", row_number().over(window))
      .filter(col("rn") == 1)
      .drop("rn")
)

# Ghi lại
# (
#     df_dedup
#     .write
#     .mode("overwrite")
#     .option("compression", "snappy")
#     .parquet("s3a://vcs-raw/cx-cso-raw/fact_cso_tickets_dedup/")
# )
(
    df_dedup
    .repartition(1)
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(output_path)
)

# pdf = df_dedup.toPandas()

# pdf.to_parquet(
#     r"C:\Temp\fact_cso_tickets.parquet",
#     engine="pyarrow",
#     compression="snappy",
#     index=False,
# )

spark.stop()