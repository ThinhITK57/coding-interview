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
    .config("spark.jars", ','.join(all_jars)) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://10.255.245.150:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY") ) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
    # .config("spark.driver.extraJavaOptions", "-Dhttp.proxyHost=192.168.5.8 -Dhttp.proxyPort=3128 -Dhttps.proxyHost=192.168.5.8 -Dhttps.proxyPort=3128")\
    # .config("spark.executor.extraJavaOptions", "-Dhttp.proxyHost=192.168.5.8 -Dhttp.proxyPort=3128 -Dhttps.proxyHost=192.168.5.8 -Dhttps.proxyPort=3128")\
    
# spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

import trino

def get_trino_cursor(schema="bi_silver"):
    return trino.dbapi.connect(
        host="10.255.245.150",
        port=8443,
        user= os.getenv("TRINO_DST_USERNAME"),
        catalog="hive",
        schema=schema,
        http_scheme="https",
        auth=BasicAuthentication(os.getenv("TRINO_DST_USERNAME"), os.getenv("TRINO_DST_PASSWORD")),
        verify=False,
    ).cursor()
    
def get_tables(cursor, schema):
    cursor.execute(f"SHOW TABLES FROM hive.{schema}")
    return [row[0] for row in cursor.fetchall()]

import re

def get_location(cursor, schema, table):
    full = f"hive.{schema}.{table}"
    cursor.execute(f"SHOW CREATE TABLE {full}")
    ddl = cursor.fetchone()[0]

    match = re.search(r"(external_location|location)\s*=\s*'([^']+)'", ddl, re.I)
    if not match:
        raise Exception(f"{full} không có location")

    return match.group(2).replace("s3a://", "s3a://")

def deduplicate_df(df):
    return df.dropDuplicates()

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, desc

def deduplicate_by_key(df, keys, order_col):
    window = Window.partitionBy(*keys).orderBy(desc(order_col))

    return df.withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .drop("rn")


def get_columns(df):
    return [field.name.lower() for field in df.schema.fields]


def auto_deduplicate(df):
    cols = get_columns(df)

    id_cols = ["id", "uuid", "pk"]
    ts_cols = ["updated_at_ts", "updated_at", "last_updated", "crawled_at_ts"]

    found_id = next((c for c in id_cols if c in cols), None)
    found_ts = next((c for c in ts_cols if c in cols), None)

    if found_id and found_ts:
        print(f"🔑 Using dedup by ({found_id}, {found_ts})")
        return deduplicate_by_key(df, [found_id], found_ts)

    print("🧹 Using dropDuplicates()")
    return df.dropDuplicates()



def cast_value(value, pa_type):
    if value is None:
        return None

    if pa.types.is_string(pa_type):
        return str(value)
    if pa.types.is_int64(pa_type):
        return int(value)
    if pa.types.is_boolean(pa_type):
        return bool(value)
    if pa.types.is_list(pa_type):
        return [cast_value(v, pa_type.value_type) for v in value]  # recursive
    if pa.types.is_struct(pa_type):
        return {k: cast_value(v, pa_type[k].type) for k, v in value.items()}
    return value


def write_parquet_file(rows, columns, file_path):
    records = [
        {col: cast_value(val, schema.field(col).type) for col, val in zip(columns, row)}
        for row in rows
    ]

    table = pa.Table.from_pylist(records, schema=schema)

    pq.write_table(
        table,
        file_path,
        compression="snappy",
        row_group_size=100000
    )



def process_table(spark, cursor, schema, table):
    print(f"\n🚀 Processing {schema}.{table}")

    location = get_location(cursor, schema, table)

    print(f"📦 Location: {location}")

    # read parquet
    df = spark.read.parquet(location)

    print(f"   rows before: {df.count()}")

    df_clean = auto_deduplicate(df)

    print(f"   rows after: {df_clean.count()}")

    # write lại (overwrite)
    df_clean.write \
        .mode("overwrite") \
        .parquet(location)

    print(f"✅ Done {table}")
    

schema = "crm_silver"
table = "sales_accounts"

cursor = get_trino_cursor(schema)
process_table(spark, cursor, schema, table)

# tables = get_tables(cursor, schema)

# for table in tables:
#     try:
#         process_table(spark, cursor, schema, table)
#     except Exception as e:
#         print(f"❌ ERROR {schema}.{table}: {e}")