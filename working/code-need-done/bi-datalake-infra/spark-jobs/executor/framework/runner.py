import sys
import json
import os
from pyspark.sql import SparkSession
import uuid

import psutil
import os

def print_driver_memory():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    rss_mb = mem_info.rss / (1024 * 1024)
    print(f"📊 Driver Memory Used: {rss_mb:.2f} MB")
    
def print_real_spark_memory(spark):
    sc = spark.sparkContext
    runtime = sc._gateway.jvm.java.lang.Runtime.getRuntime()
    total_mem = runtime.totalMemory() / (1024 * 1024)
    free_mem = runtime.freeMemory() / (1024 * 1024)
    used_mem = total_mem - free_mem
    
    print(f"☕ JVM Allocated: {total_mem:.2f} MB")
    print(f"🔥 JVM Used: {used_mem:.2f} MB")
    
    if os.path.exists('/sys/fs/cgroup/memory/memory.usage_in_bytes'):
        with open('/sys/fs/cgroup/memory/memory.usage_in_bytes', 'r') as f:
            container_mem = int(f.read()) / (1024 * 1024)
            print(f"🐳 Container Total (Docker Stats): {container_mem:.2f} MB")
            
            
if len(sys.argv) < 2:
    print("Usage: runner.py <job_file> [json_params]")
    sys.exit(1)

file_path = os.path.abspath(sys.argv[1])
print(f"Running job: {file_path}")
params_raw = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}

# --- Tạo SparkSession với config từ spark-submit ---
jars_dir = "/opt/spark/jars_extra"

jars = [os.path.join(jars_dir, f) for f in os.listdir(jars_dir) if f.endswith(".jar")]
jars_str = ",".join(jars)

spark = (SparkSession.builder 
    .appName("lightweight-"+ str(uuid.uuid4())[:8]) 
    # .config("spark.driver.memory", "512m") 
    # .config("spark.executor.memory", "512m") 
    .config("spark.sql.shuffle.partitions", "1") 
    .config("spark.default.parallelism", "1") 
    .config("spark.sql.adaptive.enabled", "true") 
    .config("spark.ui.enabled", "false") 
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.master.ui.enabled", "false")
    .config("spark.eventLog.enabled", "false") 
    .config("spark.hadoop.fs.s3a.committer.name", "directory")
    .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/spark_staging")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") 
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") 
    .config("spark.hadoop.fs.s3a.acl.default", "BucketOwnerFullControl")
    .config("spark.hadoop.fs.s3a.canned.acl", "BucketOwnerFullControl")
    .config("spark.hadoop.hadoop.proxyuser.admin.groups", "*")
    .config("spark.hadoop.hadoop.proxyuser.admin.hosts", "*")
    .config("spark.sql.catalogImplementation", "hive")
    # .config("hive.metastore.uris", os.getenv("HIVE_ENDPOINT", "thrift://localhost:9083"))
    .config("spark.hadoop.hive.metastore.uris", os.getenv("HIVE_ENDPOINT", "thrift://localhost:9083"))
    .config("spark.sql.catalogImplementation", "hive") 
    .config("spark.sql.hive.metastore.version", "3.1.3")
    # .config("spark.sql.hive.metastore.jars", "maven")
    .config("spark.sql.hive.metastore.jars", "/opt/spark/jars_extra/*") 
    .config("spark.hadoop.hive.metastore.schema.verification", "false")
    .config("spark.hadoop.datanucleus.fixedDatastore", "true") 
    .getOrCreate())

spark.sparkContext.setLogLevel(os.getenv("LOG_LEVEL","WARN"))
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

broadcast_params = spark.sparkContext.broadcast(params_raw)
# import importlib
# job_module = importlib.import_module(file_path)

import importlib.util
module_name = os.path.splitext(os.path.basename(file_path))[0]
    
spec = importlib.util.spec_from_file_location(module_name, file_path)
job_module = importlib.util.module_from_spec(spec)

setattr(job_module, 'spark', spark)
setattr(job_module, "broadcast_params", broadcast_params)
setattr(job_module, "params", params_raw)

spec.loader.exec_module(job_module)


sc = spark.sparkContext
exec_metrics = sc._jsc.sc().getExecutorMemoryStatus()

print("🚀 Executor Memory Metrics:")
metrics_dict = sc._gateway.jvm.scala.collection.JavaConverters.mapAsJavaMapConverter(exec_metrics).asJava()

for executor, mem in metrics_dict.items():
    # mem[0] là Max Memory, mem[1] là Remaining Memory
    max_mem = mem._1()
    rem_mem = mem._2()
    used_mem = (max_mem - rem_mem) / (1024 * 1024)
    print(f" - Executor {executor}: Used {used_mem:.2f} MB / Total {max_mem/(1024*1024):.2f} MB")
    
print_driver_memory()

print_real_spark_memory(spark)


spark.stop()
