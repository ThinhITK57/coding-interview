import os

from config.env_loader import load_env_file

# Load .env
load_env_file()

from pyspark.sql import SparkSession


# ============================================================
# MINIO CONFIG
# ============================================================

endpoint = os.getenv("MINIO_ENDPOINT")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")
bucket = os.getenv("MINIO_BUCKET")


print("================================")
print("MINIO S3A TEST")
print("================================")

print("Endpoint :", endpoint)
print("Bucket   :", bucket)
print("Access   :", bool(access_key))
print("Secret   :", bool(secret_key))


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("MinIO-S3A-Test")
    .master("local[2]")

    # MinIO endpoint
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        endpoint
    )

    # MinIO credentials
    .config(
        "spark.hadoop.fs.s3a.access.key",
        access_key
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        secret_key
    )

    # MinIO requires path-style access
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )

    # S3A implementation
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    .getOrCreate()
)


# ============================================================
# TEST DATA
# ============================================================

df = spark.createDataFrame(
    [
        (1, "test"),
        (2, "hello"),
        (3, "minio"),
    ],
    [
        "id",
        "name"
    ]
)


print("")
print("Data:")
df.show()


# ============================================================
# WRITE TO MINIO
# ============================================================

output_path = "s3a://{}/test_s3a".format(bucket)

print("")
print("Output:", output_path)
print("Writing Parquet to MinIO...")


df.write \
    .mode("overwrite") \
    .parquet(output_path)


print("")
print("================================")
print("WRITE SUCCESS")
print("================================")


# ============================================================
# READ BACK FROM MINIO
# ============================================================

print("")
print("Reading Parquet back from MinIO...")

df_check = spark.read.parquet(output_path)

df_check.show()

print("READ SUCCESS")


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()

print("")
print("================================")
print("S3A TEST COMPLETED")
print("================================")