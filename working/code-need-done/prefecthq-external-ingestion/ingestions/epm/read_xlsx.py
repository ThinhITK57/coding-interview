from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("S3ATest")
    .master("local[2]")

    .config("spark.driver.memory", "2g")

    # Hadoop S3A
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    # MinIO endpoint
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://127.0.0.1:9000"
    )

    # Credentials
    .config(
        "spark.hadoop.fs.s3a.access.key",
        "minioadmin"
    )

    .config(
        "spark.hadoop.fs.s3a.secret.key",
        "minioadmin123"
    )

    # MinIO uses path-style access
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )

    # Local DEV: HTTP, not HTTPS
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false"
    )

    .getOrCreate()
)

df = spark.createDataFrame(
    [
        (1, "hello"),
        (2, "world"),
    ],
    ["id", "value"]
)

df.write \
    .mode("overwrite") \
    .parquet(
        "s3a://lakehouse/test_s3a"
    )

print("WRITE SUCCESS")

print("=== READ ===")

spark.read \
    .parquet("s3a://lakehouse/test_s3a") \
    .show()

spark.stop()