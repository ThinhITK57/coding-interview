
df = spark.range(10)
df.show()

df = spark.sql("SELECT * FROM default.events LIMIT 10")
df.show()


df.write.mode("overwrite").parquet("s3a://bidatalake/hivetables/data/exmples/events5/")

df.write.mode("append") \
    .option("header", "true") \
    .csv("s3a://bidatalake/examples/nextgen-bi/reports/")

df.write.mode("overwrite") \
    .format("parquet") \
    .option("path", "s3a://bidatalake/hivetables/data/exmples/events4/") \
    .option("external", "true") \
    .option("s3a.acl.default", "BucketOwnerFullControl") \
    .saveAsTable("default.events4")