%livy.pyspark

spark.sql("create database if not exists bi_gold")

spark.sql("use bi_gold")

# spark.sql("drop  table IF EXISTS bi_gold.fct_ticket1")

