# -*- coding: utf-8 -*-
import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--sql", required=True)
parser.add_argument("--db", required=True)
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("RunSQLFile")
    .enableHiveSupport()
    .getOrCreate()
)

# set database trước
spark.sql(f"USE {args.db}")

with open(args.sql, "r") as f:
    sql_script = f.read()

for stmt in sql_script.split(";"):
    stmt = stmt.strip()
    if stmt:
        spark.sql(stmt)

spark.stop()