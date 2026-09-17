# -*- coding: utf-8 -*-

import sys
from pyspark.sql import SparkSession
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


def create_spark():
    return (
        SparkSession.builder
        .appName("GenericSparkJob")
        .enableHiveSupport()
        .getOrCreate()
    )


def main():
    if len(sys.argv) < 2:
        print("Usage: spark-submit main_submit.py <job_file.py>")
        sys.exit(1)

    job_file = sys.argv[1]

    spark = create_spark()

    # Tạo context global để inject spark vào
    job_globals = {
        "spark": spark
    }
    logging.info(f"Running job file: {job_file}")

    # Đọc file job
    with open(job_file, "r") as f:
        job_code = f.read()

    # Execute code
    exec(job_code, job_globals)

    spark.stop()
    logging.info("Job finished successfully.")


if __name__ == "__main__":
    main()