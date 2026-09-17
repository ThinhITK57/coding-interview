"""Kiem tra moi truong truoc khi chay cold-start.

    python common\\check_env.py

Chay het cac buoc, khong dung o loi dau tien, roi in bang tong ket. Buoc 3 la
quan trong nhat: toan bo co che doc theo contract dua tren viec Spark serialize
lai object JSON thanh text khi field khai StringType. Hanh vi nay da duoc kiem
chung tren Spark 3.5.1 nhung chua kiem tren 2.3.2.
"""
import json
import os
import shutil
import sys
import tempfile
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RESULTS = []


def step(name):
    def deco(fn):
        def wrapper(*args, **kwargs):
            try:
                detail = fn(*args, **kwargs)
                RESULTS.append((name, "OK", detail or ""))
                print(f"[OK ] {name}: {detail or ''}")
                return True
            except Exception as exc:
                RESULTS.append((name, "LOI", f"{type(exc).__name__}: {exc}"))
                print(f"[LOI] {name}: {type(exc).__name__}: {exc}")
                traceback.print_exc(limit=2)
                return False
        return wrapper
    return deco


@step("1. Phien ban Python / PySpark / Java")
def check_versions():
    import pyspark
    java_home = os.getenv("JAVA_HOME", "(chua dat)")
    return (f"python={sys.version.split()[0]} pyspark={pyspark.__version__} "
            f"JAVA_HOME={java_home}")


@step("2. Contract cua ca 5 endpoint")
def check_contracts():
    from transform.schema_contract import SchemaContract
    with open("config.json", encoding="utf-8") as handle:
        endpoints = [e["name"] for e in json.load(handle)["api"]["endpoints"]]
    out = []
    for name in endpoints:
        if not SchemaContract.has_contract(name):
            raise RuntimeError(f"thieu contract cho '{name}'")
        out.append(f"{name}={len(SchemaContract.load(name))}")
    return " ".join(out)


@step("3. Spark doc object JSON vao cot StringType (GIA DINH COT LOI)")
def check_jackson(spark):
    from pyspark.sql.types import StringType, StructField, StructType

    tmp = tempfile.mkdtemp()
    try:
        path = os.path.join(tmp, "probe.json")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write('{"money":{"currency":"VND","value":1234.56},'
                         '"ref":{"id":"/X/1"},"num":42,"flag":true}\n')

        schema = StructType([StructField(c, StringType(), True)
                             for c in ("money", "ref", "num", "flag")])
        row = spark.read.schema(schema).json(path).collect()[0]

        if "currency" not in (row["money"] or ""):
            raise RuntimeError(
                f"Spark KHONG giu nguyen text JSON. money={row['money']!r}. "
                "Co che unwrap se khong chay tren ban Spark nay."
            )
        return f"money={row['money']} ref={row['ref']} num={row['num']}"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@step("4. Conform + ep kieu tren du lieu staging")
def check_conform(spark):
    from transform.contract_conformer import assert_matches_contract, read_conformed
    from transform.schema_contract import SchemaContract

    staging = os.path.join("data", "staging", "clarizen", "projects")
    if not os.path.isdir(staging) or not os.listdir(staging):
        return "bo qua - khong co du lieu staging de thu"

    contract = SchemaContract.load("projects")
    df = read_conformed(spark, staging, contract).drop(SchemaContract.CORRUPT_COL)
    assert_matches_contract(df, contract)
    return f"{df.count()} ban ghi, {len(df.columns)} cot, schema khop contract"


@step("5. Ghi va doc lai parquet tren MinIO qua s3a")
def check_s3a(spark):
    from pyspark.sql import functions as F

    uri = os.getenv("WAREHOUSE_URI", "s3a://lakehouse/warehouse")
    path = f"{uri.rstrip('/')}/_preflight_check"
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
    df.write.mode("overwrite").parquet(path)
    n = spark.read.parquet(path).count()
    return f"{path} ghi/doc OK ({n} dong)"


@step("6. Sinh DDL Trino tu contract")
def check_ddl():
    from storage.trino_ddl_generator import TrinoDDLGenerator

    gen = TrinoDDLGenerator(
        base_s3_location=os.getenv("DDL_LOCATION_BASE",
                                   os.getenv("WAREHOUSE_URI", "s3a://lakehouse/warehouse"))
    )
    ddl = gen.generate_from_contract("projects")
    loc = [l.strip() for l in ddl.splitlines() if "external_location" in l]
    return f"{ddl.count('DECIMAL(18,2)')} cot DECIMAL | {loc[0] if loc else ''}"


def main():
    print("=" * 70)
    print(" PREFLIGHT COLD-START")
    print(f" cwd = {os.getcwd()}")
    print("=" * 70)

    check_versions()
    check_contracts()

    from transform.spark_session import SparkSessionFactory
    spark = SparkSessionFactory.create(app_name="preflight", master="local[2]")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        jackson_ok = check_jackson(spark)
        if jackson_ok:
            check_conform(spark)
        check_s3a(spark)
    finally:
        spark.stop()

    check_ddl()

    print("\n" + "=" * 70)
    failed = [r for r in RESULTS if r[1] != "OK"]
    for name, status, detail in RESULTS:
        print(f" {status:4s} {name}")
        if status != "OK":
            print(f"        {detail}")
    print("=" * 70)
    if failed:
        print(f" {len(failed)}/{len(RESULTS)} buoc LOI - sua truoc khi chay cold-start.")
        sys.exit(1)
    print(" Tat ca OK - co the chay cold-start.")


if __name__ == "__main__":
    main()
