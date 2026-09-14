import os
import json
import logging

from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class SparkSQLDDLGenerator:
    """Sinh CREATE TABLE theo cu phap Spark SQL de chay tren Zeppelin.

    Khac ban Trino o ba diem:
      - STRING thay cho VARCHAR, TIMESTAMP thay cho TIMESTAMP(6)
      - USING PARQUET + LOCATION thay cho WITH (format=..., external_location=...)
      - Cu phap datasource (USING PARQUET) doi hoi cot partition PHAI nam trong
        danh sach cot, nguoc voi cu phap Hive kieu cu

    Cung doc tu SchemaContract nhu TrinoDDLGenerator nen hai ban DDL khong the
    mo ta hai schema khac nhau.
    """

    METADATA_COLUMNS = [
        ("_raw_payload", "STRING", True),      # (ten, kieu, chi cho lop raw)
        ("_batch_id", "STRING", False),
        ("_ingest_timestamp", "TIMESTAMP", False),
    ]

    def __init__(
            self,
            personal_schema: str = "personal_raw",
            clean_schema: str = "global_clean",
            base_location: str = "/opt/datasets/crawlers/vcs/clarizen/data",
            partitioned: bool = False
    ):
        self.personal_schema = personal_schema
        self.clean_schema = clean_schema
        self.base_location = base_location.rstrip("/")
        # He thong dich chua ho tro partition -> ingest_date la cot thuong nam
        # trong file parquet, khong phai thu muc ingest_date=...
        self.partitioned = partitioned

    def _table_ddl(
            self,
            schema_name: str,
            table_name: str,
            columns: List[Dict[str, str]],
            partition_col: str,
            include_raw_payload: bool
    ) -> str:
        table = f"{schema_name}.{table_name.lower()}"
        location = f"{self.base_location}/{schema_name}/{table_name.lower()}"

        existing = {c["name"] for c in columns}
        defs = [
            f"    {c['name']} {c['type']}"
            for c in columns if c["name"] != partition_col
        ]
        for name, dtype, raw_only in self.METADATA_COLUMNS:
            if raw_only and not include_raw_payload:
                continue
            if name not in existing:
                defs.append(f"    {name} {dtype}")

        # Cu phap USING PARQUET doi hoi cot partition cung phai nam trong danh
        # sach cot. Khi khong partition thi no chi la mot cot DATE binh thuong.
        defs.append(f"    {partition_col} DATE")

        ddl = (
            f"CREATE DATABASE IF NOT EXISTS {schema_name};\n\n"
            f"DROP TABLE IF EXISTS {table};\n\n"
            f"CREATE TABLE {table} (\n"
            + ",\n".join(defs)
            + "\n)\n"
            f"USING PARQUET\n"
        )
        if self.partitioned:
            ddl += f"PARTITIONED BY ({partition_col})\n"
        ddl += f"LOCATION '{location}';\n"

        if self.partitioned:
            ddl += ("\n-- Nap cac thu muc partition da upload tay len\n"
                    f"MSCK REPAIR TABLE {table};\n")
        return ddl

    def generate_all(
            self,
            table_name: str,
            contract,
            partition_col: str = "ingest_date"
    ) -> str:
        columns = [
            {"name": spec.name, "type": spec.spark_sql_type}
            for spec in contract.columns
        ]
        note = (
            "-- Upload parquet len LOCATION truoc, sau do MSCK REPAIR TABLE.\n"
            if self.partitioned else
            f"-- Bang KHONG partition: dat thang cac file .parquet vao LOCATION.\n"
            f"-- '{partition_col}' la cot DATE binh thuong nam trong file.\n"
        )
        header = (
            f"-- Spark SQL DDL cho bang '{table_name.lower()}' "
            f"({len(columns)} cot tu data_type/)\n"
            f"-- Chay tren Zeppelin (%spark.sql).\n"
            + note + "\n"
        )
        return (
            header
            + self._table_ddl(self.personal_schema, table_name, columns,
                              partition_col, include_raw_payload=True)
            + "\n"
            + self._table_ddl(self.clean_schema, table_name, columns,
                              partition_col, include_raw_payload=False)
        )

    def export_sql_file(
            self,
            output_path: str,
            table_name: str,
            contract=None,
            contract_dir: Optional[str] = None,
            partition_col: str = "ingest_date"
    ) -> str:
        if contract is None:
            from transform.schema_contract import SchemaContract
            contract = SchemaContract.load(table_name, contract_dir=contract_dir)

        if os.path.isdir(output_path) or not output_path.endswith(".sql"):
            os.makedirs(output_path, exist_ok=True)
            file_path = os.path.join(
                output_path, f"{table_name.lower()}_spark_ddl.sql"
            )
        else:
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            file_path = output_path

        with open(file_path, "w", encoding="utf-8") as handle:
            handle.write(self.generate_all(table_name, contract, partition_col))

        logger.info(json.dumps({
            "event": "spark_sql_ddl_exported",
            "file_path": file_path,
            "table_name": table_name,
            "column_count": len(contract)
        }))
        return file_path
