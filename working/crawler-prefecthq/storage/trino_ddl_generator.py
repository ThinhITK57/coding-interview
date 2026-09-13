import os 
import json
import logging

from typing import Any, Dict, List, Optional, Union
logger = logging.getLogger(__name__)

class TrinoDDLGenerator:
    SPARK_TO_TRINO_TYPE_MAP = {
        "string": "VARCHAR",
        "int": "INTEGER",
        "integer": "INTEGER",
        "bigint": "BIGINT",
        "short": "SMALLINT",
        "long": "BIGINT",
        "byte": "TINYINT",
        "double": "DOUBLE",
        "float": "REAL",
        "boolean": "BOOLEAN",
        "timestamp": "TIMESTAMP",
        "date": "DATE",
        "binary": "VARBINARY"
    }

    def __init__(
            self,
            catalog: str = "hive",
            personal_schema: str = "personal_raw",
            clean_schema: str = "global_clean",
            base_s3_location: str = "s3a://lakehouse/warehouse",
            partitioned: bool = False
            ):
        self.catalog = catalog
        self.personal_schema = personal_schema
        self.clean_schema = clean_schema
        self.base_s3_location = base_s3_location.rstrip("/")
        # He thong dich chua ho tro partition -> ingest_date la cot thuong
        self.partitioned = partitioned

    def _with_clause(self, location: str, partition_col: str) -> str:
        parts = ["    format = 'PARQUET'"]
        if self.partitioned:
            parts.append(f"    partitioned_by = ARRAY['{partition_col}']")
        parts.append(f"    external_location = '{location}'")
        return "WITH (\n" + ",\n".join(parts) + "\n)"


    def map_type(self, spark_type_str: str) -> str:
        clean_type = spark_type_str.lower().strip()

        if clean_type in self.SPARK_TO_TRINO_TYPE_MAP:
            return self.SPARK_TO_TRINO_TYPE_MAP[clean_type]

        if clean_type.startswith("decimal"):
            return clean_type.upper()

        if clean_type.startswith("array") or clean_type.startswith("struct") or clean_type.startswith("map"):
            return "VARCHAR"

        return "VARCHAR"

    def extract_columns(self, schema_or_fields: Union[Any, List[str], List[Dict[str, str]]]) -> List[Dict[str, str]]:
        columns = []

        # SchemaContract: kieu lay thang tu data_type/*.sql. Uu tien nhanh nay -
        # DDL va Parquet cung sinh tu mot contract nen khong the lech nhau, thay
        # vi ca hai cung phai sinh tu mot schema Spark suy luan lai moi batch.
        if hasattr(schema_or_fields, "trino_columns"):
            return list(schema_or_fields.trino_columns())

        if hasattr(schema_or_fields, "fields"):
            for field in schema_or_fields.fields:
                col_name = field.name.lower()
                trino_type = self.map_type(field.dataType.simpleString())
                columns.append({"name": col_name, "type": trino_type})
            return columns

        # case list of strings
        if isinstance(schema_or_fields, list) and schema_or_fields and isinstance(schema_or_fields[0], str):
            for field_name in schema_or_fields:
                col_name = field_name.lower().replace(".", "_").replace(" ", "_")
                columns.append({"name": col_name, "type": "VARCHAR"})
            return columns

        if isinstance(schema_or_fields, list) and schema_or_fields and isinstance(schema_or_fields[0], dict):
            for col in schema_or_fields:
                col_name = col.get("name", "").lower()
                t_type = self.map_type(col.get("type", "string"))
                columns.append({"name": col_name, "type": t_type})
            return columns

        return columns

    def generate_personal_raw_ddl(self, table_name: str,
            columns: List[Dict[str, str]],
            partition_col: str = "ingest_date"
        )-> str:
        table_full_name = f"{self.catalog}.{self.personal_schema}.{table_name.lower()}"
        location = f"{self.base_s3_location}/{self.personal_schema}/{table_name.lower()}"

        col_defs = []
        for col in columns:
            if col["name"] != partition_col:
                col_defs.append(f"  {col['name']} {col['type']}")

        col_names = {c["name"] for c in columns}
        if "_raw_payload" not in col_names:
            col_defs.append("   _raw_payload VARCHAR")

        if "_batch_id" not in col_names:
            col_defs.append("   _batch_id VARCHAR")

        if "_ingest_timestamp" not in col_names:
            # TIMESTAMP tran trong Trino la TIMESTAMP(3), trong khi Spark ghi
            # micro-giay -> phai khai bao precision tuong minh
            col_defs.append("   _ingest_timestamp TIMESTAMP(6)")

        col_defs.append(f"  {partition_col} DATE")

        col_defs_str = ",\n".join(col_defs)
        ddl = f"""
CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.personal_schema};

CREATE TABLE IF NOT EXISTS {table_full_name} (
{col_defs_str}
)
{self._with_clause(location, partition_col)};
"""
        return ddl

    def generate_global_clean_ddl(
            self,
            table_name: str,
            columns: List[Dict[str, str]],
            partition_col: str = "ingest_date",
    )-> str:
        table_full_name = f"{self.catalog}.{self.clean_schema}.{table_name.lower()}"
        location = f"{self.base_s3_location}/{self.clean_schema}/{table_name.lower()}"

        col_defs = []
        for col in columns:
            if col["name"] != partition_col:
                col_defs.append(f"  {col['name']} {col['type']}")

        col_names = {c['name'] for c in columns}
        if "_batch_id" not in col_names:
            col_defs.append("   _batch_id VARCHAR")

        if "_ingest_timestamp" not in col_names:
            col_defs.append("   _ingest_timestamp TIMESTAMP(6)")

        col_defs.append(f"  {partition_col} DATE")
        col_defs_str = ",\n".join(col_defs)

        ddl = f"""
CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.clean_schema};

CREATE TABLE IF NOT EXISTS {table_full_name} (
{col_defs_str}
)
{self._with_clause(location, partition_col)};
"""
        return ddl

    def generate_partition_sync_command(self, table_name: str) -> str:
        return f"""
        CALL {self.catalog}.system.sync_partition_metadata('{self.personal_schema}', '{table_name.lower()}', 'ADD');
        CALL {self.catalog}.system.sync_partition_metadata('{self.clean_schema}', '{table_name.lower()}', 'ADD');
        """

    def generate_all_dll(
            self,
            table_name: str,
            schema_or_fields: Any,
            partition_col: str = "ingest_date"
    )-> str:
        columns = self.extract_columns(schema_or_fields)
        db1_ddl = self.generate_personal_raw_ddl(table_name, columns, partition_col)
        db2_ddl = self.generate_global_clean_ddl(table_name, columns, partition_col)

        # sync_partition_metadata chi co nghia voi bang co partition
        if not self.partitioned:
            return f"{db1_ddl}\n{db2_ddl}"

        return f"{db1_ddl} \n {db2_ddl} \n {self.generate_partition_sync_command(table_name)}"

    def generate_from_contract(
            self,
            table_name: str,
            contract=None,
            contract_dir: Optional[str] = None,
            partition_col: str = "ingest_date"
    ) -> str:
        """Sinh DDL tu data_type/<bang>_dataType.sql thay vi tu schema Spark."""
        if contract is None:
            from transform.schema_contract import SchemaContract
            contract = SchemaContract.load(table_name, contract_dir=contract_dir)
        return self.generate_all_dll(table_name, contract, partition_col)

    def export_sql_file(
            self,
            output_path: str,
            table_name: str,
            schema_or_fields: Any,
            partition_col: str = 'ingest_date'
    )-> str:
        if os.path.isdir(output_path) or not output_path.endswith(".sql"):
            os.makedirs(output_path, exist_ok=True)
            file_path = os.path.join(output_path, f"{table_name.lower()}_trino_ddl.sql")
        else:
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            file_path = output_path

        sql_content = self.generate_all_dll(table_name, schema_or_fields, partition_col)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(sql_content)

        logger.info(json.dumps({
            "event": "trino_ddl_exported", 
            "file_path": file_path,
            "table_name": table_name
        }))

        return file_path
        