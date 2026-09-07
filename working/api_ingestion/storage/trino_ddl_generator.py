import os
import json
import logging
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class TrinoDDLGenerator:
    """
    Auto-generates Trino / Hive DDL SQL scripts from Spark schemas or field lists.

    Deep module design:
        Interface (small): generate_ddl(table_name, fields, ...), export_sql_file(path, ...)
        Implementation (deep): Maps Spark 2.3.2 datatypes to Trino SQL types,
        generates CREATE SCHEMA, CREATE TABLE IF NOT EXISTS with format='PARQUET',
        partitioned_by, external_location, and generates partition sync procedures.
    """

    SPARK_TO_TRINO_TYPE_MAP = {
        "string": "VARCHAR",
        "int": "INTEGER",
        "integer": "INTEGER",
        "bigint": "BIGINT",
        "long": "BIGINT",
        "short": "SMALLINT",
        "byte": "TINYINT",
        "double": "DOUBLE",
        "float": "REAL",
        "boolean": "BOOLEAN",
        "timestamp": "TIMESTAMP",
        "date": "DATE",
        "binary": "VARBINARY",
    }

    def __init__(
        self,
        catalog: str = "hive",
        personal_schema: str = "personal_raw",
        clean_schema: str = "global_clean",
        base_s3_location: str = "s3a://lakehouse/warehouse",
    ):
        """Initialize generator with Trino catalog and schema targets.

        Args:
            catalog: Trino catalog name (default: "hive").
            personal_schema: Schema name for raw sandbox DB 1 (default: "personal_raw").
            clean_schema: Schema name for curated production DB 2 (default: "global_clean").
            base_s3_location: Base S3/MinIO location for external tables.
        """
        self.catalog = catalog
        self.personal_schema = personal_schema
        self.clean_schema = clean_schema
        self.base_s3_location = base_s3_location.rstrip("/")

    def map_type(self, spark_type_str: str) -> str:
        """Map a Spark datatype string to Trino SQL type."""
        clean_type = spark_type_str.lower().strip()

        if clean_type in self.SPARK_TO_TRINO_TYPE_MAP:
            return self.SPARK_TO_TRINO_TYPE_MAP[clean_type]

        if clean_type.startswith("decimal"):
            return clean_type.upper()

        if clean_type.startswith("array") or clean_type.startswith("struct") or clean_type.startswith("map"):
            # For complex types in raw/clean staging, represent as VARCHAR (JSON) or native VARCHAR
            return "VARCHAR"

        return "VARCHAR"

    def extract_columns(self, schema_or_fields: Union[Any, List[str], List[Dict[str, str]]]) -> List[Dict[str, str]]:
        """Extract column definitions from Spark DataFrame schema or field list."""
        columns = []

        # Case 1: Spark StructType
        if hasattr(schema_or_fields, "fields"):
            for field in schema_or_fields.fields:
                col_name = field.name.lower()
                trino_type = self.map_type(field.dataType.simpleString())
                columns.append({"name": col_name, "type": trino_type})
            return columns

        # Case 2: List of strings e.g. ["SYSID", "Name", "PercentCompleted"]
        if isinstance(schema_or_fields, list) and schema_or_fields and isinstance(schema_or_fields[0], str):
            for field_name in schema_or_fields:
                col_name = field_name.lower().replace(".", "_").replace(" ", "_")
                # Default type to VARCHAR if only name is provided
                columns.append({"name": col_name, "type": "VARCHAR"})
            return columns

        # Case 3: List of dicts e.g. [{"name": "SYSID", "type": "string"}]
        if isinstance(schema_or_fields, list) and schema_or_fields and isinstance(schema_or_fields[0], dict):
            for col in schema_or_fields:
                col_name = col.get("name", "").lower()
                t_type = self.map_type(col.get("type", "string"))
                columns.append({"name": col_name, "type": t_type})
            return columns

        return columns

    def generate_personal_raw_ddl(
        self,
        table_name: str,
        columns: List[Dict[str, str]],
        partition_col: str = "ingest_date",
    ) -> str:
        """Generate DDL for Trino DB 1 (personal_raw sandbox)."""
        table_full_name = f"{self.catalog}.{self.personal_schema}.{table_name.lower()}"
        location = f"{self.base_s3_location}/{self.personal_schema}/{table_name.lower()}"

        col_defs = []
        for col in columns:
            if col["name"] != partition_col:
                col_defs.append(f"    {col['name']} {col['type']}")

        # Ensure metadata columns exist in raw sandbox
        col_names = {c["name"] for c in columns}
        if "_raw_payload" not in col_names:
            col_defs.append("    _raw_payload VARCHAR")
        if "_batch_id" not in col_names:
            col_defs.append("    _batch_id VARCHAR")
        if "_ingest_timestamp" not in col_names:
            col_defs.append("    _ingest_timestamp TIMESTAMP")

        # Partition column at the end
        col_defs.append(f"    {partition_col} DATE")

        col_defs_str = ",\n".join(col_defs)
        ddl = f"""-- ====================================================================
-- TRINO DB 1: PERSONAL RAW SANDBOX (Dữ liệu thô thử nghiệm thuật toán)
-- ====================================================================
CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.personal_schema};

CREATE TABLE IF NOT EXISTS {table_full_name} (
{col_defs_str}
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['{partition_col}'],
    external_location = '{location}'
);
"""
        return ddl

    def generate_global_clean_ddl(
        self,
        table_name: str,
        columns: List[Dict[str, str]],
        partition_col: str = "ingest_date",
    ) -> str:
        """Generate DDL for Trino DB 2 (global_clean curated warehouse)."""
        table_full_name = f"{self.catalog}.{self.clean_schema}.{table_name.lower()}"
        location = f"{self.base_s3_location}/{self.clean_schema}/{table_name.lower()}"

        col_defs = []
        for col in columns:
            if col["name"] != partition_col:
                col_defs.append(f"    {col['name']} {col['type']}")

        col_names = {c["name"] for c in columns}
        if "_batch_id" not in col_names:
            col_defs.append("    _batch_id VARCHAR")
        if "_ingest_timestamp" not in col_names:
            col_defs.append("    _ingest_timestamp TIMESTAMP")

        # Partition column at the end
        col_defs.append(f"    {partition_col} DATE")

        col_defs_str = ",\n".join(col_defs)
        ddl = f"""-- ====================================================================
-- TRINO DB 2: GLOBAL CLEAN WAREHOUSE (Dữ liệu sạch chuẩn hóa qua Hive Metastore)
-- ====================================================================
CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.clean_schema};

CREATE TABLE IF NOT EXISTS {table_full_name} (
{col_defs_str}
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['{partition_col}'],
    external_location = '{location}'
);
"""
        return ddl

    def generate_partition_sync_command(self, table_name: str) -> str:
        """Generate Trino partition repair/sync commands."""
        return f"""-- ====================================================================
-- TRINO PARTITION SYNC (Chạy sau khi Spark ghi dữ liệu mới)
-- ====================================================================
CALL {self.catalog}.system.sync_partition_metadata('{self.personal_schema}', '{table_name.lower()}', 'ADD');
CALL {self.catalog}.system.sync_partition_metadata('{self.clean_schema}', '{table_name.lower()}', 'ADD');
"""

    def generate_all_ddl(
        self,
        table_name: str,
        schema_or_fields: Any,
        partition_col: str = "ingest_date",
    ) -> str:
        """Generate combined Trino DDL script for both DB1, DB2 and partition sync."""
        columns = self.extract_columns(schema_or_fields)

        db1_ddl = self.generate_personal_raw_ddl(table_name, columns, partition_col)
        db2_ddl = self.generate_global_clean_ddl(table_name, columns, partition_col)
        sync_cmd = self.generate_partition_sync_command(table_name)

        return f"{db1_ddl}\n{db2_ddl}\n{sync_cmd}"

    def export_sql_file(
        self,
        output_path: str,
        table_name: str,
        schema_or_fields: Any,
        partition_col: str = "ingest_date",
    ) -> str:
        """Generate DDL and write to a .sql file for direct execution via trino.exe.

        Args:
            output_path: Directory or full path to the .sql file.
            table_name: Entity/table name.
            schema_or_fields: Spark schema or field list.
            partition_col: Partition column name.

        Returns:
            str: Path to the written SQL file.
        """
        if os.path.isdir(output_path) or not output_path.endswith(".sql"):
            os.makedirs(output_path, exist_ok=True)
            file_path = os.path.join(output_path, f"{table_name.lower()}_trino_ddl.sql")
        else:
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            file_path = output_path

        sql_content = self.generate_all_ddl(table_name, schema_or_fields, partition_col)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(sql_content)

        logger.info(json.dumps({
            "event": "trino_ddl_exported",
            "file_path": file_path,
            "table_name": table_name,
        }))

        return file_path
