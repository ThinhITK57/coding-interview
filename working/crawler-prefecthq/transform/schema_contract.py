import os
import re
import json
import logging

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from transform.json_flattener import JSONFlattener

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ColumnSpec:
    src_name: str
    name: str
    logical: str

    @property
    def is_decimal(self):
        return self.logical.startswith("DECIMAL")

    @property
    def decimal_precision_scale(self):
        m = re.match(r"DECIMAL\((\d+),(\d+)\)", self.logical)
        return (int(m.group(1)), int(m.group(2))) if m else (18, 2)

    @property
    def trino_type(self):
        return "VARCHAR" if self.logical == "STRING" else self.logical

    @property
    def spark_sql_type(self):
        """Kieu dung trong CREATE TABLE cua Spark SQL (Zeppelin).

        Khac Trino o cho Spark dung STRING thay cho VARCHAR; cac kieu con lai
        (DOUBLE, DECIMAL(p,s), DATE, BOOLEAN) viet giong nhau.
        """
        return self.logical

    def spark_type(self):
        from pyspark.sql.types import (
            StringType, DoubleType, BooleanType, DateType, DecimalType
        )
        if self.logical == "STRING":
            return StringType()
        if self.logical == "DOUBLE":
            return DoubleType()
        if self.logical == "BOOLEAN":
            return BooleanType()
        if self.logical == "DATE":
            return DateType()
        precision, scale = self.decimal_precision_scale
        return DecimalType(precision, scale)


class SchemaContract:
    """Kieu du lieu cua 5 bang lay tu data_type/*.sql.

    File .sql la nguon su that duy nhat: schema Parquet ma Spark ghi ra va DDL
    Trino deu sinh tu day, nen hai ben khong the lech nhau. Xem them
    TrinoDDLGenerator.generate_from_contract().
    """

    # Ten endpoint trong config.json  ->  data_type/<ten>_dataType.sql
    # Endpoint khong phai luc nao cung trung ten thuc the Clarizen: 'bsc' query
    # typeName 'Objective', nen phai anh xa tuong minh - neu khong se khong tim
    # thay contract va am tham roi ve nhanh infer schema.
    TABLE_MAP = {
        "projects": "project",
        "project": "project",
        "tasks": "task",
        "task": "task",
        "bsc": "objective",
        "objectives": "objective",
        "objective": "objective",
        "targets": "target",
        "target": "target",
        "c_assignments": "c_assignment",
        "c_assignment": "c_assignment",
        "assignments": "c_assignment",
    }

    DEFAULT_CONTRACT_DIR = os.getenv(
        "CONTRACT_DIR",
        "./data_type_pruned" if os.path.exists("./data_type_pruned") else "./data_type"
    )

    # "Name TYPE" - chiu duoc ca hai dinh dang dang ton tai trong data_type/:
    # mang JSON cac chuoi (project_dataType.sql) va khoi ngoac don (4 file con lai)
    _LINE_RE = re.compile(
        r"^([A-Za-z0-9_]+)\s+"
        r"(STRING|DOUBLE|BOOLEAN|DATE|DECIMAL\(\s*\d+\s*,\s*\d+\s*\))$",
        re.IGNORECASE
    )

    # Cot ky thuat them vao moi bang, khong nam trong contract
    RAW_PAYLOAD_COL = "_raw_payload"
    BATCH_ID_COL = "_batch_id"
    INGEST_TS_COL = "_ingest_timestamp"
    CORRUPT_COL = "_corrupt_record"

    def __init__(self, table_name: str, columns: List[ColumnSpec]):
        self.table_name = table_name
        self.columns = columns

    # ------------------------------------------------------------------ load

    @classmethod
    def resolve_path(cls, table_name: str, contract_dir: Optional[str] = None) -> str:
        contract_dir = contract_dir or cls.DEFAULT_CONTRACT_DIR
        key = table_name.lower()
        base = cls.TABLE_MAP.get(key, key)
        return os.path.join(contract_dir, f"{base}_dataType.sql")

    @classmethod
    def has_contract(cls, table_name: str, contract_dir: Optional[str] = None) -> bool:
        return os.path.isfile(cls.resolve_path(table_name, contract_dir))

    @classmethod
    def load(cls, table_name: str, contract_dir: Optional[str] = None) -> "SchemaContract":
        path = cls.resolve_path(table_name, contract_dir)
        if not os.path.isfile(path):
            raise FileNotFoundError(
                f"Khong tim thay contract cho bang '{table_name}': {path}"
            )

        flattener = JSONFlattener()
        columns: List[ColumnSpec] = []
        seen: Dict[str, str] = {}

        with open(path, encoding="utf-8") as handle:
            for lineno, line in enumerate(handle, start=1):
                # Bo dau phay cuoi dong va dau nhay bao quanh (dinh dang mang JSON)
                token = line.strip().rstrip(",").strip().strip('"').strip()
                if not token or token in ("(", ")", "[", "]"):
                    continue

                match = cls._LINE_RE.match(token)
                if not match:
                    logger.warning(json.dumps({
                        "event": "contract_line_skipped",
                        "file": path,
                        "line": lineno,
                        "content": token[:120]
                    }))
                    continue

                src_name = match.group(1)
                logical = match.group(2).upper().replace(" ", "")
                name = flattener._to_snake_case(src_name)

                if name in seen:
                    raise ValueError(
                        f"{path}:{lineno} - cot '{src_name}' va '{seen[name]}' cung "
                        f"cho ra ten snake_case '{name}'"
                    )
                seen[name] = src_name
                columns.append(ColumnSpec(src_name=src_name, name=name, logical=logical))

        if not columns:
            raise ValueError(f"Khong doc duoc cot nao tu {path}")

        logger.info(json.dumps({
            "event": "schema_contract_loaded",
            "table": table_name,
            "file": path,
            "column_count": len(columns)
        }))
        return cls(table_name=table_name, columns=columns)

    # ---------------------------------------------------------------- schemas

    def read_schema(self):
        """StructType toan StringType de doc JSON ma KHONG infer.

        JacksonParser cua Spark khi gap token khong phai chuoi tren mot field
        StringType se serialize lai cay JSON con thanh text. Nho vay mot schema
        phang toan string nuot duoc ca scalar lan object ({currency,value},
        {id}, {unit,value}) ma khong can biet truoc cot nao la object.
        """
        from pyspark.sql.types import StructType, StructField, StringType

        fields = [
            StructField(col.src_name, StringType(), True) for col in self.columns
        ]
        # 'id' la URI dinh danh cua Clarizen, khong nam trong contract nhung luon co
        fields.append(StructField("id", StringType(), True))
        fields.append(StructField(self.CORRUPT_COL, StringType(), True))
        return StructType(fields)

    def spark_schema(self):
        """Schema dung cua DataFrame sau khi conform - dung de assert."""
        from pyspark.sql.types import StructType, StructField

        return StructType([
            StructField(col.name, col.spark_type(), True) for col in self.columns
        ])

    def trino_columns(self) -> List[Dict[str, str]]:
        return [{"name": col.name, "type": col.trino_type} for col in self.columns]

    @property
    def names(self) -> List[str]:
        return [col.name for col in self.columns]

    @property
    def src_names(self) -> List[str]:
        return [col.src_name for col in self.columns]

    def __len__(self):
        return len(self.columns)

    def __repr__(self):
        return f"<SchemaContract {self.table_name} cols={len(self.columns)}>"
