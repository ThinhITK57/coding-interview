
import os
import re
import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time
from datetime import datetime
from collections import defaultdict
from io import BytesIO
from pathlib import Path
from common.ambari_client import AmbariHDFSClient
from common.base_hdf_client import BaseHDFSClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def snake_case(text: str) -> str:
    text = str(text).strip().lower().replace("\n", " ")
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", "_", text)
    return text


def excel_col_name(idx: int) -> str:
    """Zero-based column index -> Excel letter (A, B, ..., AA, ...)."""
    name = ""
    while idx >= 0:
        idx, rem = divmod(idx, 26)
        name = chr(rem + ord("A")) + name
        idx -= 1
    return name


def load_mapping(domain: str, resource_name: str) -> dict:
    """
    Load the column-name mapping JSON for a given domain / table.
    Falls back to an empty dict (pure snake_case) when the file is absent.
    """
    path = Path("resources") / "mappings" / domain / f"{resource_name}.json"
    if path.exists():
        with path.open(encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def build_s3_client() -> boto3.client:
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    )


def normalize_columns(df: pd.DataFrame, mapping: dict, resource_name: str, logger) -> pd.DataFrame:
    """
    Shared column-normalisation logic for both Excel and CSV.
    Applies mapping dict first, falls back to snake_case, and disambiguates
    duplicates by appending the Excel column letter.
    """
    seen: defaultdict = defaultdict(int)
    new_columns: list = []

    for idx, col in enumerate(df.columns):
        if pd.isna(col) or str(col).strip() == "":
            base = "nan"
        else:
            base = mapping.get(col) or mapping.get(str(col).strip())
            if base is None:
                logger.warning(f"[{resource_name}] No mapping for col='{col}'; using snake_case")
                base = snake_case(col)

        seen[base] += 1
        if seen[base] > 1:
            base = f"{base}__{excel_col_name(idx).lower()}"

        new_columns.append(base)

    df.columns = new_columns
    return df

def build_hdfs_client(kind="ambari") -> BaseHDFSClient:
    return AmbariHDFSClient()

