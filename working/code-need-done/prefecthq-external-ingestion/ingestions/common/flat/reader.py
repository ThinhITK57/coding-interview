

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

from prefect import flow, task, get_run_logger

from common.config import *
from common.http_util import *
from common.crawler_util import *
from common.ambari_util import *
from common.flat.util import *

# ---------------------------------------------------------------------------
# Single-source readers  (plain functions, not Prefect tasks)
# ---------------------------------------------------------------------------

def read_excel_from_minio(
    s3, in_bucket, object_key, mapping, resource_name, logger,
    sheet_name=0, header_row=0, drop_rows=1,
) -> pd.DataFrame:
    logger.info(f"[{resource_name}] Fetching (xlsx) s3://{in_bucket}/{object_key}")
    response = s3.get_object(Bucket=in_bucket, Key=object_key)

    df = pd.read_excel(BytesIO(response["Body"].read()), sheet_name=sheet_name, header=None, dtype=str)

    # pull raw header row, then drop all rows up to drop_rows
    raw_headers = df.iloc[header_row].tolist()
    df = df.iloc[drop_rows:].reset_index(drop=True)
    df.columns = raw_headers

    return normalize_columns(df, mapping, resource_name, logger)


def read_csv_from_minio(
    s3, in_bucket, object_key, mapping, resource_name, logger,
    header_row=0, drop_rows=1, encoding="utf-8",
) -> pd.DataFrame:
    logger.info(f"[{resource_name}] Fetching (csv) s3://{in_bucket}/{object_key}")
    response = s3.get_object(Bucket=in_bucket, Key=object_key)

    df = pd.read_csv(BytesIO(response["Body"].read()), header=None, dtype=str, encoding=encoding)

    raw_headers = df.iloc[header_row].tolist()
    df = df.iloc[drop_rows:].reset_index(drop=True)
    df.columns = raw_headers

    df = normalize_columns(df, mapping, resource_name, logger)
    df["source_file"] = object_key.split("/")[-1]
    return df


def read_csv_folder_from_minio(
    s3, in_bucket, folder_prefix, mapping, resource_name, logger,
    header_row=0, drop_rows=1, encoding="utf-8",
) -> pd.DataFrame:
    """List all .csv objects under folder_prefix and union them."""
    logger.info(f"[{resource_name}] Listing CSVs under s3://{in_bucket}/{folder_prefix}")
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=in_bucket, Prefix=folder_prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].lower().endswith(".csv")
    ]

    if not keys:
        raise ValueError(f"[{resource_name}] No CSV files found under s3://{in_bucket}/{folder_prefix}")

    logger.info(f"[{resource_name}] Found {len(keys)} CSV file(s)")
    dfs = [
        read_csv_from_minio(
            s3, in_bucket, key, mapping, resource_name, logger,
            header_row=header_row, drop_rows=drop_rows, encoding=encoding,
        )
        for key in sorted(keys)
    ]
    return pd.concat(dfs, ignore_index=True)


def read_source(s3, in_bucket, source: dict, mapping: dict, resource_name: str, logger) -> pd.DataFrame:
    """
    Dispatch a single source descriptor to the right reader.

    source keys:
      object_key   : str        -- S3 key (file) or prefix (csv_folder)
      file_type    : str        -- "excel" | "csv" | "csv_folder"  (default: "excel")
      sheet_name   : int|str   -- for excel (default: 0)
      header_row   : int        -- (default: 0)
      drop_rows    : int        -- (default: 1)
      encoding     : str        -- for csv (default: "utf-8")
      add_columns  : dict       -- static columns to inject, e.g. {"data_type": "CP"}
      post_process : callable   -- optional df -> df transform applied after reading
    """
    file_type = source.get("file_type", "excel")
    object_key = source["object_key"]

    if file_type == "excel":
        df = read_excel_from_minio(
            s3, in_bucket, object_key, mapping, resource_name, logger,
            sheet_name=source.get("sheet_name", 0),
            header_row=source.get("header_row", 0),
            drop_rows=source.get("drop_rows", 1),
        )
    elif file_type == "csv":
        df = read_csv_from_minio(
            s3, in_bucket, object_key, mapping, resource_name, logger,
            header_row=source.get("header_row", 0),
            drop_rows=source.get("drop_rows", 1),
            encoding=source.get("encoding", "utf-8"),
        )
    elif file_type == "csv_folder":
        df = read_csv_folder_from_minio(
            s3, in_bucket, object_key, mapping, resource_name, logger,
            header_row=source.get("header_row", 0),
            drop_rows=source.get("drop_rows", 1),
            encoding=source.get("encoding", "utf-8"),
        )
    else:
        raise ValueError(f"[{resource_name}] Unknown file_type='{file_type}'")

    for col, val in source.get("add_columns", {}).items():
        df[col] = val

    if source.get("post_process"):
        df = source["post_process"](df)

    return df