
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

loaded = load_dotenv(verbose=True)

print(f"Loaded ENV = {loaded}")

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone, timedelta
from io import BytesIO
import boto3
import time
from common.jira_helper import upload_data_to_hdfs, parse_data_as_json, VN_TZ, CSV_ZIP, JSON_GZ, JSON_ONLY
from resources import RESOURCE_BASE_DIR
from state import STATE_BASE_DIR
from data import DATA_BASE_DIR
import gzip
import json
from typing import List


def list_local_files(base_folder: str) -> List[str]:
    """
    List all files (recursive) from base_folder
    :param base_folder: root folder
    :return: list of full file paths
    """
    files = []
    for root, _, filenames in os.walk(base_folder):
        for fname in filenames:
            files.append(os.path.join(root, fname))
    return files

def read_local_file_binary(file_path: str) -> bytes:
    """
    Read file as binary
    :param file_path: full file path
    :return: file content as bytes
    """
    with open(file_path, "rb") as f:
        return f.read()


def download_file_by_day(base_folder, category_name:str, day_str: str, crawl_mode="modified_and_new", **kwargs):
    # day_str = "2026-01-21"
    # VN_TZ = timezone(timedelta(hours=7))
    # day = datetime.fromisoformat(day_str).replace(tzinfo=VN_TZ)
    # prefix = category_name + "/" + day.strftime("%Y/%m/%d")
    file_type = kwargs.get("file_type", CSV_ZIP)
    
    filenames = list_local_files(base_folder)
    items = []
    for filename in filenames:
        print(f"Processing {filename}")
        data = read_local_file_binary(filename)
        if file_type == CSV_ZIP:
            items += parse_data_as_json(data, cate_name=category_name)
        elif file_type == JSON_GZ:
            with gzip.open(BytesIO(data), "rb") as gz:
                items += json.load(gz)
        elif file_type == JSON_ONLY:
            items += json.load(data)
    
    if len(items) ==0:
        print("No data for ", category_name, day_str)
        return
    
    data_dir = DATA_BASE_DIR + "/jira"
    os.makedirs(data_dir, exist_ok=True)
    
    state_dir = STATE_BASE_DIR + "/jira"
    os.makedirs(state_dir, exist_ok=True)
    
    upload_data_to_hdfs(category_name, records=items,
                        crawl_mode=crawl_mode,
                        resource_dir=RESOURCE_BASE_DIR,
                        state_dir=state_dir,
                        data_dir=data_dir)


def run_crawl_jira_from_minio(base_folder, category_name, crawl_mode="modified_and_new", file_type: str = None, start_day_str=None,end_day_str = None ):
    
    START_DAY = datetime.fromisoformat(start_day_str).replace(tzinfo=VN_TZ)
    END_DAY = datetime.fromisoformat(end_day_str).replace(tzinfo=VN_TZ)
    
    current = END_DAY
    
    while current >= START_DAY:
        start = current
        current -= timedelta(days=1)

        print(f"▶ Processing {start}")

        try:
            download_file_by_day(base_folder, category_name, start.strftime("%Y-%m-%d"), 
                                 crawl_mode,
                                 resource_base_dir=RESOURCE_BASE_DIR,
                                 state_base_dir=STATE_BASE_DIR,
                                 data_base_dir=DATA_BASE_DIR,
                                 file_type=file_type)
            print(f"✅ Done {start.date()} for {category_name}")
        except Exception as e:
            print(f"❌ Failed {start.date()}: {e}")
            break

def run_many_names():
    base_folder = r"C:\Users\namtv40\Projects\prefecthq-internal-ingestion\prefect-jira-crawler\out\log_work\2026\01\27"
    
    names = [
        ["log_work","modified_and_new", CSV_ZIP],
    ]
    start_day_str = "2026-01-27"
    end_day_str   = "2026-01-27"
    
    for name in names:
        run_crawl_jira_from_minio(base_folder, name[0], name[1], name[2], start_day_str,end_day_str)    
    
if __name__ == "__main__":
    run_many_names()
