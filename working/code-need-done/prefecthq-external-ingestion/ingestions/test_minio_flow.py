from dotenv import load_dotenv

load_dotenv()
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, get_run_logger
from jira_crawler_tool import run_crawl_jira_from_minio, VN_TZ, CSV_ZIP, JSON_GZ
from datetime import datetime, timezone, timedelta
from resources import RESOURCE_BASE_DIR
from state import STATE_BASE_DIR
from data import DATA_BASE_DIR
from common.jira_helper import parse_data_as_json, create_minio_client, upload_data_to_hdfs, \
    CSV_ZIP, JSON_GZ,JSON_ONLY, VN_TZ
from common.s3_helper import list_s3_files, read_s3_file
import gzip
import json
from io import BytesIO


@flow(log_prints=True, name="Test Minio Connection")
def test_minio_flow():
    logger = get_run_logger()
    client = create_minio_client()
    bucket = os.getenv("MINIO_JIRA_BUCKET")
    items = list_s3_files(client, bucket)
    logger.info(f"{items}")


if __name__ == "__main__":
    test_minio_flow.serve(
        name="Test Minio Connection",
        tags=["production","Minio Test"],
    )