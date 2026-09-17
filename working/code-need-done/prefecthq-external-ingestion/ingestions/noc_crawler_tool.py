
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

loaded = load_dotenv(verbose=True)

print(f"Loaded ENV = {loaded}")


import json
from datetime import datetime, timezone, timedelta
import gzip
from io import BytesIO
from resources import RESOURCE_BASE_DIR
from state import STATE_BASE_DIR
from data import DATA_BASE_DIR
from common.noc_metrics_helper import create_minio_client, VN_TZ, parse_data_json, upload_data_to_hdfs
from common.s3_helper import list_s3_files, read_s3_file



def download_file_by_day(category_name:str, day_str: str, crawl_mode):
    # day_str = "2026-01-21"
    bucket = os.getenv("MINIO_NOC_BUCKET")
    if not bucket:
        raise Exception(f"Not found Bucket for minio - {bucket}")
    s3 = create_minio_client()
    day = datetime.fromisoformat(day_str).replace(tzinfo=VN_TZ)
    prefix = category_name + "/" + day.strftime("%Y/%m/%d")
    filenames = list_s3_files(s3, bucket, prefix)
    items = []
    for filename in filenames:
        print(filename)
        data = read_s3_file(s3, bucket, filename)
        with gzip.open(BytesIO(data), "rb") as gz:
            items += parse_data_json(json.load(gz))
            # with gzip.open(gz, "rb") as gz2:
            #     items += parse_data_json(json.load(gz2))
    
    data_dir = DATA_BASE_DIR + "/noc_metrics"
    os.makedirs(data_dir, exist_ok=True)
    state_dir = STATE_BASE_DIR + "/noc_metrics"
    os.makedirs(state_dir, exist_ok=True)
    
    upload_data_to_hdfs(category_name, records=items,
                        resource_dir=RESOURCE_BASE_DIR,
                        state_dir=state_dir,
                        data_dir=data_dir,
                        crawl_mode = crawl_mode)


def run_crawl_noc_from_minio(category_name, crawl_mode,start_day_str:str,end_day_str:str):
    # category_name = "la_per_cpu__min"
    # download_file_by_day(category_name, "2026-01-21")
    # từ ngày 2026-01-23 trở về trước, do lưu response.raw, nên bị nén 2 lần. Từ 2026-01-24 có xử lý bỏ nén 2 lần.
    START_DAY = datetime.fromisoformat(start_day_str).replace(tzinfo=VN_TZ)
    END_DAY = datetime.fromisoformat(end_day_str).replace(tzinfo=VN_TZ)
    
    current = END_DAY
    
    while current >= START_DAY:
        start = current
        current -= timedelta(days=1)

        print(f"▶ Processing {start}")

        try:
            download_file_by_day(category_name, start.strftime("%Y-%m-%d"), crawl_mode)
            print(f"✅ Done {start.date()} for {category_name}")
        except Exception as e:
            print(f"❌ Failed {start.date()}: {e}")
            break

def run_many_names():
    
    names = [
        "mem_project__util__mean",
        "mem_project__util__max",
        "mem_project__util__min",
        "swap_used_percent__mean",
        "swap_used_percent__max",
        "swap_used_percent__min",
        "la_per_cpu__mean",
        "la_per_cpu__max",
        "la_per_cpu__min",
        "usage__mean",
        "usage__max",
        "usage__min",
        "disk_used_percent__mean",
        "disk_used_percent__max",
        "disk_used_percent__min",
        "disk_project__io_util__mean",
        "disk_project__io_util__max",
        "disk_project__io_util__min",
        "net_customer__util__mean",
        "net_customer__util__max",
        "net_customer__util__min",
        "mem_usage_average__mean",
        "mem_usage_average__max",
        "mem_usage_average__min",
        "cpu_usage_average__mean",
        "cpu_usage_average__max",
        "cpu_usage_average__min",
        "free_space__capacity",
    ]
    crawl_mode = "modified_and_new"
    
    # start_day_str = "2026-01-31"
    # end_day_str   = "2026-02-02"
   
    now = datetime.now().astimezone(VN_TZ)
    start_day_str = now.strftime("%Y-%m-%d")
    end_day_str   = now.strftime("%Y-%m-%d")
    
    for name in names:
        run_crawl_noc_from_minio(name, crawl_mode,start_day_str,end_day_str )    
    
if __name__ == "__main__":
    run_many_names()
    