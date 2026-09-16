
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

loaded = load_dotenv(verbose=True)

print(f"Loaded ENV = {loaded}")

from datetime import datetime, timezone, timedelta
from resources import RESOURCE_BASE_DIR
from state import STATE_BASE_DIR
from data import DATA_BASE_DIR
from common.jira_helper import parse_data_as_json, create_minio_client, upload_data_to_hdfs, \
    CSV_ZIP, JSON_GZ,JSON_ONLY, VN_TZ, \
    COLUMN_MAPPING, COLUMN_MAPPING_BY_TABLE
from common.s3_helper import list_s3_files, read_s3_file
import gzip
import json
from io import BytesIO


def download_file_by_day(category_name:str, day_str: str, crawl_mode="modified_and_new", **kwargs):
    file_type = kwargs.get("file_type")
    
    # day_str = "2026-01-21"
    bucket = os.getenv("MINIO_JIRA_BUCKET")
    if not bucket:
        raise Exception(f"Not found Bucket for minio - {bucket}")
    s3 = create_minio_client()
    day = datetime.fromisoformat(day_str).replace(tzinfo=VN_TZ)
    prefix = category_name + "/" + day.strftime("%Y/%m/%d")
    filenames = list_s3_files(s3, bucket, prefix)
    items = []
    for filename in filenames:
        print(f"Processing {filename}")
        data = read_s3_file(s3, bucket, filename)
        if file_type == CSV_ZIP:
            items += parse_data_as_json(data, cate_name=category_name)
        elif file_type == JSON_GZ:
            with gzip.open(BytesIO(data), "rb") as gz:
                records = json.load(gz)
                base_name = category_name
                table_mapping = COLUMN_MAPPING_BY_TABLE.get(base_name, {})
                mapped = []
                for r in records:
                    r["source_file"] = filename
                    new_r = {COLUMN_MAPPING.get(k, k): v for k, v in r.items()}      # global first
                    new_r = {table_mapping.get(k, k): v for k, v in new_r.items()}   # per-table override
                    mapped.append(new_r)
                items += mapped
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


def run_crawl_jira_from_minio(category_name, crawl_mode="modified_and_new", file_type:str = None, start_day_str=None, end_day_str=None):

    START_DAY = datetime.fromisoformat(start_day_str).replace(tzinfo=VN_TZ)
    END_DAY = datetime.fromisoformat(end_day_str).replace(tzinfo=VN_TZ)
    
    current = END_DAY
    
    while current >= START_DAY:
        start = current
        current -= timedelta(days=1)

        print(f"▶ Processing {start}")

        try:
            download_file_by_day(category_name, start.strftime("%Y-%m-%d"), 
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
    names = [
        ["ulnl","modified_and_new", CSV_ZIP],
        ["bug_prod","modified_and_new", CSV_ZIP],
        ["process_compliance","modified_and_new", CSV_ZIP],
        ["kpi_kqi","modified_and_new", CSV_ZIP],
        ["os_project_uat_bugs","modified_and_new", CSV_ZIP],
        ["task_operation","modified_and_new", CSV_ZIP],
        ["defect_task","modified_and_new", CSV_ZIP],
        ["project_management","modified_and_new", CSV_ZIP],
        ["log_work","modified_and_new", CSV_ZIP],
        
        # ["msn_task","static", CSV_ZIP ],
        # ["jira_user","static", CSV_ZIP ],
        
        # ["field_list","static", JSON_GZ],
        # ["project_list","static", JSON_GZ],
        
        # ["kpi_binh_staff","static", CSV_ZIP],

        # new tables from jira_v2
        ["log_work_v2", "modified_and_new", JSON_GZ],
        ["bug_prod_v2", "modified_and_new", JSON_GZ],
        ["sprints", "static", JSON_GZ],
        ["project_versions", "static", JSON_GZ]
        
    ]
    
    start_day_str = "2026-01-31"
    end_day_str   = "2026-02-02"
    
    now = datetime.now().astimezone(VN_TZ)
    start_day_str = now.strftime("%Y-%m-%d")
    end_day_str   = now.strftime("%Y-%m-%d")
    
    for name in names:
        run_crawl_jira_from_minio(name[0], name[1], name[2], start_day_str, end_day_str )    
    
if __name__ == "__main__":
    run_many_names()
