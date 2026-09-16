
from dotenv import load_dotenv

load_dotenv()

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
import requests
import pandas as pd
from datetime import datetime
from requests.auth import HTTPBasicAuth
from common.crawler_util import build_basic_auth_header
from common.freshdesk_cso_helper import *
from resources import RESOURCE_BASE_DIR
from state import STATE_BASE_DIR
from data import DATA_BASE_DIR
from common.freshdesk.fact_cso_tickets_sla import download_cso_sla_csv_to_records
from prefect import flow, get_run_logger


@flow(log_prints=True, name="[Hourly]-Minio CX CSO APIs - Data Lake")
def run_scrapping_cso_crawler():
    # API_KEY = read_hdfs_https(API_KEY_PATH).strip()
    FRESHDESK_API_KEY= os.getenv("FRESHDESK_API_KEY")

    if not FRESHDESK_API_KEY:
        raise Exception("Not found FRESHDESK_API_KEY")

    # API_COOKIE = read_hdfs_https(API_COOKIE_PATH).strip()

    # =========================
    # Load config
    # =========================
    # CONFIG_PATH = "/opt/datasets/crawlers/vcs/freshworks/configs/resources.json"
    # cfg = json.loads(read_hdfs_https(CONFIG_PATH))

    filename = os.path.join(RESOURCE_BASE_DIR, "resources-cx-cso.json")
    with open(filename, "r") as f:
        cfg = json.loads(f.read().strip())

    RESOURCE_NAME = "fact_cso_tickets"
    if RESOURCE_NAME not in cfg:
        raise Exception("Resource {} not found in config".format(RESOURCE_NAME))

    rc = cfg[RESOURCE_NAME]

    HDFS_BASE = rc["hdfs_base"]
    STATE_PATH = rc["state_path"]
    BASE_URL = rc["base_url"]
    RESOURCE_URL = rc["resource_url"]
    API_KEY_PATH = rc["api_key_path"]
    API_COOKIE_PATH = rc["api_cookie_path"]
    QUERY_PARAMS = rc["query_params"]
    ENABLE_STATE = rc.get("enable_state", False)

    HEADERS = {
        "Authorization": build_basic_auth_header(FRESHDESK_API_KEY, "X"),
    }
    
    
    TABLES = [
        "fact_cso_tickets",
        "dim_cso_companies",
        
        "dim_cso_ticket_fields", 
             "dim_cso_email_configs",
              "dim_cso_company_fields",
            
            "dim_cso_agents",
            "dim_cso_contacts",
            
              "dim_cso_roles",
              "dim_cso_groups",
              "dim_cso_admin_group",
              "dim_cso_time_entries",
              "dim_cso_sla_policies",
            "dim_cso_contact_fields",
    ]

    data_dir = DATA_BASE_DIR + "/cx_cso"
    os.makedirs(data_dir, exist_ok=True)
    
    state_dir = STATE_BASE_DIR + "/cx_cso"
    os.makedirs(state_dir, exist_ok=True)
    
    for resource_name in TABLES:
        out_of_data = False
        for j in range(20):
            for i in range(6):
                NUMBER_OF_PAGE = 50
                try:
                    out_of_data = fetch_resource_name_freshdesk(resource_name, cfg, headers=HEADERS, max_page=(i+1) * NUMBER_OF_PAGE, 
                                                                start_page=i*NUMBER_OF_PAGE +1,
                                                                resource_dir=RESOURCE_BASE_DIR,
                                                                state_dir=state_dir,
                                                                data_dir=data_dir,
                                                                )
    
                    if out_of_data :
                        print("out_of_data at " , i)
                        break
                except Exception as e:
                    print(e)
            if out_of_data :
                print("out_of_data at " , j)
                break
            
    # fact_cso_tickets_sla
    resource_name = "fact_cso_tickets_sla"
    try:
        records = download_cso_sla_csv_to_records()
        fetch_resource_name_freshdesk(resource_name, cfg, headers=HEADERS, max_page=1, 
                                                    start_page=0,
                                                    resource_dir=RESOURCE_BASE_DIR,
                                                    state_dir=state_dir,
                                                    data_dir=data_dir,
                                                    records=records,
                                                    )

    except Exception as e:
        print(e)
    


@flow(log_prints=True, name="[Adhoc]-Minio CX CSO APIs - Data Lake Single Table")
def run_adhoc_scrapping_cso_crawler_by_tablename(resource_name):
    # API_KEY = read_hdfs_https(API_KEY_PATH).strip()
    FRESHDESK_API_KEY= os.getenv("FRESHDESK_API_KEY")

    if not FRESHDESK_API_KEY:
        raise Exception("Not found FRESHDESK_API_KEY")

    # API_COOKIE = read_hdfs_https(API_COOKIE_PATH).strip()

    # =========================
    # Load config
    # =========================
    # CONFIG_PATH = "/opt/datasets/crawlers/vcs/freshworks/configs/resources.json"
    # cfg = json.loads(read_hdfs_https(CONFIG_PATH))

    filename = os.path.join(RESOURCE_BASE_DIR, "resources-cx-cso.json")
    with open(filename, "r") as f:
        cfg = json.loads(f.read().strip())

    RESOURCE_NAME = "fact_cso_tickets"
    if RESOURCE_NAME not in cfg:
        raise Exception("Resource {} not found in config".format(RESOURCE_NAME))

    rc = cfg[RESOURCE_NAME]

    HDFS_BASE = rc["hdfs_base"]
    STATE_PATH = rc["state_path"]
    BASE_URL = rc["base_url"]
    RESOURCE_URL = rc["resource_url"]
    API_KEY_PATH = rc["api_key_path"]
    API_COOKIE_PATH = rc["api_cookie_path"]
    QUERY_PARAMS = rc["query_params"]
    ENABLE_STATE = rc.get("enable_state", False)

    HEADERS = {
        "Authorization": build_basic_auth_header(FRESHDESK_API_KEY, "X"),
    }
    
    cfg[resource_name]["crawl_mode"] = "static"
    
    data_dir = DATA_BASE_DIR + "/cx_cso"
    os.makedirs(data_dir, exist_ok=True)
    
    state_dir = STATE_BASE_DIR + "/cx_cso"
    os.makedirs(state_dir, exist_ok=True)
    
    # fact_cso_tickets_sla
    if resource_name == "fact_cso_tickets_sla":
        try:
            records = download_cso_sla_csv_to_records()
            fetch_resource_name_freshdesk(resource_name, cfg, headers=HEADERS, max_page=1, 
                                                        start_page=0,
                                                        resource_dir=RESOURCE_BASE_DIR,
                                                        state_dir=state_dir,
                                                        data_dir=data_dir,
                                                        records=records,
                                                        )

        except Exception as e:
            print(e)
        return
    
    out_of_data = False
    for j in range(20):
        for i in range(6):
            NUMBER_OF_PAGE = 50
            try:
                out_of_data = fetch_resource_name_freshdesk(resource_name, cfg, headers=HEADERS, max_page=(i+1) * NUMBER_OF_PAGE, 
                                                            start_page=i*NUMBER_OF_PAGE +1,
                                                            resource_dir=RESOURCE_BASE_DIR,
                                                            state_dir=state_dir,
                                                            data_dir=data_dir,
                                                            )

                if out_of_data :
                    print("out_of_data at " , i)
                    break
            except Exception as e:
                print(e)
        if out_of_data :
            print("out_of_data at " , j)
            break
    

# Dòng này giúp bạn vẫn có thể test file này độc lập bằng lệnh: python main.py
if __name__ == "__main__":
    d1 = run_scrapping_cso_crawler.to_deployment(
        name="[Hourly]-Minio CX CSO APIs - Data Lake",
        cron="5 * * * *",
        tags=["production", "CSO", "ingestion"],
    )
    
    d3 = run_adhoc_scrapping_cso_crawler_by_tablename.to_deployment(
        name="[Adhoc]-Minio CX CSO APIs - Data Lake Single Tablee",
        tags=["production", "CSO", "Single", "Refresh"],
    )
    from prefect import serve
    serve(d1, d3)