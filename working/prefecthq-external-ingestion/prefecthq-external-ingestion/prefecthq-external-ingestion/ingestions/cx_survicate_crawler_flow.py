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



from common.survicate_helper import *
from resources import RESOURCE_BASE_DIR
from state import STATE_BASE_DIR
from data import DATA_BASE_DIR


from prefect import flow, get_run_logger


@flow(log_prints=True, name="[Hourly]-Minio CX Survicate APIs - Data Lake")
def run_scrapping_survicate_crawler():
    RESOURCE_NAME = "dim_survicate_surveys"
    # =========================
    # Load config
    # =========================
    # CONFIG_PATH = "/opt/datasets/crawlers/vcs/freshworks/configs/resources.json"
    # cfg = json.loads(read_hdfs_https(CONFIG_PATH))
    filename = os.path.join(RESOURCE_BASE_DIR, "resources-cx-survicate.json")
    with open(filename, "r") as f:
        cfg = json.loads(f.read().strip())

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
    
    data_dir = DATA_BASE_DIR + "/cx_survicate"
    os.makedirs(data_dir, exist_ok=True)
    
    state_dir = STATE_BASE_DIR + "/cx_survicate"
    os.makedirs(state_dir, exist_ok=True)
    
    ids = crawl_surveys(resource_dir=RESOURCE_BASE_DIR,
                        state_dir=state_dir,
                        data_dir=data_dir)
    survey_ids = read_items(data_dir + "/survicate_survey_ids.json")
    print(len(survey_ids))
    crawl_survey_details(survey_ids, resource_dir=RESOURCE_BASE_DIR,
                        state_dir=state_dir,
                        data_dir=data_dir)
    crawl_survey_details(survey_ids, resource_dir=RESOURCE_BASE_DIR,
                        state_dir=state_dir,
                        data_dir=data_dir)
    crawl_survey_question(survey_ids, resource_dir=RESOURCE_BASE_DIR,
                        state_dir=state_dir,
                        data_dir=data_dir)
    crawl_survey_responses(survey_ids, resource_dir=RESOURCE_BASE_DIR,
                        state_dir=state_dir,
                        data_dir=data_dir)
    emails = read_items(data_dir + "/survicate_emails.json")
    print(len(emails))
    
    # crawl_personal_data(emails, resource_dir=RESOURCE_BASE_DIR,
    #                     state_dir=state_dir,
    #                     data_dir=data_dir)
    
    remove_file(data_dir + "/survicate_emails.json")
    remove_file(data_dir + "/survicate_survey_ids.json")
    

# Dòng này giúp bạn vẫn có thể test file này độc lập bằng lệnh: python main.py
if __name__ == "__main__":
    run_scrapping_survicate_crawler.serve(
        name="[Hourly]-Minio CX Survicate APIs - Data Lake",
        cron="0 * * * *",
        tags=["production", "Survicate", "ingestion"],
    )