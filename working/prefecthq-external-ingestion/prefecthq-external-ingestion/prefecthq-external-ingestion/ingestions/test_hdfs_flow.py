from dotenv import load_dotenv

load_dotenv()
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import os
import sys
import json

from common.ambari_util import *
from resources import RESOURCE_BASE_DIR
from state import STATE_BASE_DIR
from data import DATA_BASE_DIR

from prefect import flow, get_run_logger
import requests
import pandas as pd
import re
import unidecode

@flow(log_prints=True, name="Test HDFS via Keytab")
def test_hdfs_get():
    logger = get_run_logger()
    hdfs_path = "/opt/datasets/crawlers/vcs/freshworks/configs/resources.json"
    v = read_hdfs_https(hdfs_path)
    logger.info(v)


if __name__ == "__main__":
    test_hdfs_get.serve(
        name="Test HDFS via Keytab",
        tags=["production","HDFS Test"],
    )