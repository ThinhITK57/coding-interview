from dotenv import load_dotenv

load_dotenv()
import os

USE_PROXY = True
PROXIES = {"http": os.getenv("CRAWLER_HTTP_PROXY", "192.168.5.8:3128"), "https": os.getenv("CRAWLER_HTTPS_PROXY", "192.168.5.8:3128")}
REQUEST_TIMEOUT = 150

import os

os.environ["no_proxy"] = os.getenv("CRAWLER_NO_PROXY", "datalake.viettelcyber.com,127.0.0.1")

TEMP_DATA_DIR = os.getenv("TEMP_DATA_DIR", "./tmp/data")
