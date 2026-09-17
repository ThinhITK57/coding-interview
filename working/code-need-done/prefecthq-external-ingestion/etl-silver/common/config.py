from dotenv import load_dotenv

load_dotenv()
import os

USE_PROXY = True 
PROXIES = {
    "http": os.getenv("CRAWLER_HTTP_PROXY"),
    "https": os.getenv("CRAWLER_HTTPS_PROXY"),
}
REQUEST_TIMEOUT = 150

os.environ["no_proxy"] = "datalake.viettelcyber.com"

