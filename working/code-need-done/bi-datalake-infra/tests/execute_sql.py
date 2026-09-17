import warnings
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter('ignore', InsecureRequestWarning)
from dotenv import load_dotenv

load_dotenv()

# pip install trino --proxy http://10.255.249.100:3128 
from trino.dbapi import connect

# ====== CONFIG ======
from trino.auth import BasicAuthentication
import os

conn = connect(
    host="10.255.245.150",
    port=8443,
    user="admin",
    catalog="hive",
    schema="default",
    http_scheme="https",
    auth=BasicAuthentication("admin", os.getenv("TRINO_PASSWORD")),
    verify=False   # self-signed cert
)

cursor = conn.cursor()

# ====== 1. LIST DATABASE (SCHEMA) ======
with open("tmp.sql", mode="rt", encoding="utf-8") as f:
    query = f.read()
    qs = query.split(";")
    for q in qs:
        q = q.strip()
        v = cursor.execute(q)
        print(cursor.fetchone())