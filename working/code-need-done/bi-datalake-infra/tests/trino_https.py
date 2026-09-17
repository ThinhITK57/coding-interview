import warnings
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter('ignore', InsecureRequestWarning)

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
print("=== SCHEMAS ===")
cursor.execute("SHOW SCHEMAS")
schemas = cursor.fetchall()

for s in schemas:
    print(s[0])

# ====== 2. LIST TABLE ======
print("\n=== TABLES ===")

for schema in schemas:
    schema_name = schema[0]

    try:
        cursor.execute(f"SHOW TABLES FROM {schema_name}")
        tables = cursor.fetchall()

        if tables:
            print(f"\n[{schema_name}]")
            for t in tables:
                print(" -", t[0])

    except Exception as e:
        print(f"Skip {schema_name}: {e}")

cursor.execute("drop table hive.cx_cso_silver.cx_cso_support_tickets")