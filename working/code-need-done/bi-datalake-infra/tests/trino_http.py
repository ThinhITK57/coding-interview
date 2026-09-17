import warnings
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter('ignore', InsecureRequestWarning)

# pip install trino --proxy http://10.255.249.100:3128 
from trino.dbapi import connect

# ====== CONFIG ======
conn = connect(
    host="localhost",       # hoặc domain/nginx
    port=8080,              # 8443 nếu HTTPS
    user="admin",
    catalog="hive",         # hoặc system
    schema="default",
    http_scheme="http",     # "https" nếu dùng SSL
    auth=None               # hoặc BasicAuthentication nếu có password
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