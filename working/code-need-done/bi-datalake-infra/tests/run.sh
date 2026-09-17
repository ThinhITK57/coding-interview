#!/bin/bash

docker pull python:3.11

docker run -it --rm python:3.11 bash

pip install trino --proxy http://10.255.249.100:3128 


cat <<EOF > test_trino.py
from trino.dbapi import connect
from trino.auth import BasicAuthentication

conn = connect(
    host="10.255.245.150",
    port=8443,
    user="admin",
    catalog="hive",
    schema="default",
    http_scheme="https",
    auth=BasicAuthentication("admin", "Welcome1"),
    verify=False   # self-signed cert
)

cur = conn.cursor()
cur.execute("SHOW SCHEMAS")
print("Schemas:", cur.fetchall())

cur.execute("SHOW TABLES")
print("Tables:", cur.fetchall())
EOF

python test_trino.py

