
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone, timedelta
import boto3
import time
from .config import *
from .http_util import *
from .crawler_util import *
from .ambari_util import *


VN_TZ = timezone(timedelta(hours=7))




def create_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_NOC_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_NOC_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_NOC_SECRET_KEY"),
        verify=os.getenv("MINIO_NOC_SECURE", "false") == "true",
        
    )



def upload_data_to_hdfs(resource_name, records, **kwargs):
    resource_dir = kwargs.get("resource_dir")
    data_dir = kwargs.get("data_dir")
    crawl_mode = kwargs.get("crawl_mode")
    
    print("Start crawl : ", resource_name)

    HDFS_BASE = "/opt/datasets/crawlers/vcs/noc_metrics/data"
    # STATE_PATH = "/opt/datasets/crawlers/vcs/noc_metrics/state"
    HIVE_DB = "noc_metrics_raw"
    schema_local_path = None

    print(resource_name)
    start_time = time.time()
    
    if not records:
        print("No new data")
        return True

    # =========================
    # Pandas → Parquet
    # =========================

    now = datetime.now()
    partition_path = "{}/{}".format(HDFS_BASE, resource_name)

    filename = "data_{}_{}{:02d}{:02d}_{}{:02d}{:02d}.parquet".format(
        resource_name, now.year, now.month, now.day, now.hour, now.minute, now.second
    )
    local_parquet = data_dir + "/{}/{}".format(resource_name, filename)
    os.makedirs(os.path.dirname(local_parquet), exist_ok=True)

    # df.to_parquet(local_parquet,engine="pyarrow", compression="snappy", index=False)
    records = convert_json_add_ts_columns(records)

    schema_tm_path = resource_dir + "/parquet_schema/noc_metrics/{}.json".format(resource_name)
    schema = None
    if schema_local_path:
        schema = load_pyarrow_schema_from_json(schema_local_path)

    if os.path.exists(schema_tm_path):
        schema = load_pyarrow_schema_from_json(schema_tm_path)

    if not schema:
        schema = infer_schema_from_json(records)
        schema_json = save_pyarrow_type_to_json(schema)
        write_file_json(schema_tm_path, schema_json)

    records = convert_json_list_by_arrow_schema(records, schema)

    df = pd.DataFrame(records)
    data_table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

    pq.write_table(data_table, local_parquet, compression="snappy")

    if crawl_mode == "static":
        replace_hdfs_https("{}".format(partition_path), local_parquet)
    else:
        upload_hdfs_https("{}".format(partition_path), local_parquet)

    print("Uploaded parquet to", partition_path)
    
    remove_file(local_parquet)
    print("Removed temp parquet to", partition_path)
    

    # =========================
    # Generate SQL (TEXT ONLY)
    # =========================
    sql = gen_spark_create_table(
        schema=schema,
        db=HIVE_DB,
        table=resource_name,
        location="{}/{}".format(HDFS_BASE, resource_name),
    )

    # filename = "create_table_{}_{}{:02d}{:02d}.sql".format(
    #     resource_name,
    #     now.hour,
    #     now.minute,
    #     now.second
    # )

    filename = "create_table_{}.sql".format(resource_name)

    local_sql = data_dir + "/{}/{}".format(resource_name, filename)
    os.makedirs(os.path.dirname(local_sql), exist_ok=True)

    with open(local_sql, "w") as f:
        f.write(sql)

    # upload_hdfs_https(
    #     "{}/{}".format(HDFS_BASE, resource_name),
    #     local_sql
    # )

    print("Uploaded SQL definition")
    
    elapsed = time.time() - start_time
    print("Loop {} took {:.3f}s".format(resource_name, elapsed))

def parse_data_json(data):
    items = []
    results = data.get("results", [])
    for result in results:
        series = result.get("series", [])
        for s in series:
            name = s.get("name")
            tags: dict = s.get("tags", {})
            columns = s.get("columns", [])
            values = s.get("values", [])
            
            customer = tags.get("customer", "-")
            host = tags.get("host", "-")
            project = tags.get("project")
            tags.pop("customer")
            tags.pop("host")
            tags.pop("project")
            
            if not tags :
                tags = None
            
            for value in values:
                item = {
                    "name": name,
                    "customer": customer,
                    "host": host,
                    "project": project,
                    "tags": tags,
                }
                for idx, col_name in enumerate(columns):
                    item[col_name] = value[idx]
                # if "time" in item:
                #    item["time_ts"] = int(datetime.fromisoformat(item["time"]).timestamp())
                items += [item]
    return items
