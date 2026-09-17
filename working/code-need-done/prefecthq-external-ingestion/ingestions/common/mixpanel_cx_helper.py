from .config import *
from .http_util import *
from .crawler_util import *
from .ambari_util import *
# from prefect import task



# @task
def push_mixpanel_file(
    resource_name,
    cfg,
    local_filename,
    target_date: str,
    **kwargs
):
    start_time = time.time()

    if resource_name not in cfg:
        print("Not found")
        out_of_data = True
        return True
    rc = cfg[resource_name]

    HDFS_BASE = rc["hdfs_base"]
    STATE_PATH = rc["state_path"]
    BASE_URL = rc["base_url"]
    RESOURCE_URL = rc["resource_url"]
    API_KEY_PATH = rc["api_key_path"]
    API_COOKIE_PATH = rc["api_cookie_path"]
    QUERY_PARAMS = rc["query_params"]
    ENABLE_STATE = rc.get("enable_state", False)
    hive_db = rc["hive_db"]
    crawl_mode = rc["crawl_mode"]
    schema_local_path = rc.get("schema_local_path")

    print(resource_name)
    start_time = time.time()
    # =========================
    # MAIN
    # =========================

    if ENABLE_STATE:
        last_state = read_last_state(resource_name)
        print("Last state =", last_state)
    else:
        last_state = None

    if not local_filename:
        print("No new data")
        out_of_data = True
        return True

    # =========================
    # Pandas → Parquet
    # =========================

    now = datetime.strptime( target_date, "%Y-%m-%d" ).date()
    partition_path = "{}/{}/{}/{:02d}/{:02d}".format(HDFS_BASE, resource_name, now.year, now.month, now.day)

    # filename = "data_{}_{}{:02d}{:02d}_{}{:02d}{:02d}.parquet".format(
    #     resource_name, now.year, now.month, now.day, now.hour, now.minute, now.second
    # )

    if crawl_mode == "static":
        replace_hdfs_https("{}".format(partition_path), local_filename)
    else:
        upload_hdfs_https("{}".format(partition_path), local_filename)

    print("Uploaded parquet to", partition_path)
    os.remove(local_filename)
    print("Removed temp parquet to", local_filename)
   
    elapsed = time.time() - start_time
    print("Loop {} took {:.3f}s".format(resource_name, elapsed))
    return False


# @task
def fetch_resource_name_freshdesk(
    resource_name,
    cfg,
    records=None, **kwargs
):
    print("Start crawl : ", resource_name)
    start_time = time.time()

    if resource_name not in cfg:
        print("Not found")
        out_of_data = True
        return True
    rc = cfg[resource_name]

    HDFS_BASE = rc["hdfs_base"]
    STATE_PATH = rc["state_path"]
    BASE_URL = rc["base_url"]
    RESOURCE_URL = rc["resource_url"]
    API_KEY_PATH = rc["api_key_path"]
    API_COOKIE_PATH = rc["api_cookie_path"]
    QUERY_PARAMS = rc["query_params"]
    ENABLE_STATE = rc.get("enable_state", False)
    hive_db = rc["hive_db"]
    crawl_mode = rc["crawl_mode"]
    schema_local_path = rc.get("schema_local_path")

    print(resource_name)
    start_time = time.time()
    # =========================
    # MAIN
    # =========================

    if ENABLE_STATE:
        last_state = read_last_state(resource_name)
        print("Last state =", last_state)
    else:
        last_state = None

    if not records:
        print("No new data")
        out_of_data = True
        return True

    # =========================
    # Pandas → Parquet
    # =========================

    now = datetime.now()
    partition_path = "{}/{}".format(HDFS_BASE, resource_name)

    filename = "data_{}_{}{:02d}{:02d}_{}{:02d}{:02d}.parquet".format(
        resource_name, now.year, now.month, now.day, now.hour, now.minute, now.second
    )
    local_parquet = "./tmp/data/cx_mixpanel_raw/{}/{}".format(resource_name, filename)
    os.makedirs(os.path.dirname(local_parquet), exist_ok=True)

    # df.to_parquet(local_parquet,engine="pyarrow", compression="snappy", index=False)
    records = convert_json_add_ts_columns(records)

    schema_tm_path = "./resources/parquet_schema/cx_mixpanel_raw/{}.json".format(resource_name)
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
    os.remove(local_parquet)
    print("Removed temp parquet to", local_parquet)

    # =========================
    # Generate SQL (TEXT ONLY)
    # =========================
    sql = gen_spark_create_table(
        schema=schema,
        db=hive_db,
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

    local_sql = "./tmp/data/cx_mixpanel_raw/{}/{}".format(resource_name, filename)
    os.makedirs(os.path.dirname(local_sql), exist_ok=True)

    with open(local_sql, "w") as f:
        f.write(sql)

    # upload_hdfs_https(
    #     "{}/{}".format(HDFS_BASE, resource_name),
    #     local_sql
    # )

    print("Uploaded SQL definition")

    # =========================
    # Save new state
    # =========================
    if ENABLE_STATE and "updated_at" in df.columns:
        max_ts = get_max_updated_at_str(df)
        max_ts = subtract_minutes(max_ts, 900)
        write_last_state(max_ts, resource_name)
        print("find last state for ", resource_name, "max_ts=", max_ts)

    elapsed = time.time() - start_time
    print("Loop {} took {:.3f}s".format(resource_name, elapsed))
    if len(records) < 100:
        print("No new data")
        out_of_data = True
        return True
    return False
