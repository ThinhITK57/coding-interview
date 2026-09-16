from dotenv import load_dotenv

load_dotenv()


import requests
import time
import json
import re


from .config import *
from .http_util import *
from .crawler_util import *
from .ambari_util import *
# from prefect import task

SURVICATE_API_BASE = "https://data-api.survicate.com/v2"

SURVICATE_API_KEY = os.getenv("SURVICATE_API_KEY")

if not SURVICATE_API_KEY:
    raise Exception("Not found SURVICATE_API_KEY")

# SURVICATE_API_KEY = read_hdfs_https(SURVICATE_API_KEY_PATH).strip()
# API_COOKIE = read_hdfs_https(API_COOKIE_PATH).strip()

HEADERS = {
    "Authorization": "Basic {}".format(SURVICATE_API_KEY),
    "Accept": "application/json",
}

HIVE_DB = "cx_survicate_raw"

# ----------------------------------------------------------------------
## 🛠️ Utility Functions
# ----------------------------------------------------------------------

# @task
def extract_email_and_ticket(url):
    """Trích xuất email, ticket_id (id=) và uid/user_id từ URL."""
    if not url:
        return None, None, None
    clean_url = url.replace("&amp;", "&")

    def get_param(pattern):
        m = re.search(pattern, clean_url)
        if not m:
            return None
        val = m.group(1)
        if re.match(r"\{\{.*?\}\}", val):
            return None
        return val

    email = get_param(r"email=([^&]+)")
    ticket_id = get_param(r"id=([^&]+)")
    uid = get_param(r"(?:uid|user_id)=([^&]+)")
    return email, ticket_id, uid


def parse_attributes(respondent_uuid, attributes_data):
    """Phân tích và làm phẳng dữ liệu attributes để tạo bảng RespondentAttributes riêng."""
    if not attributes_data or not isinstance(attributes_data, list):
        return []

    parsed_rows = []
    for attr in attributes_data:
        row = {
            "respondent_uuid": respondent_uuid,
            "id": attr.get("id"),
            "name": attr.get("name"),
            "value": attr.get("value"),
        }
        parsed_rows.append(row)
    return parsed_rows


def extract_attribute_value(attributes_data, name_key):
    """Tìm kiếm value trong danh sách attributes dựa trên name_key."""
    if not attributes_data or not isinstance(attributes_data, list):
        return None

    for attr in attributes_data:
        if attr.get("name") == name_key:
            return attr.get("value")
    return None


def write_items(filename, items):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(json.dumps(items))


def read_items(filename):
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)


def fetch_all_with_next(url, params=None, headers=None):
    """
    Lấy tất cả dữ liệu từ API Survicate theo next_url
    """
    all_data = []
    params = params or {}

    while url:
        res = safe_request(
            url,
            headers=headers,
            params=params,
            max_retries=5,
            timeout=REQUEST_TIMEOUT,
            proxies=PROXIES if USE_PROXY else None,
        )

        if not res:
            return all_data

        if res.status_code != 200:
            print(f"Error {res.status_code}: {res.text}")
            break

        data_json = res.json()
        data = data_json.get("data") or []
        all_data.extend(data)

        # next_url trả về link đầy đủ
        pagination_data = data_json.get("pagination_data", {})
        has_more = pagination_data.get("has_more", False)
        next_url = pagination_data.get("next_url")
        if next_url and has_more:
            url = SURVICATE_API_BASE + next_url
        else:
            url = None
        # params = None  # next_url đã bao gồm params

        time.sleep(0.2)  # tránh rate limit

    return all_data


# --- Crawl toàn bộ surveys ---
def get_surveys(**kwargs):
    url = f"{SURVICATE_API_BASE}/surveys?items_per_page=100"
    return fetch_all_with_next(url, headers=HEADERS)


# --- Lấy chi tiết survey ---
def get_survey_details(survey_id, **kwargs):
    url = f"{SURVICATE_API_BASE}/surveys/{survey_id}"
    res = safe_request(
        url,
        headers=HEADERS,
        params=None,
        max_retries=5,
        timeout=REQUEST_TIMEOUT,
        proxies=PROXIES if USE_PROXY else None,
    )
    return res.json() if res.status_code == 200 else None


# --- Lấy tất cả questions của survey ---
def get_survey_question(survey_id):
    url = f"{SURVICATE_API_BASE}/surveys/{survey_id}/questions?items_per_page=100"
    return fetch_all_with_next(url, headers=HEADERS)


# --- Lấy tất cả responses của survey ---
def get_survey_responses(survey_id, start_time: str = None, end_time: str = None):
    url = f"{SURVICATE_API_BASE}/surveys/{survey_id}/responses?items_per_page=100"
    params = {}
    if start_time:
        # ISO 8601 : start=2023-01-01T00:00:00.000000Z
        url = url + "&start" + start_time
    if end_time:
        # ISO 8601 : end=2023-01-01T00:00:00.000000Z
        params["end"] = end_time
    return fetch_all_with_next(url, params=params, headers=HEADERS)


# --- Lấy attributes của respondent ---
def get_respondent_attributes(visitor_id):
    url = f"{SURVICATE_API_BASE}/respondents/{visitor_id}/attributes?items_per_page=100"
    res = safe_request(
        url,
        headers=HEADERS,
        params=None,
        max_retries=5,
        timeout=REQUEST_TIMEOUT,
        proxies=PROXIES if USE_PROXY else None,
    )
    return res.json() if res.status_code == 200 else {}


# --- Lấy tất cả responses của respondent ---
def get_respondent_responses(visitor_id, start_time: str = None):
    url = f"{SURVICATE_API_BASE}/respondents/{visitor_id}/responses?items_per_page=100"
    if start_time:
        # ISO 8601 : start=2023-01-01T00:00:00.000000Z
        url = url + "&start" + start_time
    return fetch_all_with_next(url, headers=HEADERS)


# --- Lấy personal data counters ---
def get_personal_data_counters(email):
    url = f"{SURVICATE_API_BASE}/personal-data?email={email}"
    res = safe_request(
        url,
        headers=HEADERS,
        params=None,
        max_retries=5,
        timeout=REQUEST_TIMEOUT,
        proxies=PROXIES if USE_PROXY else None,
    )
    return res.json() if res.status_code == 200 else {}


def push_record_to_hdfs(
    records, hdfs_base, resource_name, crawl_mode=None, schema_local_path=None, **kwargs
):
    print(resource_name)
    start_time = time.time()
    last_state = read_last_state(resource_name)
    print("Last state =", last_state)
    
    resource_dir = kwargs.get("resource_dir")
    state_dir = kwargs.get("state_dir")
    data_dir = kwargs.get("data_dir", "./tmp")

    if not records:
        print("No new data")

    # =========================
    # Pandas → Parquet
    # =========================

    now = datetime.now()
    partition_path = "{}/{}".format(hdfs_base, resource_name)

    filename = "data_{}_{}{:02d}{:02d}_{}{:02d}{:02d}.parquet".format(
        resource_name, now.year, now.month, now.day, now.hour, now.minute, now.second
    )
    local_parquet = "./tmp/data/cx_survicate_raw/{}/{}".format(resource_name, filename)
    os.makedirs(os.path.dirname(local_parquet), exist_ok=True)

    # df.to_parquet(local_parquet,engine="pyarrow", compression="snappy", index=False)
    records = convert_json_add_ts_columns(records)

    schema_tm_path = "./resources/parquet_schema/cx_survicate_raw/{}.json".format(resource_name)
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
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

    pq.write_table(table, local_parquet, compression="snappy")

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
        db=HIVE_DB,
        table=resource_name,
        location="{}/{}".format(hdfs_base, resource_name),
    )

    # filename = "create_table_{}_{}{:02d}{:02d}.sql".format(
    #     resource_name,
    #     now.hour,
    #     now.minute,
    #     now.second
    # )

    filename = "create_table_{}.sql".format(resource_name)

    local_sql = "./tmp/data/cx_survicate_raw/{}/{}".format(resource_name, filename)
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
    if "updated_at" in df.columns:
        max_ts = str(df["updated_at"].max())
        write_last_state(max_ts, resource_name)

    elapsed = time.time() - start_time
    print("Loop {} took {:.3f}s".format(resource_name, elapsed))


# --- Hàm crawl tổng thể ---
def crawl_surveys(**kwargs):
    surveys = get_surveys()
    hdfs_base = "/opt/datasets/crawlers/vcs/survicate/data"
    resource_name = "dim_survicate_surveys"
    push_record_to_hdfs(surveys, hdfs_base, resource_name, crawl_mode="static", **kwargs)

    # result["surveys"] = []
    data_dir = kwargs.get("data_dir","./tmp" )
    survey_ids = [s.get("id") for s in surveys]
    write_items(data_dir + "/survicate_survey_ids.json", survey_ids)
    return survey_ids


def crawl_survey_details(survey_ids, **kwargs):
    records = []
    for survey_id in survey_ids:
        detail = get_survey_details(survey_id)
        detail["survey_id"] = survey_id
        records += [detail]

    hdfs_base = "/opt/datasets/crawlers/vcs/survicate/data"
    resource_name = "dim_survicate_survey_details"
    push_record_to_hdfs(records, hdfs_base, resource_name, crawl_mode="static")


def crawl_survey_question(survey_ids, **kwargs):
    records = []
    for survey_id in survey_ids:
        items = get_survey_question(survey_id)
        for r in items:
            r["survey_id"] = survey_id
        records += items

    hdfs_base = "/opt/datasets/crawlers/vcs/survicate/data"
    resource_name = "dim_survicate_questions"
    push_record_to_hdfs(records, hdfs_base, resource_name, crawl_mode="static")


def crawl_survey_responses(survey_ids, **kwargs):
    data_dir = kwargs.get("data_dir", "./tmp")
    state_dir = kwargs.get("state_dir", "./tmp")
    resource_dir = kwargs.get("resource_dir", "./tmp")
    records = []
    emails = []
    visitor_ids = []
    for survey_id in survey_ids:
        responses = get_survey_responses(survey_id)
        respondent_items = []
        for r in responses:
            visitor_id = r.get("visitor_id") or r.get("respondent_id")
            if visitor_id:
                visitor_ids += [visitor_id]
            respondent_data = {
                "survey_id": survey_id,
                "response": r,
                "attributes": (
                    get_respondent_attributes(visitor_id) if visitor_id else {}
                ),
                "all_responses": (
                    get_respondent_responses(visitor_id) if visitor_id else []
                ),
            }
            respondent_items += [respondent_data]
            email, ticket_id, uid = extract_email_and_ticket(r.get("url"))
            r["cso_ticket_id"] = ticket_id
            r["cso_uid"] = uid
            r["cso_email"] = email

            if email:
                emails += [email]

        records += respondent_items

    write_items(data_dir + "/survicate_emails.json", emails)
    write_items(data_dir + "/survicate_visitor_ids.json", visitor_ids)

    hdfs_base = "/opt/datasets/crawlers/vcs/survicate/data"
    resource_name = "fact_survicate_responses"
    push_record_to_hdfs(records, hdfs_base, resource_name, crawl_mode="static")


def crawl_personal_data(emails, **kwargs):
    records = []
    from urllib.parse import unquote

    for email in emails:
        email = unquote(email).strip()
        item = get_personal_data_counters(email)
        item["email"] = email
        records += [item]

    hdfs_base = "/opt/datasets/crawlers/vcs/survicate/data"
    resource_name = "fact_survicate_personal_data"
    push_record_to_hdfs(records, hdfs_base, resource_name, crawl_mode="static")
