
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone, timedelta
from io import BytesIO
import boto3
import time
from .config import *
from .http_util import *
from .crawler_util import *
from .ambari_util import *
import zipfile


VN_TZ = timezone(timedelta(hours=7))

CSV_ZIP = ".csv.zip"
JSON_GZ = ".json.gz"
JSON_ONLY = ".json"

COLUMN_MAPPING_BY_TABLE = {
    "sprints": {
        "ID": "id",
        "NAME": "name",
        "RAPID_VIEW_ID": "rapid_view_id",
        "SEQUENCE": "sequence",
        "GOAL": "goal",
        "CLOSED": "closed",
        "STARTED": "started",
        "AUTO_START_STOP": "auto_start_stop",
        "START_DATE": "start_date",
        "END_DATE": "end_date",
        "COMPLETE_DATE": "complete_date",
        "ACTIVATED_DATE": "activated_date"
    },

    "projects": {
        "ID": "id",
        "pname": "pname",
        "URL": "url",
        "LEAD": "lead",
        "DESCRIPTION": "description",
        "pkey": "pkey",
        "pcounter": "pcounter",
        "ASSIGNEETYPE": "assigneetype",
        "AVATAR": "avatar",
        "ORIGINALKEY": "originalkey",
        "PROJECTTYPE": "projecttype"
    },

    "custom_fields": {
        "ID": "id",
        "cfkey": "cf_key",
        "CUSTOMFIELDTYPEKEY": "custom_field_type_key",
        "CUSTOMFIELDSEARCHERKEY": "custom_field_searcher_key",
        "cfname": "cf_name",
        "DESCRIPTION": "description",
        "defaultvalue": "default_value",
        "FIELDTYPE": "field_type",
        "PROJECT": "project",
        "ISSUETYPE": "issue_type",
        "lastvalueupdate": "last_value_update",
        "issueswithvalue": "issues_with_value"
    },

    "users": {
        "ID": "id",
        "directory_id": "directory_id",
        "user_name": "user_name",
        "lower_user_name": "lower_user_name",
        "active": "active",
        "created_date": "created_date",
        "updated_date": "updated_date",
        "first_name": "first_name",
        "lower_first_name": "lower_first_name",
        "last_name": "last_name",
        "lower_last_name": "lower_last_name",
        "display_name": "display_name",
        "lower_display_name": "lower_display_name",
        "email_address": "email_address",
        "lower_email_address": "lower_email_address",
        "CREDENTIAL": "credential",
        "deleted_externally": "deleted_externally",
        "EXTERNAL_ID": "external_id"
    },

    "versions": {
        "ID": "id",
        "PROJECT": "project",
        "vname": "vname",
        "DESCRIPTION": "description",
        "SEQUENCE": "sequence",
        "RELEASED": "released",
        "ARCHIVED": "archived",
        "URL": "url",
        "STARTDATE": "startdate",
        "RELEASEDATE": "releasedate"
    },

    "app_users": {
        "ID": "id",
        "user_key": "user_key",
        "lower_user_name": "lower_user_name"
    },

    "custom_field_options": {
        "ID": "id",
        "CUSTOMFIELD": "customfield",
        "CUSTOMFIELDCONFIG": "customfieldconfig",
        "PARENTOPTIONID": "parentoptionid",
        "SEQUENCE": "sequence",
        "customvalue": "customvalue",
        "optiontype": "optiontype",
        "disabled": "disabled"
    },

    "jira_issues": {
        "ID": "id",
        "pkey": "pkey",
        "issuenum": "issuenum",
        "PROJECT": "project",
        "REPORTER": "reporter",
        "ASSIGNEE": "assignee",
        "CREATOR": "creator",
        "issuetype": "issuetype",
        "SUMMARY": "summary",
        "DESCRIPTION": "description",
        "ENVIRONMENT": "environment",
        "PRIORITY": "priority",
        "RESOLUTION": "resolution",
        "issuestatus": "issuestatus",
        "CREATED": "created",
        "UPDATED": "updated",
        "DUEDATE": "duedate",
        "RESOLUTIONDATE": "resolutiondate",
        "VOTES": "votes",
        "WATCHES": "watches",
        "TIMEORIGINALESTIMATE": "timeoriginalestimate",
        "TIMEESTIMATE": "timeestimate",
        "TIMESPENT": "timespent",
        "WORKFLOW_ID": "workflow_id",
        "SECURITY": "security",
        "FIXFOR": "fixfor",
        "COMPONENT": "component",
        "ARCHIVED": "archived",
        "ARCHIVEDBY": "archivedby",
        "ARCHIVEDDATE": "archiveddate"
    },

    "custom_field_values": {
        "ID": "id",
        "ISSUE": "issue",
        "CUSTOMFIELD": "customfield",
        "PARENTKEY": "parentkey",
        "STRINGVALUE": "stringvalue",
        "NUMBERVALUE": "numbervalue",
        "TEXTVALUE": "textvalue",
        "DATEVALUE": "datevalue",
        "VALUETYPE": "valuetype"
    },

     "version_issue": {
        "SOURCE_NODE_ID": "source_node_id",
        "SOURCE_NODE_ENTITY": "source_node_entity",
        "SINK_NODE_ID": "sink_node_id",
        "SINK_NODE_ENTITY": "sink_node_entity",
        "ASSOCIATION_TYPE": "association_type",
        "SEQUENCE": "sequence"
    },

    "work_logs": {
        "ID": "id",
        "issueid": "issueid",
        "AUTHOR": "author",
        "grouplevel": "grouplevel",
        "rolelevel": "rolelevel",
        "worklogbody": "worklogbody",
        "CREATED": "created",
        "UPDATEAUTHOR": "updateauthor",
        "UPDATED": "updated",
        "STARTDATE": "startdate",
        "timeworked": "timeworked"
    }


}


COLUMN_MAPPING = {

    #Global
        "startedDate": "started",
        "createdDate": "created",
        "updatedDate": "updated",
        "projectKey": "projectkey",
        "projectName": "projectname",
        "categoryName": "categoryname",
        "authorFullName": "authorfullname",
        "authorUserName": "authorusername",
    
    #Custom fields
        'customfield_16800': 'ttsx_product', # TTSX Product
        'customfield_13370': 'total_net_effort', #Total Estimated Effort net of reuse
        "customfield_20800": "quarterly_kpi_result",
        "customfield_18000": "monthly_accumulated_result",
        "customfield_13465": "execution_result",
        "customfield_18900": "reporting_date",
        "customfield_19000": "service_product",
        "customfield_13365": "target",
        "customfield_14306": "condition",
        "customfield_13452": "unit_of_measure",
        "customfield_17001": "ttqt_product_name",
        "customfield_17000": "process_compliance_rate",
        "customfield_20410": "agent_online",
        "customfield_20409": "agent_latest",
        "customfield_20401": "has_soc247",
        "customfield_20411": "is_exception",
        "customfield_20405": "latest_version",
        "customfield_20400": "contract_type",
        "customfield_20404": "product_type",
        "customfield_20412": "exception_reason",
        # "customfield_18900": "report_date", #Overlap
        "customfield_20406": "update_start_date",
        "customfield_20407": "update_date",
        "customfield_20403": "product_name",
        "customfield_20402": "update_frequency",
        "customfield_12333": "version",
        "customfield_20408": "latency_delay",
        "customfield_16300": "assignor",
        "customfield_16301": "work_group",
        "customfield_16400": "department_center",
        "customfield_10700": "start_date",
        "customfield_13473": "task_type",
        # "customfield_11904": "msn_type",
        "customfield_11901": "msn_type",
        "customfield_12200": "project_source",
        "customfield_13438": "pm_sdm",
        "customfield_13526": "contract_plan_start_date",
        "customfield_13527": "contract_plan_end_date",
        "customfield_13533": "project_score"
}

def create_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_JIRA_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_JIRA_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_JIRA_SECRET_KEY"),
        verify=os.getenv("MINIO_JIRA_SECURE", "false") == "true",
        
    )

def upload_data_to_hdfs(resource_name, records, **kwargs):
    resource_dir = kwargs.get("resource_dir")
    data_dir = kwargs.get("data_dir")
    
    print("Start crawl : ", resource_name)

    HDFS_BASE = "/opt/datasets/crawlers/vcs/jira/data"
    HIVE_DB = "jira_raw"
    crawl_mode = kwargs.get("crawl_mode", "modified_and_new")
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

    schema_tm_path = resource_dir + "/parquet_schema/jira/{}.json".format(resource_name)
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

def parse_data_as_json(data, cate_name):
    records = []
    
    with zipfile.ZipFile(BytesIO(data), 'r') as z:
        file_list = z.namelist()
        
        csv_files = [f for f in file_list if f.endswith('.csv')]
        
        for file_name in csv_files:
            print(f"Đang đọc file: {file_name}")
            with z.open(file_name) as f:
                df = pd.read_csv(f)
                df['source_file'] = file_name
                if COLUMN_MAPPING:
                    df = df.rename(columns=COLUMN_MAPPING, errors='ignore')
                records.extend(df.to_dict(orient="records"))

    return records

