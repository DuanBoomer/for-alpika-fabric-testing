# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "12c3a058-7da6-4848-9ae2-3c81f75777bf",
# META       "default_lakehouse_name": "Silver_Lakehouse",
# META       "default_lakehouse_workspace_id": "0ccfeef7-a13e-4fa2-8da0-fb99478b1333",
# META       "known_lakehouses": [
# META         {
# META           "id": "12c3a058-7da6-4848-9ae2-3c81f75777bf"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## **Push Pipelines Veeva PreFinal Testing :** 

# MARKDOWN ********************

# ### **Import Libraries**

# CELL ********************

import json, requests, pyspark.sql.functions as F
from urllib.parse import urlencode, quote_plus
from pyspark.sql.types import StructType, StructField, StringType
from notebookutils import mssparkutils   
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from urllib.parse import urlencode, quote_plus
from datetime import datetime, timedelta
import requests, json
import pytz
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, concat_ws, current_timestamp, expr, from_unixtime, least, min as spark_min

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Credentials**

# CELL ********************

USERNAME     = "nlagrotta@oraclinical.com"        
PASSWORD     = "Chs@9264910"                   
API_VERSION  = "v25.1"                         
VAULT_DOMAIN = "sb-oraclinical-ora-intrinsic-dw-i-sandbox"   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Paths**

# CELL ********************

SOURCE_PATH = (
    "abfss://0ccfeef7-a13e-4fa2-8da0-fb99478b1333"
    "@onelake.dfs.fabric.microsoft.com/45452c5c-41da-46be-9d22-333b8b9bec66"
    "/Tables/dbo/factvisitsv1"
)
SUMMARY_PATH = "Tables/summary_report_2"  # Silver lakehouse
CONFIG_PATH = "Tables/config_subject_visit"  # Silver Lakehouse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Implementations**

# CELL ********************

BASE_URL  = f"https://{VAULT_DOMAIN}.veevavault.com/api/{API_VERSION}"
QUERY_URL = f"{BASE_URL}/query"
VOBJ_URL  = f"{BASE_URL}/vobjects"

session_id = requests.post(
    f"{BASE_URL}/auth",
    data={"username": USERNAME, "password": PASSWORD}
).json()["sessionId"]

HEADERS = {
    "Authorization": session_id,
    "Accept"       : "application/json",
    "Content-Type" : "application/x-www-form-urlencoded"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Create and Update Functions**

# CELL ********************

def _handle_response(resp):
    try:
        return resp.json()
    except Exception as e: 
        raise Exception(f"Unexpected Error/Exception: {resp.text} (Status Code: {resp.status_code})")

def safe_first_row(obj, fields, where):
    try:
        r = requests.get(QUERY_URL, headers=HEADERS, params={"q": f"SELECT {', '.join(fields)} FROM {obj} WHERE {where}"})
        data = r.json().get("data", [])
        return data[0] if data else None
    except Exception as e:
        print(f"Error during safe_first_row for {obj}: {e}")
        return None

def safe_first_id(obj, where):
    row = safe_first_row(obj, ["id"], where)
    return row.get("id") if row else None

def vql_query_all(obj, fields, where_clause="", order_by_clause="", limit=1000):
    """
    Executes a VQL query to get all records matching the criteria, handling pagination.
    Args:
        obj (str): The Veeva Vault object API name (e.g., 'subject__clin').
        fields (list): A list of field API names to select.
        where_clause (str): The WHERE clause for the VQL query.
        order_by_clause (str): The ORDER BY clause for the VQL query (optional, good for pagination consistency).
        limit (int): The maximum number of records to fetch per request.
    Returns:
        list: A list of dictionaries, where each dictionary represents a record.
    """
    all_records = []
    offset = 0

    while True:
        q = f"select {', '.join(fields)} from {obj}"
        if where_clause:
            q += f" where {where_clause}"
        if order_by_clause:
            q += f" order by {order_by_clause}"
        q += f" limit {limit} offset {offset}"

       
        r = requests.get(QUERY_URL, headers=HEADERS, params={"q": q})
        response_data = _handle_response(r).get("data", [])

        if not response_data:
            break # No more records

        all_records.extend(response_data)
        offset += len(response_data)

        if len(response_data) < limit:
            break # Less than limit means it's the last page

    return all_records

# def create_vobj(obj, fields):
#     """
#     Creates a new Veeva Vault object record.
#     """
#     payload = urlencode(fields, quote_via=quote_plus)
#     r = requests.post(f"{VOBJ_URL}/{obj}", headers=HEADERS, data=payload)
#     return _handle_response(r)["data"]["id"]

def create_vobj(obj, fields):
    payload = urlencode(fields, quote_via=quote_plus)
    r = requests.post(f"{VOBJ_URL}/{obj}", headers=HEADERS, data=payload)
    try:
        resp_json = r.json()
    except Exception:
        print(f"Failed to parse JSON: {r.text}")
        raise
    if "data" not in resp_json:
        print(f"API error response: {resp_json}")
        raise Exception("API response missing 'data' key")
    return resp_json["data"]["id"]

def update_vobj(obj, rec_id, fields):
    """
    Updates an existing Veeva Vault object record.
    """
    payload = urlencode(fields, quote_via=quote_plus)
    r = requests.put(f"{VOBJ_URL}/{obj}/{rec_id}", headers=HEADERS, data=payload)
    return _handle_response(r)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Log Summary Functions**

# CELL ********************

def get_last_sync_time():
    config_table = "config_subject_visit"
    subject_name = "subject_visits"
    default_time = datetime.strptime("2023-10-02 00:00:00.000000", "%Y-%m-%d %H:%M:%S.%f")

    try:
        df = spark.read.table(config_table)
        filtered_df = df.filter(col("table_name") == subject_name).select("last_sync")
        ts_row = filtered_df.first()

        if ts_row is None or ts_row["last_sync"] is None:
            print(f"No valid 'last_sync' for '{subject_name}'. Returning default.")
            return default_time

        return ts_row["last_sync"]

    except AnalysisException:
        # If the config table doesn't exist, create it with default row
        df_default = spark.createDataFrame([(subject_name, default_time)], ["table_name", "last_sync"])
        df_default.write.mode("overwrite").saveAsTable(config_table)
        print(f"Table '{config_table}' not found. Created with default sync time: {default_time}")
        return default_time

# def get_last_sync_time():
#     default_time = datetime.strptime("2023-08-02 00:00:00.000000", "%Y-%m-%d %H:%M:%S.%f")
#     print("Static 'last_sync' time being used:", default_time)
#     return default_time

def update_last_sync_time(new_time: datetime):
    config_table = "config_subject_visit"
    subject_name = "subject_visits"

    df_new = spark.createDataFrame([(subject_name, new_time)], ["table_name", "last_sync"])
    df_new.write.mode("overwrite").saveAsTable(config_table)
    print(f"Updated '{config_table}' with new sync time for '{subject_name}': {new_time}")


# Log Schema - Added three more columns here on 14 july 
log_schema = StructType([
    StructField("subjectKey",        StringType(), True),
    StructField("study_key",         StringType(), True),
    StructField("site_name",         StringType(), True),
    StructField("responsestatus",    StringType(), True),
    StructField("method",            StringType(), True),
    StructField("error_message",     StringType(), True),
    StructField("inserted_date",     TimestampType(), True),
    StructField("type",              StringType(), True),
    StructField("visit_instance_id", StringType(), True)  # This is added for Subject-visits pipeline
])

def log_summary(subjectKey, study_key, site_name, status, method, err_msg=None, visit_instance_id=None):
    inserted_date = datetime.utcnow()
    record_type = "SubjectVisit"

    data = [(subjectKey, study_key, site_name, status, method, err_msg, inserted_date, record_type, visit_instance_id)]

    df_to_log = spark.createDataFrame(data, schema=log_schema)

    if DeltaTable.isDeltaTable(spark, SUMMARY_PATH):
        delta_table = DeltaTable.forPath(spark, SUMMARY_PATH)
        (
            delta_table.alias("t")
            .merge(
                df_to_log.alias("s"),
                """
                t.subjectKey = s.subjectKey AND
                t.study_key = s.study_key AND
                t.visit_instance_id = s.visit_instance_id AND
                t.method = s.method
                """
            )
            .whenMatchedUpdate(set={
                "responsestatus": "s.responsestatus",
                "error_message": "s.error_message",
                "inserted_date": "s.inserted_date"
            })
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # If table does not exist, create it
        df_to_log.write.format("delta").mode("overwrite").save(SUMMARY_PATH)
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Old**

# CELL ********************

# from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()

# # Define the table path
# table_path = "abfss://0ccfeef7-a13e-4fa2-8da0-fb99478b1333@onelake.dfs.fabric.microsoft.com/12c3a058-7da6-4848-9ae2-3c81f75777bf/Tables/summary_report_2"

# # Read the table schema and create an empty DataFrame
# empty_df = spark.read.format("delta").load(table_path).limit(0)

# # Overwrite the table with the empty DataFrame to delete all rows
# empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql import SparkSession

# # Create Spark session
# spark = SparkSession.builder.appName("CleanDeltaTable").getOrCreate()

# # Define the table path
# table_path = (
#     "abfss://0ccfeef7-a13e-4fa2-8da0-fb99478b1333@onelake.dfs.fabric.microsoft.com/"
#     "12c3a058-7da6-4848-9ae2-3c81f75777bf/Tables/summary_report_2"
# )

# # Load existing data
# df = spark.read.format("delta").load(table_path)

# print("Before cleaning row count:", df.count())

# # Filter out the bad rows
# filtered_df = df.filter(df.study_key != "24-110-0004")

# print("After cleaning row count:", filtered_df.count())

# # Overwrite the table with cleaned data
# filtered_df.write.format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "true") \
#     .save(table_path)

# print("Table has been cleaned and overwritten successfully ✅")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Subject Visits Common Validation & Context Preparation**

# CELL ********************

def prepare_subject_visit_context(row):
    row_dict = row.asDict() if hasattr(row, "asDict") else row
    study_key = row_dict["study_key"].strip()
    interval_name = row_dict["interval_name"].strip()
    visitid_instanceid = row_dict["visitid_instanceid"].strip()
    visit_date = row["visit_date"].strip() if row["visit_date"] else ""
    is_deleted = row_dict["is_deleted"] if row_dict["is_deleted"] is not None else 0
    subjectKey = row_dict["subjectKey"].strip()
    visit_type = row_dict["visit_type"].strip() if row_dict["visit_type"] else ""
    ieyn = row_dict["ieyn"].strip().lower() if row_dict["ieyn"] else "no"
    site_name = row["site_name"]

    # -------------------------
    # CHANGE #1: Enhanced Study Lookup with (Prod)
    # -------------------------
    study_row = safe_first_row(
        "study__v", ["id", "name__v", "external_id__v"],
        f"external_id__v = '{study_key}'"
    )

    if not study_row:
        prod_study_key = f"{study_key}(Prod)"
        study_row = safe_first_row(
            "study__v", ["id", "name__v", "external_id__v"],
            f"external_id__v = '{prod_study_key}'"
        )

    if not study_row:
        raise Exception(f"Study '{study_key}' or '{study_key} (Prod)' not found")

    study_id = study_row.get("id")
    matched_study_external_id = study_row.get("external_id__v")  # <-- NEW

    # -------------------------
    # CHANGE #2: Use matched_study_external_id in external IDs
    # -------------------------
    external_id = f"{matched_study_external_id}-{subjectKey}-{visitid_instanceid}"
    visit_def_external_id = f"{matched_study_external_id}-{interval_name}-SF"
    externalid_subject = f"{matched_study_external_id}-{subjectKey}"

    # --- SITE & COUNTRY ---
    site_name_truncated = site_name[:50]
    site_row = safe_first_row(
        "site__v", ["id", "study_country__v"],
        f"study__v = '{study_id}' and external_id__v = '{site_name_truncated}'"
    )
    if not site_row:
        raise Exception(f"Site '{site_name}' not found for study '{study_key}'")
    site_id = site_row.get("id")
    country_id = site_row.get("study_country__v")
    if not country_id:
        raise Exception(f"Missing country for site '{site_name}'")

    # --- SUBJECT ---
    subject_row = safe_first_row(
        "subject__clin",
        ["id"],
        f"external_id__v = '{externalid_subject}'"
    )
    if not subject_row:
        raise Exception(f"Subject '{subjectKey}' not found for study '{study_key}'")
    subject_id = subject_row.get("id")

    # --- VISIT DEF ---
    visit_type_val = (visit_type or "").lower()
    ieyn_val = row_dict["ieyn"].strip().lower() if row_dict["ieyn"] else None  

    if visit_type_val == "usv":
        category = "Unscheduled visit"
        visitdef_id = safe_first_id("visit_def__v", f"study__v = '{study_id}' and name__v = '{interval_name}'")

    elif ieyn_val == "yes":
        category = "Screenfail visit"
        visitdef_id = safe_first_id("visit_def__v", f"external_id__v = '{visit_def_external_id}'")

    else:  # Standard visit
        category = "Standard visit"
        visitdef_id = safe_first_id("visit_def__v", f"study__v = '{study_id}' and name__v = '{interval_name}'")

    if not visitdef_id:
        raise Exception(
            f"Visit Definition not found for interval='{interval_name}', VisitType='{category}'"
        )

    # --- VISIT (existing) ---
    existing_visits = vql_query_all(
        "visit__v",
        ["id"],
        f"external_id__v = '{external_id}'"
    )
    vault_visitid_instanceid = existing_visits[0]["id"] if existing_visits else None

    # --- Determine status ---
    if is_deleted in ["1", 1, "true", "True"]:
        visit_status = "deleted_in_cdms__v"
    elif visit_date:
        visit_status = "submitted__v"
    else:
        visit_status = "in_progress__v"

    visit_fields = {
        "external_id__v": external_id,
        "study__v": study_id,
        "site__v": site_id,
        "subject__v": subject_id,
        "study_country__v": country_id,
        "visit_date__v": visit_date,
        "visit_def__v": visitdef_id,
        "visit_status__v": visit_status,
        "reporting_status__c": "active__c"
    }

    return visit_fields, vault_visitid_instanceid


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Need to Test**

# CELL ********************

def prepare_subject_visit_context(row):
    row_dict = row.asDict() if hasattr(row, "asDict") else row
    study_key = row_dict["study_key"].strip()
    interval_name = row_dict["interval_name"].strip()
    visitid_instanceid = row_dict["visitid_instanceid"].strip()
    visit_date = row["visit_date"].strip() if row["visit_date"] else ""
    is_deleted = row_dict["is_deleted"] if row_dict["is_deleted"] is not None else 0
    subjectKey = row_dict["subjectKey"].strip()
    visit_type = row_dict["visit_type"].strip() if row_dict["visit_type"] else ""
    ieyn = row_dict["ieyn"].strip().lower() if row_dict["ieyn"] else "no"
    site_name = row["site_name"]
    source = row_dict.get("source", "").strip().lower()  # <-- NEW: Handle source

    # -------------------------
    # STUDY LOOKUP
    # -------------------------
    study_row = None

    if source == "medidata":
        # First try normal study_key
        study_row = safe_first_row(
            "study__v", ["id", "name__v", "external_id__v"],
            f"external_id__v = '{study_key}'"
        )

        # If not found, try with (Prod)
        if not study_row:
            prod_study_key = f"{study_key}(Prod)"
            study_row = safe_first_row(
                "study__v", ["id", "name__v", "external_id__v"],
                f"external_id__v = '{prod_study_key}'"
            )
    else:
        # Non-medidata: Only normal lookup
        study_row = safe_first_row(
            "study__v", ["id", "name__v", "external_id__v"],
            f"external_id__v = '{study_key}'"
        )

    if not study_row:
        raise Exception(
            f"Study not found for externalId '{study_key}'"
        )

    study_id = study_row.get("id")
    matched_study_external_id = study_row.get("external_id__v")  # Actual ID from Vault

    # -------------------------
    # Use matched external_id__v in all external IDs
    # -------------------------
    external_id = f"{matched_study_external_id}-{subjectKey}-{visitid_instanceid}"
    visit_def_external_id = f"{matched_study_external_id}-{interval_name}-SF"
    externalid_subject = f"{matched_study_external_id}-{subjectKey}"

    # --- SITE & COUNTRY ---
    site_name_truncated = site_name[:50]
    site_row = safe_first_row(
        "site__v", ["id", "study_country__v"],
        f"study__v = '{study_id}' and external_id__v = '{site_name_truncated}'"
    )
    if not site_row:
        raise Exception(f"Site '{site_name}' not found for study '{matched_study_external_id}'")
    site_id = site_row.get("id")
    country_id = site_row.get("study_country__v")
    if not country_id:
        raise Exception(f"Missing country for site '{site_name}'")

    # --- SUBJECT ---
    subject_row = safe_first_row(
        "subject__clin",
        ["id"],
        f"external_id__v = '{externalid_subject}'"
    )
    if not subject_row:
        raise Exception(f"Subject '{subjectKey}' not found for study '{matched_study_external_id}'")
    subject_id = subject_row.get("id")

    # --- VISIT DEF ---
    visit_type_val = (visit_type or "").lower()
    ieyn_val = row_dict["ieyn"].strip().lower() if row_dict["ieyn"] else None  

    if visit_type_val == "usv":
        category = "Unscheduled visit"
        visitdef_id = safe_first_id("visit_def__v", f"study__v = '{study_id}' and name__v = '{interval_name}'")

    elif ieyn_val == "no":
        category = "Screenfail visit"
        visitdef_id = safe_first_id("visit_def__v", f"external_id__v = '{visit_def_external_id}'")

    else:  # Standard visit
        category = "Standard visit"
        visitdef_id = safe_first_id("visit_def__v", f"study__v = '{study_id}' and name__v = '{interval_name}'")

    if not visitdef_id:
        raise Exception(
            f"Visit Definition not found for interval='{interval_name}', VisitType='{category}'"
        )

    # --- VISIT (existing) ---
    existing_visits = vql_query_all(
        "visit__v",
        ["id"],
        f"external_id__v = '{external_id}'"
    )
    vault_visitid_instanceid = existing_visits[0]["id"] if existing_visits else None

    # --- Determine status ---
    if is_deleted in ["1", 1, "true", "True"]:
        visit_status = "deleted_in_cdms__v"
    elif visit_date:
        visit_status = "submitted__v"
    else:
        visit_status = "in_progress__v"

    visit_fields = {
        "external_id__v": external_id,
        "study__v": study_id,
        "site__v": site_id,
        "subject__v": subject_id,
        "study_country__v": country_id,
        "visit_date__v": visit_date,
        "visit_def__v": visitdef_id,
        "visit_status__v": visit_status,
        "reporting_status__c": "active__c"
    }

    return visit_fields, vault_visitid_instanceid


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  ### **Retry Failed Subject-Visits**

# CELL ********************

def retry_failed_subject_visits():
    # Load dimsubject from OneLake
    dimsubject_df = (
        spark.read.format("delta")
        .load("abfss://0ccfeef7-a13e-4fa2-8da0-fb99478b1333@onelake.dfs.fabric.microsoft.com/"
              "45452c5c-41da-46be-9d22-333b8b9bec66/Tables/dbo/dimsubject")
        .select("studyKey", "subjectId", "siteName")
    )
    dimsubject_df = F.broadcast(dimsubject_df)

    # Load failed SubjectVisit records from summary path
    failed_df = (
        spark.read.format("delta").load(SUMMARY_PATH)
        .filter(
            (F.col("responsestatus") == "FAILURE") &
            (F.col("type") == "SubjectVisit")
        )
        .select("study_key", "visit_instance_id", "method", "subjectKey")
    )

    if failed_df.rdd.isEmpty():
        print("No failed SubjectVisit records to retry.")
        return 0

    # Get subjectId from source visit table for join with dimsubject
    visit_subject_df = (
        spark.read.format("delta").load(SOURCE_PATH)
        .select(
            F.col("studyKey").alias("v_studyKey"),
            F.col("visitid_instanceid").alias("v_visit_instance_id"),
            F.col("subjectId").alias("v_subjectId")
        )
        .distinct()
    )

    # Join failed_df with visit_subject_df to get subjectId
    failed_df = failed_df.join(
        visit_subject_df,
        (failed_df.study_key == visit_subject_df.v_studyKey) &
        (failed_df.visit_instance_id == visit_subject_df.v_visit_instance_id),
        "left"
    )

    # Join with dimsubject to get siteName
    failed_df = failed_df.join(
        dimsubject_df,
        (failed_df.study_key == dimsubject_df.studyKey) &
        (failed_df.v_subjectId == dimsubject_df.subjectId),
        "left"
    ).drop("studyKey", "subjectId", "v_studyKey", "v_visit_instance_id", "v_subjectId")

    retry_success_count = 0

    for row in failed_df.collect():
        study_key = row["study_key"]
        visit_instance_id = row["visit_instance_id"]
        subjectKey = row["subjectKey"]
        site_name = row["siteName"]

        try:
            # Load source data for this failed record
            src_data = (
                spark.read.format("delta").load(SOURCE_PATH)
                .filter(
                    (F.col("studyKey") == study_key) &
                    (F.col("visitid_instanceid") == visit_instance_id)
                )
                .select(
                    F.col("intervalName").cast("string").alias("interval_name"),
                    F.col("visitDate").cast("string").alias("visit_date"),
                    F.col("deleted").cast("string").alias("is_deleted"),
                    F.col("subjectKey").cast("string").alias("subjectKey"),
                    F.col("visitType").cast("string").alias("visit_type"),
                    F.col("ieyn").cast("string").alias("ieyn"),
                    F.col("source").cast("string").alias("source") #new
                )
                .limit(1)
                .collect()
            )

            if not src_data:
                raise Exception("Source data not found for failed record.")

            row_data = src_data[0]
            row_data_dict = {
                "study_key": study_key,
                "interval_name": row_data["interval_name"],
                "visitid_instanceid": visit_instance_id,
                "visit_date": row_data["visit_date"],
                "is_deleted": row_data["is_deleted"],
                "subjectKey": row_data["subjectKey"],
                "visit_type": row_data["visit_type"],
                "ieyn": row_data["ieyn"],
                "site_name": site_name,
                "source": row_data["source"] #new
            }

            # Format visit_date
            if row_data_dict["visit_date"]:
                raw_date = row_data_dict["visit_date"]
                try:
                    dt = datetime.strptime(raw_date, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    dt = datetime.strptime(raw_date, "%Y-%m-%d %H:%M:%S")
                row_data_dict["visit_date"] = dt.date().isoformat()

            # --- Prepare visit fields
            visit_fields, vault_visit_id = prepare_subject_visit_context(row_data_dict)

            external_id = visit_fields["external_id__v"]

            if vault_visit_id:
                # Update path - only send 3 fields
                method = "update"
                update_fields = {
                    "visit_date__v": visit_fields["visit_date__v"],
                    "visit_def__v": visit_fields["visit_def__v"],
                    "visit_status__v": visit_fields["visit_status__v"]
                }
                update_vobj("visit__v", vault_visit_id, update_fields)
                print(f"Retry update successful for visit: {external_id}")
                log_summary(subjectKey, study_key, site_name, "SUCCESSFUL", method, None, visit_instance_id)
            else:
                # Insert path - send full fields
                method = "insert"
                create_vobj("visit__v", visit_fields)
                print(f"Retry create successful for visit: {external_id}")
                log_summary(subjectKey, study_key, site_name, "SUCCESSFUL", method, None, visit_instance_id)

            retry_success_count += 1

        except Exception as e:
            err_msg = str(e)

            # infer method even on failure
            try:
                method = "update" if vault_visit_id else "insert"
            except NameError:
                method = "insert"  # default if vault_visit_id was never set

            print(f"Retry failed for visit_instance_id={visit_instance_id}: {err_msg}")
            log_summary(subjectKey, study_key, site_name, "FAILURE", method, err_msg, visit_instance_id)

    return retry_success_count


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Insert and Update Functions**

# CELL ********************

def sync_visits():
    retry_success_count = retry_failed_subject_visits()
    print(f"Retry of failed visits completed. Successful retries: {retry_success_count}")

    last_sync = get_last_sync_time()
    now_sync = datetime.utcnow()
    successful_pushes_count = 0

    print("Last Sync Time being used:", last_sync)

    # Load siteName from dimsubject table
    dimsubject_df = (
        spark.read.format("delta")
        .load(
            "abfss://0ccfeef7-a13e-4fa2-8da0-fb99478b1333@onelake.dfs.fabric.microsoft.com/"
            "45452c5c-41da-46be-9d22-333b8b9bec66/Tables/dbo/dimsubject"
        )
        .select(
            F.col("studyKey").alias("dim_studyKey"),
            F.col("subjectId").alias("dim_subjectId"),
            F.col("siteName").alias("site_name")
        )
    )
    dimsubject_df = F.broadcast(dimsubject_df)

    df = (
        spark.read.format("delta").load(SOURCE_PATH)
        .filter(
            (F.col("studyKey") == "SB-421a-006") &
            # (F.col("subjectKey") == "13-016") &
            (
                (F.col("dateCreated") >= F.lit(last_sync)) |
                (F.col("dateModified") >= F.lit(last_sync))
            )
        )
        .select(
            F.col("studyKey").cast("string").alias("study_key"),
            F.col("subjectId").cast("string").alias("subj_id"),
            F.col("intervalName").cast("string").alias("interval_name"),
            F.col("visitid_instanceid").cast("string").alias("visitid_instanceid"),
            F.col("visitDate").cast("string").alias("visit_date"),
            F.col("deleted").cast("string").alias("is_deleted"),
            F.col("subjectKey").cast("string").alias("subjectKey"),
            F.col("visitType").cast("string").alias("visit_type"),
            F.col("ieyn").cast("string").alias("ieyn"),
            F.col("source").cast("string").alias("source")
        )
    )

    # df = (
    #     spark.read.format("delta").load(SOURCE_PATH)
    #     .filter(
    #         (F.col("dateCreated") >= F.lit(last_sync)) |
    #         (F.col("dateModified") >= F.lit(last_sync))
    #     )
    #     .select(
    #         F.col("studyKey").cast("string").alias("study_key"),
    #         F.col("subjectId").cast("string").alias("subj_id"),
    #         F.col("intervalName").cast("string").alias("interval_name"),
    #         F.col("visitid_instanceid").cast("string").alias("visitid_instanceid"),
    #         F.col("visitDate").cast("string").alias("visit_date"),
    #         F.col("deleted").cast("string").alias("is_deleted"),
    #         F.col("subjectKey").cast("string").alias("subjectKey"),
    #         F.col("visitType").cast("string").alias("visit_type"),
    #         F.col("ieyn").cast("string").alias("ieyn"),
    #         F.col("source").cast("string").alias("source")
    #     )
    # )

    # display(df)
    # --- Count records ---
    total_count = df.count()
    print(f"Total records after filtering and selecting: {total_count}")

    if df.limit(1).count() == 0:
        print("No records found in source.")
        return

    # Join and keep site_name in consistent snake_case
    df = (
        df.join(
            dimsubject_df,
            (df.study_key == dimsubject_df.dim_studyKey) & (df.subj_id == dimsubject_df.dim_subjectId),
            "left"
        )
        .drop("dim_studyKey", "dim_subjectId")
    )

    for row in df.collect():
        subjectKey = None
        study_key = None
        site_name = None
        visitid_instanceid = None
        vault_visitid_instanceid = None
        visit_fields = None
        method = None 

        try:
            # Convert visit_date if present
            if row["visit_date"]:
                raw_date = row["visit_date"]
                try:
                    dt = datetime.strptime(raw_date, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    dt = datetime.strptime(raw_date, "%Y-%m-%d %H:%M:%S")
                visit_date_formatted = dt.date().isoformat()
            else:
                visit_date_formatted = None

            row_dict = row.asDict()
            row_dict["visit_date"] = visit_date_formatted

            subjectKey = row_dict.get("subjectKey")
            study_key = row_dict.get("study_key")
            site_name = row_dict.get("site_name")
            visitid_instanceid = row_dict.get("visitid_instanceid")

            # Check for missing site_name
            if not site_name:
                raise Exception(f"Site name missing for study '{study_key}' and subject '{subjectKey}'")

            # Call cache-free version
            visit_fields, vault_visitid_instanceid = prepare_subject_visit_context(row_dict)

            if vault_visitid_instanceid:
                # Update path - send only 3 fields
                method = "update"
                update_fields = {
                    "visit_date__v": visit_fields["visit_date__v"],
                    "visit_def__v": visit_fields["visit_def__v"],
                    "visit_status__v": visit_fields["visit_status__v"]
                }
                update_vobj("visit__v", vault_visitid_instanceid, update_fields)
                log_summary(subjectKey, study_key, site_name, "SUCCESSFUL", method, None, visitid_instanceid)
                print(f"Visit updated: {visit_fields['external_id__v']}")
            else:
                # Insert path - keep all fields
                method = "insert"
                create_vobj("visit__v", visit_fields)
                log_summary(subjectKey, study_key, site_name, "SUCCESSFUL", method, None, visitid_instanceid)
                print(f"Visit created: {visit_fields['external_id__v']}")

            successful_pushes_count += 1

        except Exception as e:
            err_msg = f"{e}"
            if not method:
                method = "insert" if not vault_visitid_instanceid else "update"

            log_summary(subjectKey, study_key, site_name, "FAILURE", method, err_msg=err_msg, visit_instance_id=visitid_instanceid)
            print(err_msg)

    if successful_pushes_count > 0:
        update_last_sync_time(now_sync)
        print(f"Visit sync complete. Last sync time updated to: {now_sync}")
    else:
        print("No successful visit pushes. Last sync time not updated.")

if __name__ == "__main__":                                                                              
    sync_visits()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql.functions import lit
# from datetime import datetime

# config_table = "config_subject_visit"
# subject_name = "subject_visits"
# new_sync_time = datetime.strptime("2020-07-02 00:00:00", "%Y-%m-%d %H:%M:%S")

# try:
#     # Read the existing config table
#     df = spark.read.table(config_table)
    
#     # Update the last_sync for the specific table
#     df_updated = df.withColumn(
#         "last_sync",
#         lit(new_sync_time)
#     ).where(df.table_name == subject_name).union(
#         df.filter(df.table_name != subject_name)
#     )

#     # Overwrite the table with updated values
#     df_updated.write.mode("overwrite").saveAsTable(config_table)
#     print(f"Updated 'last_sync' for '{subject_name}' to {new_sync_time}")

# except AnalysisException:
#     # If table doesn't exist, create it
#     df_default = spark.createDataFrame([(subject_name, new_sync_time)], ["table_name", "last_sync"])
#     df_default.write.mode("overwrite").saveAsTable(config_table)
#     print(f"Table '{config_table}' not found. Created with sync time: {new_sync_time}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
