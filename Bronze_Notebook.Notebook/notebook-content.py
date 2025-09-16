# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e6e7f31f-0c00-454b-8243-0e8d26389396",
# META       "default_lakehouse_name": "Bronze_Lakehouse",
# META       "default_lakehouse_workspace_id": "0ccfeef7-a13e-4fa2-8da0-fb99478b1333",
# META       "known_lakehouses": [
# META         {
# META           "id": "e6e7f31f-0c00-454b-8243-0e8d26389396"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "1242970d-288d-4d30-a5cb-9c0f16c40bfc",
# META       "workspaceId": "0ccfeef7-a13e-4fa2-8da0-fb99478b1333"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ###### **Import libraries**

# CELL ********************

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_str
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit
from urllib.parse import quote_plus
import re
from log_custom import fabric_log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Define Variables**

# CELL ********************

# date = "2025-04-24T23:59:59Z"

# # filter = f"dateCreated>{date},dateModified>{date}"
# min_date_str = "2025-04-24T23:59:59Z"
# max_date= "2025-04-28T23:59:59Z"

# filter_str = (
#         f"dateCreated>{min_date_str} and dateCreated<{max_date} or dateModified>{min_date_str} and dateModified<{max_date}"
# )

# encoded_filter = quote_plus(filter)
# print(encoded_filter)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# dynamic date filter

from urllib.parse import quote_plus
from datetime import datetime, timedelta
import pandas as pd

# Step 1: Get current UTC timestamp - 1 day
max_date = (datetime.utcnow()).strftime("%Y-%m-%dT%H:%M:%SZ")
print("Max Date:", max_date)

# Step 2: Read config table
config_df = spark.table("config_table")  # Adjust if needed
config_pdf = config_df.toPandas()

# Step 3: Build records for DataFrame
records = []

for _, row in config_pdf.iterrows():
    table_name = row["Table"]
    min_date = row["Latest_Date"]

    if pd.isna(min_date):
        print(f"Skipping {table_name} — no Latest_Date found.")
        continue

    try:
        min_date_str = pd.to_datetime(min_date).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        print(f"Error parsing date for {table_name}: {e}")
        continue

    # Construct filter string
    filter_str = (
        f"dateCreated>{min_date_str} and dateCreated<{max_date} or dateModified>{min_date_str} and dateModified<{max_date}"
    )

    # Add record to list
    records.append({
        "table_name": table_name,
        "min_date": min_date_str,
        "max_date": max_date,
        "filter": filter_str
    })

# Step 4: Convert list to DataFrame
filters_pdf = pd.DataFrame(records)
# filters_pdf.index = filters_pdf['table_name'] # table name exists as a index and a col
filters_pdf = filters_pdf.set_index('table_name') # table name only exists as a index, not a col
filters_df = spark.createDataFrame(filters_pdf)

# # Show or use filters_df
# display(filters_pdf)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Config Table**

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import lit
from datetime import datetime
 
def update_config_date(table_name: str, iso_date: str):
    formatted = datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
 
    delta_table_name = "config_table"  # or fully qualified like "OnBoardingLakehouse.config_table"
    delta_table = DeltaTable.forName(spark, delta_table_name)

    delta_table.update(
            condition=f"Table = '{table_name}'",
            set={"Latest_Date": lit(formatted)}
        )
    
    print(f" Config table updated for '{table_name}' with date '{formatted}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **API Definition**

# CELL ********************

def get_data_imednet(endpoint, filter, deltatable, schema, mode, table):
    if table == "visits":
        url = f"https://edc.prod.imednetapi.com/api/v1/edc/{endpoint}?size=500"
    else:
        url = f"https://edc.prod.imednetapi.com/api/v1/edc/{endpoint}?size=500&filter={filter}"

    headers = {
        "x-api-key": "caguL0WXmO3ZcC5pChes62qD5KrlLwEfakRJ0IYJ",
        "x-imn-security-key": "b34aa85c-d900-4f68-8b92-cb380ec93ef2"
    }
    # get and store initial data
    response = requests.get(url, headers=headers)
    # print(url)
    # print(response.text)
    # print(response.status_code)
    if response.status_code == 200:
        data = response.json()["data"]
        df_data=spark.createDataFrame(data,schema=schema)
        timestamp = response.json()['metadata']['timestamp']
        df_data = df_data.withColumn("timestamp", lit(timestamp))
        
        if not (endpoint == 'studies'):
            # Extract studyKey using regex
            match = re.search(rf"studies/([^/]+)/{table}", endpoint)
            study_key = match.group(1)
            # print(study_key)
            df_data = df_data.withColumn("studyKey", lit(study_key))
        
        df_data.write.mode(f"{mode}").option("mergeSchema", "true").format("delta").saveAsTable(deltatable)
 
        pages = response.json()['pagination']['totalPages']
        # print(pages)
    else:
        print('error occured when fetching data with status code', response.status_code)
        pages = 1
    
    # apply pagination for respective pages
    for i in range(1,pages):
        url =f"{url}&page={i}"
        # get and store data
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()["data"]
            df_data=spark.createDataFrame(data,schema=schema)
            timestamp = response.json()['metadata']['timestamp']
            df_data = df_data.withColumn("timestamp", lit(timestamp))
            df_data.write.mode(f"{mode}").option("mergeSchema", "true").format("delta").saveAsTable(deltatable)
        else:
            print('error occured when fetching data for', study_key, 'with status code', response.status_code)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### testing for log table

# CELL ********************

import datetime
import re
from pyspark.sql.functions import lit
import requests

def get_data_imednet(endpoint, filter, deltatable, schema, mode, table):
    if table == "visits":
        url = f"https://edc.prod.imednetapi.com/api/v1/edc/{endpoint}?size=500"
    else:
        url = f"https://edc.prod.imednetapi.com/api/v1/edc/{endpoint}?size=500&filter={filter}"

    headers = {
        "x-api-key": "caguL0WXmO3ZcC5pChes62qD5KrlLwEfakRJ0IYJ",
        "x-imn-security-key": "b34aa85c-d900-4f68-8b92-cb380ec93ef2"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()["data"]

        if not data:
            return 0  # No data loaded

        # Convert first page of data to DataFrame
        raw_df = spark.createDataFrame(data)

        for field in schema.fields:
            raw_df = raw_df.withColumn(field.name, raw_df[field.name].cast(field.dataType))

        timestamp = response.json()['metadata']['timestamp']
        raw_df = raw_df.withColumn("timestamp", lit(timestamp))

        study_key = "n/a"
        if endpoint != 'studies':
            match = re.search(rf"studies/([^/]+)/{table}", endpoint)
            if match:
                study_key = match.group(1)
                raw_df = raw_df.withColumn("studyKey", lit(study_key))

        raw_df.write.mode(mode).option("mergeSchema", "true").format("delta").saveAsTable(deltatable)

        total_rows = raw_df.count()  # First page count

        # === Handle pagination ===
        pages = response.json().get('pagination', {}).get('totalPages', 1)

        for i in range(1, pages):
            page_url = f"{url}&page={i}"
            page_response = requests.get(page_url, headers=headers)

            if page_response.status_code == 200:
                page_data = page_response.json().get("data", [])
                if not page_data:
                    continue

                page_df = spark.createDataFrame(page_data)

                for field in schema.fields:
                    page_df = page_df.withColumn(field.name, page_df[field.name].cast(field.dataType))

                page_timestamp = page_response.json().get('metadata', {}).get('timestamp', datetime.datetime.now().isoformat())
                page_df = page_df.withColumn("timestamp", lit(page_timestamp))

                if endpoint != 'studies' and study_key != "n/a":
                    page_df = page_df.withColumn("studyKey", lit(study_key))

                page_df.write.mode(mode).option("mergeSchema", "true").format("delta").saveAsTable(deltatable)
                total_rows += page_df.count()
            else:
                print('Error occurred when fetching data for', study_key, 'with status code', page_response.status_code)

        return total_rows

    except Exception as e:
        raise  # Let the calling function handle error logging


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Studies**

# CELL ********************

# # Table studies
# # studyKey, subjectId, subjectOid, annotationType, annotationId, type, description, recordId, variable, subjectKey, dateCreated, dateModified, queryComments

# schemaStudies = StructType([
# StructField("studyKey", StringType(), True),
# StructField("studyId", StringType(), True),
# StructField("studyName", StringType(), True),
# StructField("studyType", StringType(), True),
# StructField("studyDescription", StringType(), True),
# StructField("sponsorKey", StringType(), True),
# StructField("dateCreated", StringType(), True),
# StructField("dateModified", StringType(), True)
# ])
# try:  
#     # Get filter from filters_pdf
#     row_subjects = filters_pdf.loc["stg_studies"]
#     filter= row_subjects["filter"]
#     mode = "overwrite"
#     endpoint = "studies"
# #    date= max_date
#     get_data_imednet(endpoint,filter, "stg_studies", schemaStudies, mode,"studies")
#     update_config_date("i_studies", max_date)  # only run if above succeeds
# except Exception as e:
#     print(f" Error running get_data_imednet for 'stg_studies': {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Table studies
# # studyKey, subjectId, subjectOid, annotationType, annotationId, type, description, recordId, variable, subjectKey, dateCreated, dateModified, queryComments

# schemaStudies = StructType([
# StructField("studyKey", StringType(), True),
# StructField("studyId", StringType(), True),
# StructField("studyName", StringType(), True),
# StructField("studyType", StringType(), True),
# StructField("studyDescription", StringType(), True),
# StructField("sponsorKey", StringType(), True),
# StructField("dateCreated", StringType(), True),
# StructField("dateModified", StringType(), True)
# ])

# any_data_loaded = False
# start_time = datetime.now()
# total_rows=0
# i = 0

# try:  
#     # Get filter from filters_pdf
#     row_subjects = filters_pdf.loc["stg_studies"]
#     filter= row_subjects["filter"]
#     mode = "overwrite"
#     endpoint = "studies"
# #    date= max_date
#     row_count = get_data_imednet(endpoint, filter, "stg_studies", schemaStudies, mode, "studies")  # now returns int (or 0 if no data)
#     if row_count > 0:
#                 total_rows += row_count
#                 any_data_loaded = True

#     print(f"Total rows loaded: {total_rows}")
  
#     # Log only once at the end
#     if any_data_loaded:
#         fabric_log.log_api_execution(
#             spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
#             error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
#             custom_description="API Execution"
#         )
#     else:
#         fabric_log.log_api_execution(
#             spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
#             error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
#             custom_description="No data available"
#         )

#    # update_config_date("stg_studies", max_date)  # only run if above succeeds
# except Exception as e:
#      # Default fallback
#     status_code = 500
#     error_message = str(e)

#     # If the exception is from requests and has a response with status_code
#     if hasattr(e, 'response') and e.response is not None:
#         status_code = e.response.status_code
#         error_message = f"{status_code} {e.response.reason}: {e.response.text}"

#     print(f"Error during data retrieval : {error_message}")
#     fabric_log.log_api_execution(
#         spark, table="studies", log_type="Error", etl_status="Fail", layer="Bronze",
#         data_source="Mednet", error=error_message, status_code=status_code,row_count=total_rows,
#         start_time=start_time,custom_description=f"Exception occurred: {error_message}"
#     )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **List of Study Keys**

# CELL ********************

df_studykey = spark.sql("SELECT studyKey FROM Silver_Lakehouse.i_dim_studies")
values = df_studykey.select("studyKey").rdd.flatMap(lambda x: x).collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # df_studykey = spark.sql("SELECT studyKey FROM Silver_Lakehouse.i_dim_studies")
# # values = df_studykey.select("studyKey").rdd.flatMap(lambda x: x).collect()
# df_studykey = spark.sql("""
#     SELECT DISTINCT studyKey FROM (
#         SELECT studyKey FROM Silver_Lakehouse.i_dim_studies
#         UNION
#         SELECT external_id__v as studyKey FROM Bronze_Lakehouse.v_sites_all
#     )
#     WHERE studyKey IS NOT NULL
# """)

# values = df_studykey.select("studyKey").rdd.flatMap(lambda x: x).collect()
# print(values)

# # Check if all values are non-null
# assert all(v is not None for v in values), "There are null values in the result!"

# # Check if all values are unique
# assert len(values) == len(set(values)), "There are duplicate studyKeys!"

# print("All studyKeys are non-null and unique.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Sites**

# CELL ********************

# Define schema for sites table
schemaSites = StructType([
StructField("siteId", StringType(), True),
StructField("siteName", StringType(), True),
StructField("siteEnrollmentStatus", StringType(), True),
StructField("dateCreated", StringType(), True),
StructField("dateModified", StringType(), True)
])
i=0
try:
    # Get filter for 'sites' using the index
    row = filters_pdf.loc["i_sites"]
    filter = row["filter"]
    
    for studyKey in values:
        i = i+1
        # print(studyKey)
        # endpoint = f"studies/PRJ1013/sites"
        endpoint = f"studies/{studyKey}/sites"
        table="sites"
        if(i==1):
            mode = "overwrite"
        else:
            mode = "append"
        # print(i)
        # print(mode)
        # print(f" Encoded Filter: {filter}")
        get_data_imednet(endpoint, filter, "i_sites", schemaSites, mode,table)
    # update config table with max_date
    # update_config_date("i_sites", max_date)

except Exception as e:
    print(f" Error during data retrieval for 'sites': {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

# Define schema for sites table
schemaSites = StructType([
StructField("siteId", StringType(), True),
StructField("siteName", StringType(), True),
StructField("siteEnrollmentStatus", StringType(), True),
StructField("dateCreated", StringType(), True),
StructField("dateModified", StringType(), True)
])

any_data_loaded = False
start_time = datetime.now()
total_rows=0
i = 0

try:
    # Get filter for 'sites' using the index
    row = filters_pdf.loc["i_sites"]
    filter = row["filter"]
    
    for studyKey in values:
        i = i+1
        # print(studyKey)
        # endpoint = f"studies/PRJ1013/sites"
        endpoint = f"studies/{studyKey}/sites"
        table="sites"
        if(i==1):
            mode = "overwrite"
        else:
            mode = "append"
        # print(i)
        # print(mode)
        # print(f" Encoded Filter: {filter}")
        row_count = get_data_imednet(endpoint, filter, "i_sites", schemaSites, mode, table)  # now returns int (or 0 if no data)
        if row_count > 0:
                total_rows += row_count
                any_data_loaded = True

    print(f"Total rows loaded: {total_rows}")

    # Log only once at the end
    if any_data_loaded:
        fabric_log.log_api_execution(
            spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
            error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
            custom_description="API Execution"
        )
    else:
        fabric_log.log_api_execution(
            spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
            error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
            custom_description="No data available"
        )
    # update config table with max_date
    # update_config_date("i_sites", max_date)

except Exception as e:
    # Default fallback
    status_code = 500
    error_message = str(e)

    # If the exception is from requests and has a response with status_code
    if hasattr(e, 'response') and e.response is not None:
        status_code = e.response.status_code
        error_message = f"{status_code} {e.response.reason}: {e.response.text}"

    print(f"Error during data retrieval: {error_message}")
    fabric_log.log_api_execution(
        spark, table="sites", log_type="Error", etl_status="Fail", layer="Bronze",
        data_source="Mednet", error=error_message, status_code=status_code,row_count=total_rows,
        start_time=start_time,custom_description=f"Exception occurred: {error_message}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_Lakehouse.execution_log LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_Lakehouse.i_sites LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Subjects**

# CELL ********************

# Define schema for subjects table
schemasubjects = StructType([
    StructField("studyKey", StringType(), True),
    StructField("subjectId", StringType(), True),
    StructField("subjectOid", StringType(), True),
    StructField("subjectKey", StringType(), True),
    StructField("subjectStatus", StringType(), True),
    StructField("siteId", StringType(), True),
    StructField("siteName", StringType(), True),
    StructField("deleted", StringType(), True),
    StructField("enrollmentStartDate", StringType(), True),
    StructField("dateCreated", StringType(), True),
    StructField("dateModified", StringType(), True),
    StructField("keywords", StringType(), True) 
])

i=0
try:  
    # Get filter for 'stg_subjects' from filters_pdf
    row_subjects = filters_pdf.loc["i_subjects"]
    filter= row_subjects["filter"]

    for studyKey in values:
        i = i+1
        endpoint = f"studies/{studyKey}/subjects"
        table="subjects"
        if(i==1):
            mode = "overwrite"
        else:
            mode = "append"
        # print(i)
        # print(mode)
        get_data_imednet(endpoint,filter, "i_subjects", schemasubjects, mode,table)
    # update config table with max_date
    update_config_date("i_subjects", max_date)
except Exception as e:
    print(f" Error running get_data_imednet for 'i_subjects': {str(e)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_Lakehouse.i_subjects LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Define schema for subjects table
# schemasubjects = StructType([
#     StructField("studyKey", StringType(), True),
#     StructField("subjectId", StringType(), True),
#     StructField("subjectOid", StringType(), True),
#     StructField("subjectKey", StringType(), True),
#     StructField("subjectStatus", StringType(), True),
#     StructField("siteId", StringType(), True),
#     StructField("siteName", StringType(), True),
#     StructField("deleted", StringType(), True),
#     StructField("enrollmentStartDate", StringType(), True),
#     StructField("dateCreated", StringType(), True),
#     StructField("dateModified", StringType(), True),
#     StructField("keywords", StringType(), True) 
# ])


# any_data_loaded = False
# start_time = datetime.now()
# total_rows=0
# i = 0

# try:  
#     # Get filter for 'stg_subjects' from filters_pdf
#     row_subjects = filters_pdf.loc["i_subjects"]
#     filter= row_subjects["filter"]

#     for studyKey in values:
#         i = i+1
#         endpoint = f"studies/{studyKey}/subjects"
#         table="subjects"
#         if(i==1):
#             mode = "overwrite"
#         else:
#             mode = "append"
#         # print(i)
#         # print(mode)
#         row_count = get_data_imednet(endpoint, filter, "i_subjects", schemasubjects, mode, table)  # now returns int (or 0 if no data)
#         if row_count > 0:
#                 total_rows += row_count
#                 any_data_loaded = True

#     print(f"Total rows loaded: {total_rows}")
#     

#     # Log only once at the end
#     if any_data_loaded:
#         fabric_log.log_api_execution(
#             spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
#             error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
#             custom_description="API Execution"
#         )
#     else:
#         fabric_log.log_api_execution(
#             spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
#             error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
#             custom_description="No data available"
#         )
#     # update config table with max_date
#     # update_config_date("i_subjects", max_date)
# except Exception as e:
#     # Default fallback
#     status_code = 500
#     error_message = str(e)

#     # If the exception is from requests and has a response with status_code
#     if hasattr(e, 'response') and e.response is not None:
#         status_code = e.response.status_code
#         error_message = f"{status_code} {e.response.reason}: {e.response.text}"

#     print(f"Error during data retrieval : {error_message}")
#     fabric_log.log_api_execution(
#         spark, table="subjects", log_type="Error", etl_status="Fail", layer="Bronze",
#         data_source="Mednet", error=error_message, status_code=status_code,row_count=total_rows,
#         start_time=start_time,custom_description=f"Exception occurred: {error_message}"
#     )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Queries**

# CELL ********************

# Define schema for queries table
schemaQueries = StructType([
    StructField("studyKey", StringType(), True),
    StructField("subjectId", StringType(), True),
    StructField("subjectOid", StringType(), True),
    StructField("annotationType", StringType(), True),
    StructField("annotationId", StringType(), True),
    StructField("type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("recordId", StringType(), True),
    StructField("variable", StringType(), True),
    StructField("subjectKey", StringType(), True),
    StructField("dateCreated", StringType(), True),
    StructField("dateModified", StringType(), True),
    StructField("queryComments", StringType(), True)
])

# Initialize the counter
i = 0

try:  
    # Get filter for 'stg_visits' from filters_pdf
    row_subjects = filters_pdf.loc["i_queries"]
    filter= row_subjects["filter"]
    # Loop through studyKeys
    for studyKey in values:
        i = i+1
        endpoint = f"studies/{studyKey}/queries"
        table="queries"
        if(i==1):
            mode = "overwrite"
        else:
            mode = "append"
        # print(i)
        # print(mode)
        get_data_imednet(endpoint, filter, "i_queries", schemaQueries, mode,table)
    # update config table with max_date
    update_config_date("i_queries", max_date)
except Exception as e:
    print(f" Error running get_data_imednet for 'i_queries': {str(e)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Define schema for queries table
# schemaQueries = StructType([
#     StructField("studyKey", StringType(), True),
#     StructField("subjectId", StringType(), True),
#     StructField("subjectOid", StringType(), True),
#     StructField("annotationType", StringType(), True),
#     StructField("annotationId", StringType(), True),
#     StructField("type", StringType(), True),
#     StructField("description", StringType(), True),
#     StructField("recordId", StringType(), True),
#     StructField("variable", StringType(), True),
#     StructField("subjectKey", StringType(), True),
#     StructField("dateCreated", StringType(), True),
#     StructField("dateModified", StringType(), True),
#     StructField("queryComments", StringType(), True)
# ])

# any_data_loaded = False
# start_time = datetime.now()
# total_rows=0
# i = 0

# try:  
#     # Get filter for 'stg_visits' from filters_pdf
#     row_subjects = filters_pdf.loc["i_queries"]
#     filter= row_subjects["filter"]
#     # Loop through studyKeys
#     for studyKey in values:
#         i = i+1
#         endpoint = f"studies/{studyKey}/queries"
#         table="queries"
#         if(i==1):
#             mode = "overwrite"
#         else:
#             mode = "append"
#         # print(i)
#         # print(mode)
#         row_count = get_data_imednet(endpoint, filter, "i_queries", schemaQueries, mode,table)  # now returns int (or 0 if no data)
#         if row_count > 0:
#                 total_rows += row_count
#                 any_data_loaded = True

#     print(f"Total rows loaded: {total_rows}")

#     # Log only once at the end
#     if any_data_loaded:
#         fabric_log.log_api_execution(
#             spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
#             error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
#             custom_description="API Execution"
#         )
#     else:
#         fabric_log.log_api_execution(
#             spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
#             error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
#             custom_description="No data available"
#         )
    # update config table with max_date
    # update_config_date("i_queries", max_date)
# except Exception as e:
#      # Default fallback
#     status_code = 500
#     error_message = str(e)

#     # If the exception is from requests and has a response with status_code
#     if hasattr(e, 'response') and e.response is not None:
#         status_code = e.response.status_code
#         error_message = f"{status_code} {e.response.reason}: {e.response.text}"

#     print(f"Error during data retrieval : {error_message}")
#     fabric_log.log_api_execution(
#         spark, table="queries", log_type="Error", etl_status="Fail", layer="Bronze",
#         data_source="Mednet", error=error_message, status_code=status_code,row_count=total_rows,
#         start_time=start_time,custom_description=f"Exception occurred: {error_message}"
#     )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Records**

# CELL ********************

schemarecords = StructType([            
                    StructField("siteId", StringType(), True),
                    StructField("studyKey", StringType(), True),
                    StructField("subjectId", StringType(), True),
                    StructField("formId", StringType(), True),
                    StructField("formKey", StringType(), True),
                    StructField("recordId", StringType(), True),
                    StructField("recordType", StringType(), True),
                    StructField("recordStatus", StringType(), True),
                    StructField("deleted", StringType(), True),
                    StructField("dateModified", StringType(), True),
                    StructField("dateCreated", StringType(), True),
                    StructField("intervalId", StringType(), True),
                    StructField("subjectKey", StringType(), True),
                    StructField("visitId", StringType(), True),
                    StructField("parentRecordId", StringType(), True)
                    ])
                    
# Initialize the counter
i=0

try:
    # Get filter for 'records' using the index
    row = filters_pdf.loc["i_records"]
    base_filter = row["filter"]
    
    # Add extra condition to exclude 'CATALOG' and 'New Record'
    exclusion_filter = "(recordType != 'CATALOG') and (recordStatus != 'New Record')"
    
    # Combine both filters with "and"
    combined_filter = f"({base_filter}) and {exclusion_filter}"
    
    for studyKey in values:
        i = i+1
        # print(studyKey)
        endpoint = f"studies/{studyKey}/records"
        table="records"
        if(i==1):
            mode = "overwrite"
        else:
            mode = "append"
        # print(i)
        # print(mode)
        # print(f" Encoded Filter: {filter}")
        get_data_imednet(endpoint, combined_filter, "i_records", schemarecords, mode,table)
    # update config table with max_date
    update_config_date("i_records", max_date)

except Exception as e:
    print(f" Error during data retrieval for 'records': {e}")
                   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# schemarecords = StructType([            
#                     StructField("siteId", StringType(), True),
#                     StructField("studyKey", StringType(), True),
#                     StructField("subjectId", StringType(), True),
#                     StructField("formId", StringType(), True),
#                     StructField("formKey", StringType(), True),
#                     StructField("recordId", StringType(), True),
#                     StructField("recordType", StringType(), True),
#                     StructField("recordStatus", StringType(), True),
#                     StructField("deleted", StringType(), True),
#                     StructField("dateModified", StringType(), True),
#                     StructField("dateCreated", StringType(), True),
#                     StructField("intervalId", StringType(), True),
#                     StructField("subjectKey", StringType(), True),
#                     StructField("visitId", StringType(), True),
#                     StructField("parentRecordId", StringType(), True)
#                     ])
                    
# any_data_loaded = False
# start_time = datetime.now()
# total_rows=0
# i = 0

# try:
#     # Get filter for 'records' using the index
#     row = filters_pdf.loc["i_records"]
#     base_filter = row["filter"]
    
#     # Add extra condition to exclude 'CATALOG' and 'New Record'
#     exclusion_filter = "(recordType != 'CATALOG') and (recordStatus != 'New Record')"
    
#     # Combine both filters with "and"
#     combined_filter = f"({base_filter}) and {exclusion_filter}"
    
#     for studyKey in values:
#         i = i+1
#         # print(studyKey)
#         endpoint = f"studies/{studyKey}/records"
#         table="records"
#         if(i==1):
#             mode = "overwrite"
#         else:
#             mode = "append"
#         # print(i)
#         # print(mode)
#         # print(f" Encoded Filter: {filter}")
#         row_count = get_data_imednet(endpoint, combined_filter, "i_records", schemarecords, mode,table)  # now returns int (or 0 if no data)
#         if row_count > 0:
#                 total_rows += row_count
#                 any_data_loaded = True

#     print(f"Total rows loaded: {total_rows}")


#     # Log only once at the end
#     if any_data_loaded:
#         fabric_log.log_api_execution(
#             spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
#             error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
#             custom_description="API Execution"
#         )
#     else:
#         fabric_log.log_api_execution(
#             spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
#             error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
#             custom_description="No data available"
#         )
    # update config table with max_date
  #   update_config_date("i_records", max_date)

# except Exception as e:
#     # Default fallback
#     status_code = 500
#     error_message = str(e)

#     # If the exception is from requests and has a response with status_code
#     if hasattr(e, 'response') and e.response is not None:
#         status_code = e.response.status_code
#         error_message = f"{status_code} {e.response.reason}: {e.response.text}"

#     print(f"Error during data retrieval : {error_message}")
#     fabric_log.log_api_execution(
#         spark, table="records", log_type="Error", etl_status="Fail", layer="Bronze",
#         data_source="Mednet", error=error_message, status_code=status_code,row_count=total_rows,
#         start_time=start_time,custom_description=f"Exception occurred: {error_message}"
#     )
                   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Forms**

# CELL ********************

# Define schema for subjects table
schemaforms = StructType([
    StructField("studyKey", StringType(), True),
    StructField("formId", StringType(), True),
    StructField("formKey", StringType(), True),
    StructField("formName", StringType(), True),
    StructField("formType", StringType(), True),
    StructField("revision", StringType(), True),
    StructField("embeddedLog", StringType(), True),
    StructField("enforceOwnership", StringType(), True),
    StructField("userAgreement", StringType(), True),
    StructField("subjectRecordReport", StringType(), True),
    StructField("unscheduledVisit", StringType(), True),
    StructField("otherForms", StringType(), True),
    StructField("eproForm", StringType(), True),
    StructField("allowCopy", StringType(), True),
    StructField("disabled", StringType(), True),
    StructField("dateCreated", StringType(), True),
    StructField("dateModified", StringType(), True)
])

i=0
try:  
    # Get filter for 'stg_visits' from filters_pdf
    row_subjects = filters_pdf.loc["i_forms"]
    filter= row_subjects["filter"]
    for studyKey in values:
        i = i+1
        endpoint = f"studies/{studyKey}/forms"
        table="forms"
        if(i==1):
            mode = "overwrite"
        else:
            mode = "append"
        # print(i)
        # print(mode)
        get_data_imednet(endpoint, filter, "i_forms", schemaforms, mode, table)
    # update config table with max_date
    update_config_date("i_forms", max_date)
except Exception as e:
    print(f" Error running get_data_imednet for 'i_forms': {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Define schema for subjects table
# schemaforms = StructType([
#     StructField("studyKey", StringType(), True),
#     StructField("formId", StringType(), True),
#     StructField("formKey", StringType(), True),
#     StructField("formName", StringType(), True),
#     StructField("formType", StringType(), True),
#     StructField("revision", StringType(), True),
#     StructField("embeddedLog", StringType(), True),
#     StructField("enforceOwnership", StringType(), True),
#     StructField("userAgreement", StringType(), True),
#     StructField("subjectRecordReport", StringType(), True),
#     StructField("unscheduledVisit", StringType(), True),
#     StructField("otherForms", StringType(), True),
#     StructField("eproForm", StringType(), True),
#     StructField("allowCopy", StringType(), True),
#     StructField("disabled", StringType(), True),
#     StructField("dateCreated", StringType(), True),
#     StructField("dateModified", StringType(), True)
# ])

# any_data_loaded = False
# start_time = datetime.now()
# total_rows=0
# i = 0

# try:  
#     # Get filter for 'stg_visits' from filters_pdf
#     row_subjects = filters_pdf.loc["i_forms"]
#     filter= row_subjects["filter"]
#     for studyKey in values:
#         i = i+1
#         endpoint = f"studies/{studyKey}/forms"
#         table="forms"
#         if(i==1):
#             mode = "overwrite"
#         else:
#             mode = "append"
#         # print(i)
#         # print(mode)
#         row_count = get_data_imednet(endpoint, filter, "i_forms", schemaforms, mode, table)  # now returns int (or 0 if no data)
#         if row_count > 0:
#                 total_rows += row_count
#                 any_data_loaded = True

#     print(f"Total rows loaded: {total_rows}")


#     # Log only once at the end
#     if any_data_loaded:
#         fabric_log.log_api_execution(
#             spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
#             error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
#             custom_description="API Execution"
#         )
#     else:
#         fabric_log.log_api_execution(
#             spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
#             error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
#             custom_description="No data available"
 #       )
    # update config table with max_date
    #update_config_date("i_forms", max_date)

# except Exception as e:
#     # Default fallback
#     status_code = 500
#     error_message = str(e)

#     # If the exception is from requests and has a response with status_code
#     if hasattr(e, 'response') and e.response is not None:
#         status_code = e.response.status_code
#         error_message = f"{status_code} {e.response.reason}: {e.response.text}"

#     print(f"Error during data retrieval: {error_message}")
#     fabric_log.log_api_execution(
#         spark, table="forms", log_type="Error", etl_status="Fail", layer="Bronze",
#         data_source="Mednet", error=error_message, status_code=status_code,row_count=total_rows,
#         start_time=start_time,custom_description=f"Exception occurred: {error_message}"
#     )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Visits**

# CELL ********************

# Define schema for subjects table
schemavisits = StructType([
    StructField("visitId", StringType(), True),
    StructField("studyKey", StringType(), True),
    StructField("intervalId", StringType(), True),
    StructField("intervalName", StringType(), True),
    StructField("subjectId", StringType(), True),
    StructField("subjectKey", StringType(), True),
    StructField("startDate", StringType(), True),
    StructField("endDate", StringType(), True),
    StructField("dueDate", StringType(), True),
    StructField("visitDate", StringType(), True),
    StructField("visitDateForm", StringType(), True),
    StructField("deleted", StringType(), True),
    StructField("visitDateQuestion", StringType(), True),
    StructField("dateCreated", StringType(), True),
    StructField("dateModified", StringType(), True)
])

i=0
try:  
    # Get filter for 'stg_visits' from filters_pdf
    row_subjects = filters_pdf.loc["i_visits"]
    filter= row_subjects["filter"]
    for studyKey in values:
        i = i+1
        endpoint = f"studies/{studyKey}/visits"
        table="visits"
        if(i==1):
            mode = "overwrite"
        else:
            mode = "append"
        # print(i)
        # print(mode)
        get_data_imednet(endpoint, filter, "i_visits", schemavisits, mode, table)
    # update config table with max_date
    update_config_date("i_visits", max_date)
    

except Exception as e:
    print(f" Error running get_data_imednet for 'i_visits': {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Define schema for subjects table
# schemavisits = StructType([
#     StructField("visitId", StringType(), True),
#     StructField("studyKey", StringType(), True),
#     StructField("intervalId", StringType(), True),
#     StructField("intervalName", StringType(), True),
#     StructField("subjectId", StringType(), True),
#     StructField("subjectKey", StringType(), True),
#     StructField("startDate", StringType(), True),
#     StructField("endDate", StringType(), True),
#     StructField("dueDate", StringType(), True),
#     StructField("visitDate", StringType(), True),
#     StructField("visitDateForm", StringType(), True),
#     StructField("deleted", StringType(), True),
#     StructField("visitDateQuestion", StringType(), True),
#     StructField("dateCreated", StringType(), True),
#     StructField("dateModified", StringType(), True)
# ])

# any_data_loaded = False
# start_time = datetime.now()
# total_rows=0
# i = 0

# try:  
#     # Get filter for 'stg_visits' from filters_pdf
#     row_subjects = filters_pdf.loc["i_visits"]
#     filter= row_subjects["filter"]
#     for studyKey in values:
#         i = i+1
#         endpoint = f"studies/{studyKey}/visits"
#         table="visits"
#         if(i==1):
#             mode = "overwrite"
#         else:
#             mode = "append"
#         # print(i)
#         # print(mode)
#         row_count = get_data_imednet(endpoint, filter, "i_visits", schemavisits, mode, table)  # now returns int (or 0 if no data)
#         if row_count > 0:
#                 total_rows += row_count
#                 any_data_loaded = True

#     print(f"Total rows loaded: {total_rows}")
# 

#     # Log only once at the end
#     if any_data_loaded:
#         fabric_log.log_api_execution(
#             spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
#             error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
#             custom_description="API Execution"
#         )
#     else:
#         fabric_log.log_api_execution(
#             spark, table, log_type="ETL", etl_status="Success", layer="Bronze", data_source="Mednet",
#             error=None, status_code=200,row_count=total_rows, start_time=start_time, end_time=datetime.now(),
#             custom_description="No data available"
#         )
    # update config table with max_date
    # update_config_date("i_visits", max_date)
# except Exception as e:
#     # Default fallback
#     status_code = 500
#     error_message = str(e)

#     # If the exception is from requests and has a response with status_code
#     if hasattr(e, 'response') and e.response is not None:
#         status_code = e.response.status_code
#         error_message = f"{status_code} {e.response.reason}: {e.response.text}"

#     print(f"Error during data retrieval: {error_message}")
#     fabric_log.log_api_execution(
#         spark, table="visits", log_type="Error", etl_status="Fail", layer="Bronze",
#         data_source="Mednet", error=error_message, status_code=status_code,row_count=total_rows,
#         start_time=start_time,custom_description=f"Exception occurred: {error_message}"
#     )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
