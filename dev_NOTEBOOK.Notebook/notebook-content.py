# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0c752a71-b6d8-4213-af4f-b83a00b16272",
# META       "default_lakehouse_name": "test",
# META       "default_lakehouse_workspace_id": "b10e8e74-6aee-4b16-9743-9e97aa17d2b5",
# META       "known_lakehouses": [
# META         {
# META           "id": "0c752a71-b6d8-4213-af4f-b83a00b16272"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd

# Full abfss path
file_path = "abfss://development_test@onelake.dfs.fabric.microsoft.com/test.Lakehouse/Files/Screenfail and USV subject visit.xlsx"

# Read Excel
pdf = pd.read_excel(file_path, engine="openpyxl")

# ✅ Convert all columns to string
pdf = pdf.astype(str)

# Convert pandas → Spark DataFrame (all as string)
df = spark.createDataFrame(pdf)

# Show first 5 rows
df.show(5, truncate=False)

# Save as Delta table
df.write.format("delta").mode("overwrite").saveAsTable("Screenfail_USV_SubjectVisitnew")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
