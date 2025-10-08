# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3c875536-2d75-46e8-9e89-41498b7103d9",
# META       "default_lakehouse_name": "profitandlossadvanced_lakehouse",
# META       "default_lakehouse_workspace_id": "f4f0fd69-066c-47fa-9b3e-b287bdf1dde2",
# META       "known_lakehouses": [
# META         {
# META           "id": "3c875536-2d75-46e8-9e89-41498b7103d9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import base64
import pandas as pd
from io import StringIO

Fiscal_Date_base64_string = """
RmlzY2FsRGF0ZSxGaXNjYWxZZWFyLEZpc2NhbFF1YXJ0ZXIsRmlzY2FsUGVyaW9kLEZpc2NhbFllYXJRdWFydGVyLEZpc2NhbFllYXJQZXJpb2QsRmlzY2FsWWVhclNvcnQsRmlzY2FsUXVhcnRlclNvcnQsRmlzY2FsUGVyaW9kU29ydCxGaXNjYWxZZWFyUXVhcnRlclNvcnQsRmlzY2FsWWVhclBlcmlvZFNvcnQsRmlzY2FsUGVyaW9kQ1AsRmlzY2FsUGVyaW9kQ1BTb3J0LEZ1dHVyZURhdGUsRmlzY2FsWWVhclBlcmlvZE51bWJlcixGaXNjYWxQZXJpb2ROdW1iZXIKMDEvMzEvMjAyNCwyMDI0LFExLFAxLDIwMjRRMSwyMDI0UDEsMSwxLDEyLDQsMTIsQ1kgIFAxLDgsRmFsc2UsMjAyNDAxLDE=
"""

# Step 1: Decode the Base64 string
Fiscal_Date_decoded_bytes = base64.b64decode(Fiscal_Date_base64_string)
Fiscal_Date_decoded_str = Fiscal_Date_decoded_bytes.decode("utf-8")

# Step 2: Convert to a pandas DataFrame
Fiscal_Date_csv_data = StringIO(Fiscal_Date_decoded_str)
Fiscal_Date_df = pd.read_csv(Fiscal_Date_csv_data)

# Step 3: CovertDatestoDate
Fiscal_Date_df['FiscalDate'] = pd.to_datetime(Fiscal_Date_df['FiscalDate'], format='%m/%d/%Y', errors='coerce')

# Step 4: Save to Parquet file
Fiscal_Date_df.to_parquet("abfss://GitIntegrationFabricTrial@onelake.dfs.fabric.microsoft.com/profitandlossadvanced_lakehouse.Lakehouse/Files/Date.parquet")

# Step 5: Save to Parquet table
Fiscal_Date_df_table = spark.read.parquet("abfss://GitIntegrationFabricTrial@onelake.dfs.fabric.microsoft.com/profitandlossadvanced_lakehouse.Lakehouse/Files/Date.parquet")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Fiscal_Date_df_table.write.mode("overwrite").format("delta").saveAsTable("Date")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
