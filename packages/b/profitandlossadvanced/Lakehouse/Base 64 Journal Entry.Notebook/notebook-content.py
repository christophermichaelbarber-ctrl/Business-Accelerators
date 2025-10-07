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
RmlzY2FsRGF0ZSxXb3JraW5nRGF5S2V5LEFjY291bnRLZXksTGVnYWxFbnRpdHlLZXksQnVzaW5lc3NVbml0S2V5LE93bmVyc2hpcEtleSxKb3VybmFsRW50cnlOdW1iZXIsSm91cm5hbEVudHJ5U2VxdWVuY2VJRCxDdXJyZW5jeUtleSxDcmVkaXRMZXNzRGViaXRhdFRyYW5zYWN0aW9uQ3VycmVuY3ksQ3JlZGl0TGVzc0RlYml0YXRHbG9iYWxGeCxDcmVkaXRMZXNzRGViaXRhdFBZRngsQ3JlZGl0TGVzc0RlYml0YXRGY3N0RngsQ3JlZGl0TGVzc0RlYml0YXRQbGFuRngKMjAyMy0wMS0wNCAwMDowMDowMC4wMDAsMTYsNzA0MzAsOSw1LDAsNDg0MDQ5OTkxNDYsMSw3LC0wLjcxNTgsLTAuNzE1OCwtMC43MTU4LC0wLjcxNTgsLTAuNzE1OA==
"""

# Step 1: Decode the Base64 string
Ledger_decoded_bytes = base64.b64decode(Ledger_base64_string)
Ledger_decoded_str = Ledger_decoded_bytes.decode("utf-8")

# Step 2: Convert to a pandas DataFrame
Ledgere_csv_data = StringIO(Ledger_decoded_str)
Ledger_df = pd.read_csv(Ledger_csv_data)

# Step 3: CovertDatestoDate
Ledger_df['FiscalDate'] = pd.to_datetime(Ledger_df['FiscalDate'], format='%m/%d/%Y', errors='coerce')

# Step 4: Save to Parquet file
Ledger_df.to_parquet("abfss://GitIntegrationFabricTrial@onelake.dfs.fabric.microsoft.com/profitandlossadvanced_lakehouse.Lakehouse/Files/Ledger.parquet")

# Step 5: Save to Parquet table
Ledger_df_table = spark.read.parquet("abfss://GitIntegrationFabricTrial@onelake.dfs.fabric.microsoft.com/profitandlossadvanced_lakehouse.Lakehouse/Files/Ledger.parquet")


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
