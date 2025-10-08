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

#Creating the date table

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import *

start = datetime(2021, 1, 1)
end = datetime(2024, 1, 31)   ##datetime.today() + relativedelta(months=6)
date_list = [(start + timedelta(days=i),) for i in range((end - start).days + 1)]
df = spark.createDataFrame(date_list, ["Date"])
df = df.withColumn("Year", year(col("Date"))) \
       .withColumn("Month", month(col("Date"))) \
       .withColumn("Quarter", quarter(col("Date"))) \
       .withColumn("MonthName", date_format(col("Date"), "MMMM")) \
       .withColumn("DayName", date_format(col("Date"), "EEEE"))
df.write.mode("overwrite").saveAsTable("Date")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create the account table from Base64 string (use same method for layout ect)

import base64
import pandas as pd
from io import StringIO

Acccounts_base64_string = """
QWNjb3VudF9LZXksQWNjb3VudF9OdW1iZXJfYW5kX05hbWUsQWNjb3VudF9UeXBlLEFjY291bnRfVHlwZV9JbmRpY2F0b3IsSW5jb21lX1N0YXRlbWVudF9LZXkKMTAxMDAsMTAxMDAgLSBSZXZlbnVlLFJldmVudWUsMSwyCjEwMTEwLDEwMTEwIC0gUmV2ZW51ZSxSZXZlbnVlLDEsMgoxMDEyMCwxMDEyMCAtIFJldmVudWUsUmV2ZW51ZSwxLDIKMTAxMzAsMTAxMzAgLSBSZXZlbnVlLFJldmVudWUsMSwyCjEwMjAwLDEwMjAwIC0gUmV2ZW51ZSxSZXZlbnVlLDEsMwoxMDIxMCwxMDIxMCAtIFJldmVudWUsUmV2ZW51ZSwxLDMKMTAyMjAsMTAyMjAgLSBSZXZlbnVlLFJldmVudWUsMSwzCjEwMjMwLDEwMjMwIC0gUmV2ZW51ZSxSZXZlbnVlLDEsMwoyMDEwMCwyMDEwMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDcKMjAxMTAsMjAxMTAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSw3CjIwMTIwLDIwMTIwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsNwoyMDEzMCwyMDEzMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDcKMjAxNDAsMjAxNDAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSw3CjIwMjAwLDIwMjAwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsOAoyMDIxMCwyMDIxMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDgKMjAyMjAsMjAyMjAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSw4CjIwMjMwLDIwMjMwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsOAoyMDI0MCwyMDI0MCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDgKMzAxMDAsMzAxMDAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxMwozMDExMCwzMDExMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDEzCjMwMTIwLDMwMTIwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTMKMzAxMzAsMzAxMzAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxMwozMDE0MCwzMDE0MCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDEzCjMwMTUwLDMwMTUwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTMKMzAxNjAsMzAxNjAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxMwozMDE3MCwzMDE3MCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDEzCjMwMTgwLDMwMTgwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTMKNDAxMDAsNDAxMDAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxMwo0MDExMCw0MDExMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDEzCjQwMTIwLDQwMTIwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTMKNDAxMzAsNDAxMzAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxMwo1MDEwMCw1MDEwMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE0CjUwMTEwLDUwMTEwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTQKNTAxMjAsNTAxMjAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxNAo1MDEzMCw1MDEzMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE0CjUwMTQwLDUwMTQwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTQKNTAxNTAsNTAxNTAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxNAo1MDE2MCw1MDE2MCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE0CjYwMTAwLDYwMTAwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTQKNjAxMTAsNjAxMTAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxNAo2MDEyMCw2MDEyMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE0CjcwMTAwLDcwMTAwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTUKNzAxMTAsNzAxMTAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxNQo3MDEyMCw3MDEyMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE1CjcwMTMwLDcwMTMwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTUKNzAxNDAsNzAxNDAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxNQo3MDQwMCw3MDQwMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE1CjcwNDEwLDcwNDEwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTUKNzA0MjAsNzA0MjAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxNQo3MDQzMCw3MDQzMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE1CjEwNzEwLDEwNzEwIC0gT3RoZXIgaW5jb21lLFJldmVudWUsMSwyMAoxMDcyMCwxMDcxMCAtIE90aGVyIGluY29tZSxSZXZlbnVlLDEsMjAKODAxMDAsODAxMDAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwyMwo4MDExMCw4MDExMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDIzCjgwMTIwLDgwMTIwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMjMKODAxMzAsODAxMzAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwyMwo=
"""

# Step 1: Decode the Base64 string
Acccounts_decoded_bytes = base64.b64decode(Acccounts_base64_string)
Acccounts_decoded_str = Acccounts_decoded_bytes.decode("utf-8")

# Step 2: Convert to a pandas DataFrame
Acccounts_csv_data = StringIO(Acccounts_decoded_str)
Acccounts_df = pd.read_csv(Acccounts_csv_data)

# Step 4: Save to Parquet file
Acccounts_df.to_parquet("abfss://GitIntegrationFabricTrial@onelake.dfs.fabric.microsoft.com/profitandlossadvanced_lakehouse.Lakehouse/Files/Accounts.parquet")

# Step 5: Save to Parquet table
Acccounts_df_table = spark.read.parquet("abfss://GitIntegrationFabricTrial@onelake.dfs.fabric.microsoft.com/profitandlossadvanced_lakehouse.Lakehouse/Files/Accounts.parquet")

# Step 6: Create table
Acccounts_df_table.write.mode("overwrite").format("delta").saveAsTable("Accounts")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
