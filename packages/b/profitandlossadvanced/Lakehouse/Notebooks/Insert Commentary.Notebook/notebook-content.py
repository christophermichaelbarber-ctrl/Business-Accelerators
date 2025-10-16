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

# Welcome to your new notebook
# Type here in the cell editor to add code!

from pyspark.sql import SparkSession
from datetime import date

spark = SparkSession.builder.getOrCreate()

# Create a small DataFrame for the new row
data = [(date(2024,1,1), "Group", "Test Commentary")]
columns = ["FiscalDate", "Level", "Commentary"]
df = spark.createDataFrame(data, columns)

# Append to the Delta table in the Lakehouse
df.write.format("delta").mode("append").saveAsTable("profitandlossadvanced_lakehouse.commentary")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
