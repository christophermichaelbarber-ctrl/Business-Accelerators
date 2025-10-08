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

df = spark.read.format("csv").option("header","true").load("abfss://GitIntegrationFabricTrial@onelake.dfs.fabric.microsoft.com/profitandlossadvanced_lakehouse.Lakehouse/Files/Journal Entry.csv")
# df now is a Spark DataFrame containing CSV data from "abfss://GitIntegrationFabricTrial@onelake.dfs.fabric.microsoft.com/profitandlossadvanced_lakehouse.Lakehouse/Files/Journal Entry.csv".
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
