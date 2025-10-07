# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3c875536-2d75-46e8-9e89-41498b7103d9",
# META       "default_lakehouse_name": "profitandlossbasic_lakehouse",
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

import pandas as pd

accounts_df = pd.read_csv("https://aka.ms/wrangler/titanic.csv")
display(accounts_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "editable": true
# META }

# CELL ********************

accounts_df.to_parquet("abfss://GitIntegrationFabricTrial@onelake.dfs.fabric.microsoft.com/profitandlossbasic_lakehouse.Lakehouse/Files/accounts.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

accounts_df_table = spark.read.parquet("abfss://GitIntegrationFabricTrial@onelake.dfs.fabric.microsoft.com/profitandlossbasic_lakehouse.Lakehouse/Files/accounts.parquet")
accounts_df_table.write.mode("overwrite").format("delta").saveAsTable("Accounts_table")

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
