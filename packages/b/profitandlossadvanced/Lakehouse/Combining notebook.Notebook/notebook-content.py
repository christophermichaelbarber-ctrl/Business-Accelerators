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

##VERIFIED Layout Table
import base64
import pandas as pd
from io import StringIO
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ==== CONFIG ====
BASE64_STR = """
SW5jb21lX1N0YXRlbWVudF9LZXksTGluZV9uYW1lLFN1YnRvdGFsX0Zyb20sU3VidG90YWxfVG8sRGl2aWRlX051bWVyYXRvcixEaXZpZGVfRGVub21pbmF0b3IsU2hvd19QZXJjLEZvcm1hdF9TdHJpbmdfRGVmYXVsdCxGb3JtYXRfU3RyaW5nX1ZhcmlhbmNlX0FicyxGb3JtYXRfU3RyaW5nX1ZhcmlhbmNlX1BlcixGb3JtYXR0aW5nX1BfTF9IZXhfVGV4dCxGb3JtYXR0aW5nIFBfTF9IZXhfQmFja2dyb3VuZCxTaG93X1ZhbHVlcyxDYWxjdWxhdGlvbl90eXBlCjEsUmV2ZW51ZTosLCwsLDEsMDswLTA7O0AsMDswLTA7O0AsMDswLTA7O0AsI0ZGRkZGRiwjRkZGRkZGLDEsQmxhbmsKNSzCoCwsLCwsMSwwOzAtMDs7QCwwOzAtMDs7QCwwOzAtMDs7QCwjRkZGRkZGLCNGRkZGRkYsMSxCbGFuawo2LENvc3Qgb2YgcmV2ZW51ZTrCoMKgLCwsLCwxLDA7MC0wOztALDA7MC0wOztALDA7MC0wOztALCNGRkZGRkYsI0ZGRkZGRiwxLEJsYW5rCjIswqDCoMKgUHJvZHVjdCwsLCwsMSwiIywjIzAsOygjLCMjMCwpOy0iLCIrIywjIzAsOy0jLCMjMCw7LSIsIisjLCMjMC4wMCU7LSMsIyMwLjAwJTstIiwjOGI4NjgwLCNGRkZGRkYsMSxMaW5lLWl0ZW0gcmV2ZW51ZQozLMKgwqDCoFNlcnZpY2UgYW5kIG90aGVyLCwsLCwxLCIjLCMjMCw7KCMsIyMwLCk7LSIsIisjLCMjMCw7LSMsIyMwLDstIiwiKyMsIyMwLjAwJTstIywjIzAuMDAlOy0iLCM4Yjg2ODAsI0ZGRkZGRiwxLExpbmUtaXRlbSByZXZlbnVlCjQswqDCoMKgwqDCoFRvdGFsIHJldmVudWUsMiwxLCwsMSwiIywjIzAsOygjLCMjMCwpOy0iLCIrIywjIzAsOy0jLCMjMCw7LSIsIisjLCMjMC4wMCU7LSMsIyMwLjAwJTstIiwjMDAwMDAwLCNGM0YzRjMsMSxTdWJ0b3RhbCBOQ0xECjcswqDCoMKgUHJvZHVjdMKgLCwsLCwxLCIjLCMjMCw7KCMsIyMwLCk7LSIsIisjLCMjMCw7LSMsIyMwLDstIiwiKyMsIyMwLjAwJTstIywjIzAuMDAlOy0iLCM4Yjg2ODAsI0ZGRkZGRiwxLExpbmUtaXRlbSBleHBlbnNlCjgswqDCoMKgU2VydmljZSBhbmQgb3RoZXLCoCwsLCwsMSwiIywjIzAsOygjLCMjMCwpOy0iLCIrIywjIzAsOy0jLCMjMCw7LSIsIisjLCMjMC4wMCU7LSMsIyMwLjAwJTstIiwjOGI4NjgwLCNGRkZGRkYsMSxMaW5lLWl0ZW0gZXhwZW5zZQo5LMKgwqDCoMKgwqBUb3RhbCBjb3N0IG9mIHJldmVudWXCoCw3LDgsLCwxLCIjLCMjMCw7KCMsIyMwLCk7LSIsIisjLCMjMCw7LSMsIyMwLDstIiwiKyMsIyMwLjAwJTstIywjIzAuMDAlOy0iLCMwMDAwMDAsI0YzRjNGMywxLFN1YnRvdGFsIE5ETEMKMTAswqDCoMKgwqDCoEdyb3NzIG1hcmdpbsKgLDIsOCwsLDEsIiMsIyMwLDsoIywjIzAsKTstIiwiKyMsIyMwLDstIywjIzAsOy0iLCIrIywjIzAuMDAlOy0jLCMjMC4wMCU7LSIsIzAwMDAwMCwjRjNGM0YzLDEsU3VidG90YWwgTkNMRAoxMSwiwqDCoMKgwqDCoEdyb3NzIG1hcmdpbiwgJSIsLCwxMCw0LCwiIywjIzAuMCU7KCMsIyMwLjAlKTstIiwiKyMsIyMwLjAwJTstIywjIzAuMDAlOy0iLDA7MC0wOztALCMwMDAwMDAsI0YzRjNGMywxLERpdmlkZSBQZXJjZW50YWdlCjEyLMKgwqAsLCwsLDEsMDswLTA7O0AsMDswLTA7O0AsMDswLTA7O0AsI0ZGRkZGRiwjRkZGRkZGLDEsQmxhbmsKMTMsUmVzZWFyY2ggYW5kIGRldmVsb3BtZW50LCwsLCwxLCIjLCMjMCw7KCMsIyMwLCk7LSIsIisjLCMjMCw7LSMsIyMwLDstIiwiKyMsIyMwLjAwJTstIywjIzAuMDAlOy0iLCM4Yjg2ODAsI0ZGRkZGRiwxLExpbmUtaXRlbSBleHBlbnNlCjE0LFNhbGVzIGFuZCBtYXJrZXRpbmcsLCwsLDEsIiMsIyMwLDsoIywjIzAsKTstIiwiKyMsIyMwLDstIywjIzAsOy0iLCIrIywjIzAuMDAlOy0jLCMjMC4wMCU7LSIsIzhiODY4MCwjRkZGRkZGLDEsTGluZS1pdGVtIGV4cGVuc2UKMTUsR2VuZXJhbCBhbmQgYWRtaW5pc3RyYXRpdmUsLCwsLDEsIiMsIyMwLDsoIywjIzAsKTstIiwiKyMsIyMwLDstIywjIzAsOy0iLCIrIywjIzAuMDAlOy0jLCMjMC4wMCU7LSIsIzhiODY4MCwjRkZGRkZGLDEsTGluZS1pdGVtIGV4cGVuc2UKMTYswqDCoMKgLCwsLCwxLDA7MC0wOztALDA7MC0wOztALDA7MC0wOztALCNGRkZGRkYsI0ZGRkZGRiwxLEJsYW5rCjE3LE9wZXJhdGluZyBpbmNvbWUsMiwxNiwsLDEsIiMsIyMwLDsoIywjIzAsKTstIiwiKyMsIyMwLDstIywjIzAsOy0iLCIrIywjIzAuMDAlOy0jLCMjMC4wMCU7LSIsIzAwMDAwMCwjRjNGM0YzLDEsU3VidG90YWwgTkNMRAoxOCwiT3BlcmF0aW5nIGluY29tZSwgJSIsLCwxNyw0LCwiIywjIzAuMCU7KCMsIyMwLjAlKTstIiwiKyMsIyMwLjAwJTstIywjIzAuMDAlOy0iLDA7MC0wOztALCMwMDAwMDAsI0YzRjNGMywxLERpdmlkZSBQZXJjZW50YWdlCjE5LMKgwqDCoMKgLCwsLCwxLDA7MC0wOztALDA7MC0wOztALDA7MC0wOztALCNGRkZGRkYsI0ZGRkZGRiwxLEJsYW5rCjIwLCJPdGhlciBpbmNvbWUsIG5ldCIsLCwsLDEsIiMsIyMwLDsoIywjIzAsKTstIiwiKyMsIyMwLDstIywjIzAsOy0iLCIrIywjIzAuMDAlOy0jLCMjMC4wMCU7LSIsIzhiODY4MCwjRkZGRkZGLDEsTGluZS1pdGVtIHJldmVudWUKMjEsSW5jb21lIGJlZm9yZSBpbmNvbWUgdGF4ZXMsMiwyMCwsLDEsIiMsIyMwLDsoIywjIzAsKTstIiwiKyMsIyMwLDstIywjIzAsOy0iLCIrIywjIzAuMDAlOy0jLCMjMC4wMCU7LSIsIiMwMDAwMDAiIiIsI0YzRjNGMywxLFN1YnRvdGFsIE5DTEQKMjIswqDCoMKgwqDCoMKgLCwsLCwxLDA7MC0wOztALDA7MC0wOztALDA7MC0wOztALCNGRkZGRkYsI0ZGRkZGRiwxLEJsYW5rCjIzLFRheCBQcm92aXNpb24sLCwsLDEsIiMsIyMwLDsoIywjIzAsKTstIiwiKyMsIyMwLDstIywjIzAsOy0iLCIrIywjIzAuMDAlOy0jLCMjMC4wMCU7LSIsIzhiODY4MCwjRkZGRkZGLDEsTGluZS1pdGVtIGV4cGVuc2UKMjQswqDCoMKgwqDCoMKgLCwsLCwxLDA7MC0wOztALDA7MC0wOztALDA7MC0wOztALCNGRkZGRkYsI0ZGRkZGRiwxLEJsYW5rCjI1LE5ldCBpbmNvbWUsMiwyNCwsLDEsIiMsIyMwLDsoIywjIzAsKTstIiwiKyMsIyMwLDstIywjIzAsOy0iLCIrIywjIzAuMDAlOy0jLCMjMC4wMCU7LSIsIzAwMDAwMCwjRjNGM0YzLDEsU3VidG90YWwgTkNMRAoyNiwiTmV0IGluY29tZSwgJSIsLCwyNSwwNCwsIiMsIyMwLjAlOygjLCMjMC4wJSk7LSIsIisjLCMjMC4wMCU7LSMsIyMwLjAwJTstIiwwOzAtMDs7QCwjMDAwMDAwLCNGM0YzRjMsMSxEaXZpZGUgUGVyY2VudGFnZQ==
"""   # <-- put your base64 (no extra comments inside)
TABLE_NAME = "Layout"                   # target Delta table name

# Optional: define a schema to control column types in Spark (recommended)
# Comment this block out if you want automatic inference.
spark_schema = T.StructType([
    T.StructField("Income_Statement_Key", T.IntegerType(), True),
    T.StructField("Line_name", T.StringType(), True),
    T.StructField("Subtotal_From", T.IntegerType(), True),
    T.StructField("Subtotal_To", T.IntegerType(), True),
    T.StructField("Divide_Numerator", T.IntegerType(), True),
    T.StructField("Divide_Denominator", T.IntegerType(), True),
    T.StructField("Show_Perc",T.IntegerType(), True),
    T.StructField("Format_String_Default", T.StringType(), True),
    T.StructField("Format_String_Variance_Abs", T.StringType(), True),
    T.StructField("Format_String_Variance_Per", T.StringType(), True),
    T.StructField("Formatting_P_L_Hex_Text", T.StringType(), True),
    T.StructField("Formatting_P_L_Hex_Background", T.StringType(), True),
    T.StructField("Show_Values", T.IntegerType(), True),
    T.StructField("Calculation_type", T.StringType(), True),
])

# ==== 1) Decode Base64 safely ====
b64_clean = BASE64_STR.strip()
if not b64_clean:
    raise ValueError("Base64 string is empty. Paste a valid Base64 CSV.")

csv_bytes = base64.b64decode(b64_clean)
csv_text = csv_bytes.decode("utf-8")
if not csv_text.strip():
    raise ValueError("Decoded CSV is empty.")

# ==== 2) Read CSV with pandas ====
# If your CSV has a header row, this is enough. Otherwise: pd.read_csv(..., header=None, names=[...])
pdf = pd.read_csv(StringIO(csv_text))

# (Optional) pandas-side cleanup or typing
# e.g., ensure ints where possible:
# for col in ["Income_Statement_Key"]:
#     pdf[col] = pd.to_numeric(pdf[col], errors="coerce").astype("Int64")

# ==== 3) Convert pandas -> Spark ====
if 'spark_schema' in locals() and spark_schema is not None:
    sdf = spark.createDataFrame(pdf, schema=spark_schema)
else:
    sdf = spark.createDataFrame(pdf)  # infer schema

# (Optional) Final type tweaks in Spark (example)
# sdf = sdf.withColumn("Income_Statement_Key", F.col("Income_Statement_Key").cast("int"))

# ==== 4) Write as managed Delta table (in attached Lakehouse) ====
(sdf.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(TABLE_NAME))

print(f"✅ Wrote Delta table: {TABLE_NAME}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##VERIFIED Accounts Table
import base64
import pandas as pd
from io import StringIO
from pyspark.sql import types as T

# ==== CONFIG ====
BASE64_STR = """
QWNjb3VudF9LZXksQWNjb3VudF9OdW1iZXJfYW5kX05hbWUsQWNjb3VudF9UeXBlLEFjY291bnRfVHlwZV9JbmRpY2F0b3IsSW5jb21lX1N0YXRlbWVudF9LZXkKMTAxMDAsMTAxMDAgLSBSZXZlbnVlLFJldmVudWUsMSwyCjEwMTEwLDEwMTEwIC0gUmV2ZW51ZSxSZXZlbnVlLDEsMgoxMDEyMCwxMDEyMCAtIFJldmVudWUsUmV2ZW51ZSwxLDIKMTAxMzAsMTAxMzAgLSBSZXZlbnVlLFJldmVudWUsMSwyCjEwMjAwLDEwMjAwIC0gUmV2ZW51ZSxSZXZlbnVlLDEsMwoxMDIxMCwxMDIxMCAtIFJldmVudWUsUmV2ZW51ZSwxLDMKMTAyMjAsMTAyMjAgLSBSZXZlbnVlLFJldmVudWUsMSwzCjEwMjMwLDEwMjMwIC0gUmV2ZW51ZSxSZXZlbnVlLDEsMwoyMDEwMCwyMDEwMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDcKMjAxMTAsMjAxMTAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSw3CjIwMTIwLDIwMTIwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsNwoyMDEzMCwyMDEzMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDcKMjAxNDAsMjAxNDAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSw3CjIwMjAwLDIwMjAwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsOAoyMDIxMCwyMDIxMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDgKMjAyMjAsMjAyMjAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSw4CjIwMjMwLDIwMjMwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsOAoyMDI0MCwyMDI0MCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDgKMzAxMDAsMzAxMDAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxMwozMDExMCwzMDExMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDEzCjMwMTIwLDMwMTIwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTMKMzAxMzAsMzAxMzAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxMwozMDE0MCwzMDE0MCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDEzCjMwMTUwLDMwMTUwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTMKMzAxNjAsMzAxNjAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxMwozMDE3MCwzMDE3MCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDEzCjMwMTgwLDMwMTgwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTMKNDAxMDAsNDAxMDAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxMwo0MDExMCw0MDExMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDEzCjQwMTIwLDQwMTIwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTMKNDAxMzAsNDAxMzAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxMwo1MDEwMCw1MDEwMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE0CjUwMTEwLDUwMTEwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTQKNTAxMjAsNTAxMjAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxNAo1MDEzMCw1MDEzMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE0CjUwMTQwLDUwMTQwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTQKNTAxNTAsNTAxNTAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxNAo1MDE2MCw1MDE2MCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE0CjYwMTAwLDYwMTAwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTQKNjAxMTAsNjAxMTAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxNAo2MDEyMCw2MDEyMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE0CjcwMTAwLDcwMTAwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTUKNzAxMTAsNzAxMTAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxNQo3MDEyMCw3MDEyMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE1CjcwMTMwLDcwMTMwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTUKNzAxNDAsNzAxNDAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxNQo3MDQwMCw3MDQwMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE1CjcwNDEwLDcwNDEwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMTUKNzA0MjAsNzA0MjAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwxNQo3MDQzMCw3MDQzMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDE1CjEwNzEwLDEwNzEwIC0gT3RoZXIgaW5jb21lLFJldmVudWUsMSwyMAoxMDcyMCwxMDcxMCAtIE90aGVyIGluY29tZSxSZXZlbnVlLDEsMjAKODAxMDAsODAxMDAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwyMwo4MDExMCw4MDExMCAtIEV4cGVuZGl0dXJlLEV4cGVuZGl0dXJlLC0xLDIzCjgwMTIwLDgwMTIwIC0gRXhwZW5kaXR1cmUsRXhwZW5kaXR1cmUsLTEsMjMKODAxMzAsODAxMzAgLSBFeHBlbmRpdHVyZSxFeHBlbmRpdHVyZSwtMSwyMwo=

"""     # your base64 from the snippet
TABLE_NAME = "Accounts"                   # target Delta table

# (Optional) Spark schema – recommended to avoid type drift
spark_schema = T.StructType([
    T.StructField("Account_Key", T.IntegerType(), True),
    T.StructField("Account_Number_and_Name", T.StringType(), True),
    T.StructField("Account_Type", T.StringType(), True),
    T.StructField("Account_Type_Indicator", T.IntegerType(), True),
    T.StructField("Income_Statement_Key", T.IntegerType(), True),
])

# ==== 1) Decode Base64 safely ====
b64 = BASE64_STR.strip()
if not b64:
    raise ValueError("Base64 string is empty. Paste a valid Base64 CSV.")

csv_bytes = base64.b64decode(b64)
csv_text = csv_bytes.decode("utf-8")
if not csv_text.strip():
    raise ValueError("Decoded CSV is empty.")

# ==== 2) Read CSV with pandas ====
pdf = pd.read_csv(StringIO(csv_text))

# (Optional) pandas cleanup / typing
# pdf["Account_Key"] = pd.to_numeric(pdf["Account_Key"], errors="coerce").astype("Int64")
# pdf["Account_Type_Indicator"] = pd.to_numeric(pdf["Account_Type_Indicator"], errors="coerce").astype("Int64")
# pdf["Income_Statement_Key"] = pd.to_numeric(pdf["Income_Statement_Key"], errors="coerce").astype("Int64")

# ==== 3) Convert pandas -> Spark ====
sdf = spark.createDataFrame(pdf, schema=spark_schema)

# ==== 4) Write as managed Delta table ====
(sdf.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(TABLE_NAME))

print(f"✅ Wrote Delta table: {TABLE_NAME}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Base64 Date table -> Delta (Fabric)
import base64
import pandas as pd
from io import StringIO
from pyspark.sql import functions as F, types as T

# ==== CONFIG ====
BASE64_STR = """
RmlzY2FsRGF0ZSxGaXNjYWxZZWFyLEZpc2NhbFF1YXJ0ZXIsRmlzY2FsUGVyaW9kLEZpc2NhbFllYXJRdWFydGVyLEZpc2NhbFllYXJQZXJpb2QsRmlzY2FsWWVhclNvcnQsRmlzY2FsUXVhcnRlclNvcnQsRmlzY2FsUGVyaW9kU29ydCxGaXNjYWxZZWFyUXVhcnRlclNvcnQsRmlzY2FsWWVhclBlcmlvZFNvcnQsRmlzY2FsUGVyaW9kQ1AsRmlzY2FsUGVyaW9kQ1BTb3J0LEZ1dHVyZURhdGUsRmlzY2FsWWVhclBlcmlvZE51bWJlcixGaXNjYWxQZXJpb2ROdW1iZXIKMDEvMzEvMjAyNCwyMDI0LFExLFAxLDIwMjRRMSwyMDI0UDEsMSwxLDEyLDQsMTIsQ1kgIFAxLDgsRmFsc2UsMjAyNDAxLDE=
"""
TABLE_NAME = "Date"   # target Delta table name

# Expected columns in the CSV (order taken from your header)
expected_cols = [
    "FiscalDate", "FiscalYear", "FiscalQuarter", "FiscalPeriod",
    "FiscalYearQuarter", "FiscalYearPeriod",
    "FiscalYearSort", "FiscalQuarterSort", "FiscalPeriodSort",
    "FiscalPeriodCP", "FiscalPeriodCPSort",
    "FutureDate", "FiscalYearPeriodNumber", "FiscalPeriodNumber"
]

# Spark schema we want on the final table
spark_schema = T.StructType([
    T.StructField("FiscalDate", T.DateType(), True),
    T.StructField("FiscalYear", T.IntegerType(), True),
    T.StructField("FiscalQuarter", T.StringType(), True),
    T.StructField("FiscalPeriod", T.StringType(), True),
    T.StructField("FiscalYearQuarter", T.StringType(), True),
    T.StructField("FiscalYearPeriod", T.StringType(), True),
    T.StructField("FiscalYearSort", T.IntegerType(), True),
    T.StructField("FiscalQuarterSort", T.IntegerType(), True),
    T.StructField("FiscalPeriodSort", T.IntegerType(), True),
    T.StructField("FiscalPeriodCP", T.StringType(), True),
    T.StructField("FiscalPeriodCPSort", T.IntegerType(), True),
    T.StructField("FutureDate", T.BooleanType(), True),
    T.StructField("FiscalYearPeriodNumber", T.IntegerType(), True),
    T.StructField("FiscalPeriodNumber", T.IntegerType(), True),
])

# ==== 1) Decode Base64 safely ====
b64 = BASE64_STR.strip()
if not b64:
    raise ValueError("Base64 string is empty. Paste a valid Base64 CSV.")
csv_text = base64.b64decode(b64).decode("utf-8")
if not csv_text.strip():
    raise ValueError("Decoded CSV is empty.")

# ==== 2) Read CSV with pandas ====
pdf = pd.read_csv(StringIO(csv_text))
# Ensure expected columns (useful if pandas infers differently)
missing = set(expected_cols) - set(pdf.columns)
if missing:
    raise ValueError(f"CSV missing expected columns: {sorted(missing)}")

# Parse date (mm/dd/yyyy in your sample like 01/31/2024)
pdf["FiscalDate"] = pd.to_datetime(pdf["FiscalDate"], format="%m/%d/%Y", errors="coerce")

# Coerce numerics
for c in ["FiscalYear","FiscalYearSort","FiscalQuarterSort","FiscalPeriodSort",
          "FiscalPeriodCPSort","FiscalYearPeriodNumber","FiscalPeriodNumber"]:
    pdf[c] = pd.to_numeric(pdf[c], errors="coerce")

# Coerce boolean (handles True/False, 'true'/'false', 1/0)
pdf["FutureDate"] = (
    pdf["FutureDate"]
      .astype(str)
      .str.strip()
      .str.lower()
      .map({"true": True, "false": False, "1": True, "0": False})
)

# ==== 3) Convert pandas -> Spark and enforce Spark dtypes ====
sdf = spark.createDataFrame(pdf)

# Cast columns to final schema (idempotent if already correct)
for f in spark_schema:
    if f.name in sdf.columns:
        sdf = sdf.withColumn(f.name, F.col(f.name).cast(f.dataType))

# ==== 4) Write managed Delta table in the attached Lakehouse ====
(sdf.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(TABLE_NAME))

print(f"✅ Wrote Delta table: {TABLE_NAME}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [(1, 'MeasureTable')]
columns = ['ID', 'Col1']

measure_df = spark.createDataFrame(data, columns)
measure_df.show()
spark.sql("DROP TABLE IF EXISTS MeasureTable")
measure_df.write.format("delta").saveAsTable('MeasureTable')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
