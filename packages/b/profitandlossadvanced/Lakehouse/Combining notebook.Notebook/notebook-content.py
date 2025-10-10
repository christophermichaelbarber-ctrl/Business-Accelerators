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

# CELL ********************

# Base64 Legal Entity table -> Delta (Fabric)
import base64
import pandas as pd
from io import StringIO
from pyspark.sql import functions as F, types as T

# ==== CONFIG ====
BASE64_STR = """
TGVnYWxfRW50aXR5X0tleSxMZWdhbF9lbnRpdHlfbmFtZSxMZWdhbF9FbnRpdHlfU29ydF9PcmRlcixHcm91cCxDb3VudHJ5LFJlZ2lvbixDaGlsZCxQYXJlbnQsUGF0aCxMZWdhbF9lbnRpdHlfbGV2ZWxfMSxMZWdhbF9lbnRpdHlfbGV2ZWxfMixMZWdhbF9lbnRpdHlfbGV2ZWxfMyxMZWdhbF9lbnRpdHlfbGV2ZWxfNCxMZWdhbF9lbnRpdHlfbGV2ZWxfNSxMZWdhbF9lbnRpdHlfbGV2ZWxfMV9uYW1lLExlZ2FsX2VudGl0eV9sZXZlbF8yX25hbWUsTGVnYWxfZW50aXR5X2xldmVsXzNfbmFtZSxMZWdhbF9lbnRpdHlfbGV2ZWxfNF9uYW1lLExlZ2FsX2VudGl0eV9sZXZlbF81X25hbWUKMSxDb21wYW55IEhvbGRpbmdzLDEsR3JvdXAsVVNBLEFtZXJpY2FzLDEsLDEsMSwsLCwsQ29tcGFueSBIb2xkaW5ncywsLCwKMixDb21wYW55IExvZ2lzdGljcyBVUywyLEdyb3VwLFVTQSxBbWVyaWNhcywyLDEsMXwyLDEsMiwsLCxDb21wYW55IEhvbGRpbmdzLENvbXBhbnkgTG9naXN0aWNzIFVTLCwsCjMsQ29tcGFueSBSZXRhaWwgSW5jLDMsR3JvdXAsVVNBLEFtZXJpY2FzLDMsMiwxfDJ8MywxLDIsMywsLENvbXBhbnkgSG9sZGluZ3MsQ29tcGFueSBMb2dpc3RpY3MgVVMsQ29tcGFueSBSZXRhaWwgSW5jLCwKNCxDb21wYW55IFJldGFpbCBDYW5hZGEsNCxHcm91cCxDYW5hZGEsQW1lcmljYXMsNCwzLDF8MnwzfDQsMSwyLDMsNCwsQ29tcGFueSBIb2xkaW5ncyxDb21wYW55IExvZ2lzdGljcyBVUyxDb21wYW55IFJldGFpbCBJbmMsQ29tcGFueSBSZXRhaWwgQ2FuYWRhLAo1LENvbXBhbnkgUmV0YWlsIE1leGljbyw1LEdyb3VwLE1leGljbyxBbWVyaWNhcyw1LDMsMXwyfDN8NSwxLDIsMyw1LCxDb21wYW55IEhvbGRpbmdzLENvbXBhbnkgTG9naXN0aWNzIFVTLENvbXBhbnkgUmV0YWlsIEluYyxDb21wYW55IFJldGFpbCBNZXhpY28sCjYsQ29tcGFueSBGaW5hbmNlIEx0ZCw2LEdyb3VwLFVLLEV1cm9wZSw2LDEsMXw2LDEsNiwsLCxDb21wYW55IEhvbGRpbmdzLENvbXBhbnkgRmluYW5jZSBMdGQsLCwKNyxDb21wYW55IEludmVzdG1lbnRzLDcsR3JvdXAsVUssRXVyb3BlLDcsNiwxfDZ8NywxLDYsNywsLENvbXBhbnkgSG9sZGluZ3MsQ29tcGFueSBGaW5hbmNlIEx0ZCxDb21wYW55IEludmVzdG1lbnRzLCwKOCxDb21wYW55IFRlY2ggTHRkLDgsR3JvdXAsVUssRXVyb3BlLDgsNywxfDZ8N3w4LDEsNiw3LDgsLENvbXBhbnkgSG9sZGluZ3MsQ29tcGFueSBGaW5hbmNlIEx0ZCxDb21wYW55IEludmVzdG1lbnRzLENvbXBhbnkgVGVjaCBMdGQsCjksQ29tcGFueSBMb2dpc3RpY3MgTHRkLDksR3JvdXAsVUssRXVyb3BlLDksNywxfDZ8N3w5LDEsNiw3LDksLENvbXBhbnkgSG9sZGluZ3MsQ29tcGFueSBGaW5hbmNlIEx0ZCxDb21wYW55IEludmVzdG1lbnRzLENvbXBhbnkgTG9naXN0aWNzIEx0ZCwKMTAsQ29tcGFueSBEaWdpdGFsIEdtYkgsMTAsR3JvdXAsR2VybWFueSxFdXJvcGUsMTAsNiwxfDZ8MTAsMSw2LDEwLCwsQ29tcGFueSBIb2xkaW5ncyxDb21wYW55IEZpbmFuY2UgTHRkLENvbXBhbnkgRGlnaXRhbCBHbWJILCwKMTEsQ29tcGFueSBEaWdpdGFsIEZyYW5jZSwxMSxHcm91cCxGcmFuY2UsRXVyb3BlLDExLDYsMXw2fDExLDEsNiwxMSwsLENvbXBhbnkgSG9sZGluZ3MsQ29tcGFueSBGaW5hbmNlIEx0ZCxDb21wYW55IERpZ2l0YWwgRnJhbmNlLCwKMTIsQ29tcGFueSBBc2lhIEhvbGRpbmdzLDEyLEdyb3VwLFNpbmdhcG9yZSxBc2lhLDEyLDEsMXwxMiwxLDEyLCwsLENvbXBhbnkgSG9sZGluZ3MsQ29tcGFueSBBc2lhIEhvbGRpbmdzLCwsCjEzLENvbXBhbnkgQXNpYSBUZWNoLDEzLEdyb3VwLEluZGlhLEFzaWEsMTMsMTIsMXwxMnwxMywxLDEyLDEzLCwsQ29tcGFueSBIb2xkaW5ncyxDb21wYW55IEFzaWEgSG9sZGluZ3MsQ29tcGFueSBBc2lhIFRlY2gsLAoxNCxDb21wYW55IEFzaWEgRmluYW5jZSwxNCxHcm91cCxIb25nIEtvbmcsQXNpYSwxNCwxMiwxfDEyfDE0LDEsMTIsMTQsLCxDb21wYW55IEhvbGRpbmdzLENvbXBhbnkgQXNpYSBIb2xkaW5ncyxDb21wYW55IEFzaWEgRmluYW5jZSwsCjE1LENvbXBhbnkgTG9naXN0aWNzIEFzaWEsMTUsR3JvdXAsU2luZ2Fwb3JlLEFzaWEsMTUsMTIsMXwxMnwxNSwxLDEyLDE1LCwsQ29tcGFueSBIb2xkaW5ncyxDb21wYW55IEFzaWEgSG9sZGluZ3MsQ29tcGFueSBMb2dpc3RpY3MgQXNpYSwsCjE2LENvbXBhbnkgTG9naXN0aWNzIEluZGlhLDE2LEdyb3VwLEluZGlhLEFzaWEsMTYsMTUsMXwxMnwxNXwxNiwxLDEyLDE1LDE2LCxDb21wYW55IEhvbGRpbmdzLENvbXBhbnkgQXNpYSBIb2xkaW5ncyxDb21wYW55IExvZ2lzdGljcyBBc2lhLENvbXBhbnkgTG9naXN0aWNzIEluZGlhLAoxNyxDb21wYW55IExvZ2lzdGljcyBIb25rIEtvbmcsMTcsR3JvdXAsSW5kaWEsQXNpYSwxNywxNiwxfDEyfDE1fDE2fDE3LDEsMTIsMTUsMTYsMTcsQ29tcGFueSBIb2xkaW5ncyxDb21wYW55IEFzaWEgSG9sZGluZ3MsQ29tcGFueSBMb2dpc3RpY3MgQXNpYSxDb21wYW55IExvZ2lzdGljcyBJbmRpYSxDb21wYW55IExvZ2lzdGljcyBIb25rIEtvbmc=
"""
TABLE_NAME = "LegalEntity"   # target Delta table name

# Expected CSV columns (exact order from your Base64 header)
expected_cols = [
    "Legal_Entity_Key","Legal_entity_name","Legal_Entity_Sort_Order",
    "Group","Country","Region","Child","Parent","Path",
    "Legal_entity_level_1","Legal_entity_level_2","Legal_entity_level_3",
    "Legal_entity_level_4","Legal_entity_level_5",
    "Legal_entity_level_1_name","Legal_entity_level_2_name",
    "Legal_entity_level_3_name","Legal_entity_level_4_name",
    "Legal_entity_level_5_name"
]

# Spark schema for the legal-entity hierarchy
# NOTE: some hierarchy columns can contain pipe-delimited values (e.g., "1|2"),
# so we keep them as strings to avoid parsing errors.
spark_schema = T.StructType([
    T.StructField("Legal_Entity_Key",            T.IntegerType(), True),
    T.StructField("Legal_entity_name",           T.StringType(),  True),
    T.StructField("Legal_Entity_Sort_Order",     T.IntegerType(), True),
    T.StructField("Group",                       T.StringType(),  True),
    T.StructField("Country",                     T.StringType(),  True),
    T.StructField("Region",                      T.StringType(),  True),
    T.StructField("Child",                       T.StringType(),  True),
    T.StructField("Parent",                      T.StringType(),  True),
    T.StructField("Path",                        T.StringType(),  True),
    T.StructField("Legal_entity_level_1",        T.IntegerType(),  True),
    T.StructField("Legal_entity_level_2",        T.IntegerType(),  True),
    T.StructField("Legal_entity_level_3",        T.IntegerType(),  True),
    T.StructField("Legal_entity_level_4",        T.IntegerType(),  True),
    T.StructField("Legal_entity_level_5",        T.IntegerType(),  True),
    T.StructField("Legal_entity_level_1_name",   T.StringType(),  True),
    T.StructField("Legal_entity_level_2_name",   T.StringType(),  True),
    T.StructField("Legal_entity_level_3_name",   T.StringType(),  True),
    T.StructField("Legal_entity_level_4_name",   T.StringType(),  True),
    T.StructField("Legal_entity_level_5_name",   T.StringType(),  True),
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
missing = set(expected_cols) - set(pdf.columns)
if missing:
    raise ValueError(f"CSV missing expected columns: {sorted(missing)}")

# Trim whitespace in all string-like columns
for c in pdf.columns:
    if pdf[c].dtype == object:
        pdf[c] = pdf[c].astype(str).str.strip()

# Coerce numerics where safe
for c in ["Legal_Entity_Key", "Legal_Entity_Sort_Order"]:
    pdf[c] = pd.to_numeric(pdf[c], errors="coerce")

# ==== 3) Convert pandas -> Spark and enforce Spark dtypes ====
sdf = spark.createDataFrame(pdf)

# Cast columns to final schema (idempotent if already correct)
for f in spark_schema:
    if f.name in sdf.columns:
        sdf = sdf.withColumn(f.name, F.col(f.name).cast(f.dataType))

# Optional: de-duplicate by key/sort order + name if needed
# sdf = sdf.dropDuplicates(["Legal_Entity_Key", "Legal_entity_name"])

# ==== 4) Write managed Delta table in the attached Lakehouse ====
(sdf.write
    .mode("overwrite")        # change to "append" if you want to accumulate
    .format("delta")
    .saveAsTable(TABLE_NAME))

print(f"✅ Wrote Delta table: {TABLE_NAME}  (rows: {sdf.count()})")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Base64 Currency table -> Delta (Fabric)
import base64
import pandas as pd
from io import StringIO
from pyspark.sql import functions as F, types as T

# ==== CONFIG ====
BASE64_STR = """
Q3VycmVuY3lfS2V5LEN1cnJlbmN5LEN1cnJlbmN5X0dyb3VwLEN1cnJlbmN5X0dyb3VwX1NvcnRfT3JkZXIsVW5kZXJseWluZ19DdXJyZW5jeQo3LFVTRCxVU0QsMSwwCjQsR0JQLEdCUCwyLDAKNixFVVIsRVVSLDMsMAo1LElOUixJTlIsNCwwCjMsWkFSLFpBUiw1LDAKMixBVUQsT3RoZXIgLDYsMAoxLEJSTCxPdGhlciAsNiwwCjAsQ0FELE90aGVyICw2LDAK
"""
TABLE_NAME = "Currency"   # target Delta table name

# Expected columns in the CSV (taken from the header)
expected_cols = [
    "Currency_Key",
    "Currency",
    "Currency_Group",
    "Currency_Group_Sort_Order",
    "Underlying_Currency"
]

# Spark schema we want on the final table
spark_schema = T.StructType([
    T.StructField("Currency_Key",                T.IntegerType(), True),
    T.StructField("Currency",                    T.StringType(),  True),
    T.StructField("Currency_Group",              T.StringType(),  True),
    T.StructField("Currency_Group_Sort_Order",   T.IntegerType(), True),
    T.StructField("Underlying_Currency",         T.IntegerType(), True),
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
missing = set(expected_cols) - set(pdf.columns)
if missing:
    raise ValueError(f"CSV missing expected columns: {sorted(missing)}")

# Trim whitespace in all string-like columns
for c in pdf.columns:
    if pdf[c].dtype == object:
        pdf[c] = pdf[c].astype(str).str.strip()

# Coerce numerics
for c in ["Currency_Key", "Currency_Group_Sort_Order", "Underlying_Currency"]:
    pdf[c] = pd.to_numeric(pdf[c], errors="coerce")

# ==== 3) Convert pandas -> Spark and enforce Spark dtypes ====
sdf = spark.createDataFrame(pdf)

# Cast columns to final schema (idempotent if already correct)
for f in spark_schema:
    if f.name in sdf.columns:
        sdf = sdf.withColumn(f.name, F.col(f.name).cast(f.dataType))

# (Optional) de-duplicate by key if needed
# sdf = sdf.dropDuplicates(["Currency_Key"])

# ==== 4) Write managed Delta table in the attached Lakehouse ====
(sdf.write
    .mode("overwrite")          # change to "append" if you want to accumulate
    .format("delta")
    .saveAsTable(TABLE_NAME))

print(f"✅ Wrote Delta table: {TABLE_NAME}  (rows: {sdf.count()})")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
