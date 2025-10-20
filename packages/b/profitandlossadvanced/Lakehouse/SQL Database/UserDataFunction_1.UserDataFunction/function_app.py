

# UDF: echo_hello  (no connections, no params)
import fabric.functions as fn
udf = fn.UserDataFunctions()

@udf.function()
def echo_hello() -> str:
    return "hello from UDF"




###################################


# UDF: db_ping  (has connection, but only reads server info)
import fabric.functions as fn, json
udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB", alias="profitandlossdb")
@udf.function()
def db_ping(sqlDB: fn.FabricSqlConnection) -> str:
    conn = sqlDB.connect()
    cur = conn.cursor()
    cur.execute("SELECT DB_NAME() AS DbName, SUSER_SNAME() AS SqlIdentity")
    row = cur.fetchone(); cols = [c[0] for c in cur.description]
    cur.close(); conn.close()
    return json.dumps(dict(zip(cols, row)))

###########################

# UDF: table_probe  (read-only checks on your table)
import fabric.functions as fn, json
udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB", alias="profitandlossdb")
@udf.function()
def table_probe(sqlDB: fn.FabricSqlConnection) -> str:
    q = """
    SELECT
      IIF(OBJECT_ID('dbo.CommentaryDescription') IS NULL,0,1) AS TableExists,
      HAS_PERMS_BY_NAME('dbo.CommentaryDescription','OBJECT','INSERT') AS CanInsert
    """
    conn = sqlDB.connect(); cur = conn.cursor()
    cur.execute(q); row = cur.fetchone(); cols=[c[0] for c in cur.description]
    cur.close(); conn.close()
    return json.dumps(dict(zip(cols, row)))

#############################
import fabric.functions as fn
import traceback

udf = fn.UserDataFunctions()

##hfgh

@udf.connection(argName="sqlDB", alias="profitandlossdb")
@udf.function()
def probe_env(sqlDB: fn.FabricSqlConnection) -> str:
    conn = sqlDB.connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            DB_NAME() AS DbName,
            SUSER_SNAME() AS SqlIdentity,
            IIF(OBJECT_ID('dbo.CommentaryDescription') IS NULL,0,1) AS TableExists,
            HAS_PERMS_BY_NAME('dbo.CommentaryDescription','OBJECT','INSERT') AS CanInsert
    """)
    row = cur.fetchone()
    cols = [c[0] for c in cur.description]
    cur.close(); conn.close()
    # return a compact string to avoid PB truncation
    return "; ".join(f"{k}={v}" for k, v in dict(zip(cols, row)).items())

    



########################################
# UDF 1 Commentary (fixed)
import fabric.functions as fn
import traceback

udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB", alias="profitandlossdb")
@udf.function()
def add_commentary_logged(sqlDB: fn.FabricSqlConnection, commentaryText: str) -> str:
    if commentaryText is None:
        commentaryText = ""
    if len(commentaryText) > 400:
        commentaryText = commentaryText[:400]  # keep insert safe if you still use NVARCHAR(400)

    conn = sqlDB.connect()
    cur = conn.cursor()
    try:
        # Who/where am I (for auditing the PB caller)?
        cur.execute("SELECT DB_NAME(), SUSER_SNAME()")
        dbname, caller = cur.fetchone()

        # Try the insert
        cur.execute(
            "INSERT INTO dbo.CommentaryDescription (Description) "
            "OUTPUT INSERTED.CommentaryDescriptionID VALUES (?)",
            commentaryText
        )
        inserted_id = cur.fetchone()[0]
        conn.commit()

        # Log success
        cur.execute("""
            INSERT INTO dbo.UdfInvocationLog
                (UdfName, CallerPrincipal, DbName, InputSummary, Succeeded, InsertedID)
            VALUES
                (?, ?, ?, ?, 1, ?)
        """, ("add_commentary_logged", caller, dbname, commentaryText[:200], inserted_id))
        conn.commit()

        return f"OK: CommentaryDescriptionID={inserted_id}"

    except Exception as e:
        tb = traceback.format_exc()
        # Log failure (don’t rely on PB showing details)
        try:
            cur.execute("""
                INSERT INTO dbo.UdfInvocationLog
                    (UdfName, CallerPrincipal, DbName, InputSummary, Succeeded,
                     ErrorType, ErrorMessage, ErrorTrace)
                VALUES (?, SUSER_SNAME(), DB_NAME(), ?, 0, ?, ?, ?)
            """, (
                "add_commentary_logged",
                commentaryText[:200],
                type(e).__name__,
                str(e),
                tb
            ))
            conn.commit()
        except Exception:
            # If even logging fails, we still want to return *something*
            pass

        # Return a short status so the visual won’t mask it further
        return "ERROR: see dbo.UdfInvocationLog for details"
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()



###############################################################################################################################################################################
#UDF 1 Commentary

import fabric.functions as fn
import uuid

udf = fn.UserDataFunctions()

@udf.connection(argName="dbo",alias="profitandlossdb") 
@udf.function() 

# Users will provide these parameters in the PowerBI report
def write_one_to_sql_db_commentary_attempt3(dbo: fn.FabricSqlConnection, commentaryDescription: str) -> str: 

    # Establish a connection to the SQL database  
    connection = dbo.connect() 
    cursor = connection.cursor() 

    # Insert data into the ProductDescription table  
    insert_description_query = "INSERT INTO [dbo].[CommentaryDescription] (Description) OUTPUT INSERTED.CommentaryDescriptionID VALUES (?)" 
    cursor.execute(insert_description_query, commentaryDescription) 

    # Commit the transaction 
    connection.commit() 
    cursor.close() 
    connection.close()  

    return "Commentary was added"

    ###############################################################################################################################################################################
#UDF 2 Product

import fabric.functions as fn
import uuid

udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB",alias="profitandlossdb") 
@udf.function() 

# Take a product description and product model ID as input parameters and write them back to the SQL database
# Users will provide these parameters in the PowerBI report
def write_one_to_sql_db_product(sqlDB: fn.FabricSqlConnection, productDescription: str) -> str: 

    # Error handling to ensure product description doesn't go above 200 characters
    if(len(productDescription) > 200):
        raise fn.UserThrownError("Descriptions have a 200 character limit. Please shorten your description.", {"Description:": productDescription})

    # Establish a connection to the SQL database  
    connection = sqlDB.connect() 
    cursor = connection.cursor() 

    # Insert data into the ProductDescription table  
    insert_description_query = "INSERT INTO [dbo].[ProductDescription] (Description) OUTPUT INSERTED.ProductDescriptionID VALUES (?)" 
    cursor.execute(insert_description_query, productDescription) 


    # Commit the transaction 
    connection.commit() 
    cursor.close() 
    connection.close()  

    return "Product description was added"





###############################################################################################################################################################################

import fabric.functions as fn
import uuid

udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB",alias="profitandlossdb") 
@udf.function() 

# Take a product description and product model ID as input parameters and write them back to the SQL database
# Users will provide these parameters in the PowerBI report
def write_one_to_sql_db_commentary_with_modified(sqlDB: fn.FabricSqlConnection, Description: str) -> str: 

    # Error handling to ensure product description doesn't go above 200 characters
    if(len(Description) > 200):
        raise fn.UserThrownError("Descriptions have a 200 character limit. Please shorten your description.", {"Description:": Description})

    # Establish a connection to the SQL database  
    connection = sqlDB.connect() 
    cursor = connection.cursor() 

    # Insert data into the ProductDescription table  
    insert_description_query = "INSERT INTO [dbo].[Commentary] (Description) OUTPUT INSERTED.CommentaryID VALUES (?)" 
    cursor.execute(insert_description_query, Description) 


    # Commit the transaction 
    connection.commit() 
    cursor.close() 
    connection.close()  

    return "Product description was added"


###############################################################################################################################################################################





import fabric.functions as fn
import uuid

udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB",alias="profitandlossdb") 
@udf.function() 

# Take a product description and product model ID as input parameters and write them back to the SQL database
# Users will provide these parameters in the PowerBI report
def write_one_to_sql_db_commentary_multiple(sqlDB: fn.FabricSqlConnection, Commentary: str, Level:str) -> str: 

    # Error handling to ensure product description doesn't go above 200 characters
    if(len(Commentary) > 200):
        raise fn.UserThrownError("Descriptions have a 200 character limit. Please shorten your description.", {"Description:": Commentary})

    # Establish a connection to the SQL database  
    connection = sqlDB.connect() 
    cursor = connection.cursor() 

    # Insert data into the Commentary table  
    insert_description_query = "INSERT INTO [dbo].[Commentary] (Commentary, Level) OUTPUT INSERTED.Level VALUES (?, ?)" 
    #cursor.execute(insert_description_query, Commentary) 
    cursor.execute(insert_description_query, (Commentary, Level)) 

    # Get the result from the previous query 
    #results = cursor.fetchall() 

    # In real-world cases, call an API to retrieve the cultureId
    # For this example, generate a random Id instead
    #cultureId = str(uuid.uuid4()) 

    # Insert data into the ProductModelProductDescription table 
   # insert_model_description_query = "INSERT INTO [dbo].[commentary] (ProductModelID, ProductDescriptionID, Culture) VALUES (?, ?, ?);" 
    #cursor.execute(insert_model_description_query, (productModelId, results[0][0], cultureId[:6])) 

    # Commit the transaction 
    connection.commit() 
    cursor.close() 
    connection.close()  

    return "Product description was added"

###############################################################################################################################################################################

import fabric.functions as fn
import uuid

udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB",alias="profitandlossdb") 
@udf.function() 

# Take a product description and product model ID as input parameters and write them back to the SQL database
# Users will provide these parameters in the PowerBI report
def write_one_to_sql_db_commentary(sqlDB: fn.FabricSqlConnection, Commentary: str) -> str: 

    # Error handling to ensure product description doesn't go above 200 characters
    if(len(Commentary) > 200):
        raise fn.UserThrownError("Descriptions have a 200 character limit. Please shorten your description.", {"Description:": Commentary})

    # Establish a connection to the SQL database  
    connection = sqlDB.connect() 
    cursor = connection.cursor() 

    # Insert data into the Commentary table  
    insert_description_query = "INSERT INTO [dbo].[Commentary] (Commentary) OUTPUT INSERTED.Level VALUES (?)" 
    cursor.execute(insert_description_query, Commentary) 

    # Get the result from the previous query 
    #results = cursor.fetchall() 

    # In real-world cases, call an API to retrieve the cultureId
    # For this example, generate a random Id instead
    #cultureId = str(uuid.uuid4()) 

    # Insert data into the ProductModelProductDescription table 
   # insert_model_description_query = "INSERT INTO [dbo].[commentary] (ProductModelID, ProductDescriptionID, Culture) VALUES (?, ?, ?);" 
    #cursor.execute(insert_model_description_query, (productModelId, results[0][0], cultureId[:6])) 

    # Commit the transaction 
    connection.commit() 
    cursor.close() 
    connection.close()  

    return "Product description was added"

###############################################################################################################################################################################

import fabric.functions as fn
import uuid

udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB",alias="AdventureWorksL") 
@udf.function() 

# Take a product description and product model ID as input parameters and write them back to the SQL database
# Users will provide these parameters in the PowerBI report
def write_one_to_sql_db(sqlDB: fn.FabricSqlConnection, productDescription: str, productModelId:int) -> str: 

    # Error handling to ensure product description doesn't go above 200 characters
    if(len(productDescription) > 200):
        raise fn.UserThrownError("Descriptions have a 200 character limit. Please shorten your description.", {"Description:": productDescription})

    # Establish a connection to the SQL database  
    connection = sqlDB.connect() 
    cursor = connection.cursor() 

    # Insert data into the ProductDescription table  
    insert_description_query = "INSERT INTO [SalesLT].[ProductDescription] (Description) OUTPUT INSERTED.ProductDescriptionID VALUES (?)" 
    cursor.execute(insert_description_query, productDescription) 

    # Get the result from the previous query 
    results = cursor.fetchall() 

    # In real-world cases, call an API to retrieve the cultureId
    # For this example, generate a random Id instead
    cultureId = str(uuid.uuid4()) 

    # Insert data into the ProductModelProductDescription table 
    insert_model_description_query = "INSERT INTO [SalesLT].[ProductModelProductDescription] (ProductModelID, ProductDescriptionID, Culture) VALUES (?, ?, ?);" 
    cursor.execute(insert_model_description_query, (productModelId, results[0][0], cultureId[:6])) 

    # Commit the transaction 
    connection.commit() 
    cursor.close() 
    connection.close()  

    return "Product description was added"


