
###############################################################################################################################################################################
#UDF 1 Commentary

import fabric.functions as fn
##import datetime as datefn

udf = fn.UserDataFunctions()

@udf.connection(argName="dbo",alias="profitandlossdb") 
@udf.function() 

# Users will provide these parameters in the PowerBI report
def write_one_to_sql_db_commentary_attempt3(dbo: fn.FabricSqlConnection, commentaryDescription: str, Level: str, Date: str) -> str: 

    # Establish a connection to the SQL database  
    connection = dbo.connect() 
    cursor = connection.cursor() 

    # Insert data into the ProductDescription table  
    insert_description_query = "INSERT INTO [dbo].[Commentary] (Description, Level, Date) OUTPUT INSERTED.CommentaryID VALUES (?, ?, ?)" 
    cursor.execute(insert_description_query, ( commentaryDescription, Level, Date ))

    # Commit the transaction 
    connection.commit() 
    cursor.close() 
    connection.close()  

    return "Commentary was added"

    ###############################################################################################################################################################################