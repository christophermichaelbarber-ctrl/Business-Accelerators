
###############################################################################################################################################################################
#UDF 1 Commentary

import fabric.functions as fn

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