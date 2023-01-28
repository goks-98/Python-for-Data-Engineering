#import needed libraries
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import urllib

#load environment variables
load_dotenv()

#get password from environmnet var
pwd = os.getenv('mssql_pwd')
uid = os.getenv('mssql_uid')
#sql db details
driver = os.getenv('mssql_driver')
server = os.getenv('mssql_server')
database = os.getenv("mssql_database")

#extract data from sql server
def extract():
    #Defining Connections
    params = urllib.parse.quote_plus(f"DRIVER={driver};"
                                    f"SERVER={server};"
                                    f"DATABASE={database};"
                                    "Trusted_Connection=yes;"
                                    "TrustServerCertificate=yes")
                                    
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    #Defining how many tables to extract
    no_of_tables = int(input("Enter no. of tables to be extracted: "))
    i = 0
    list_of_tables = []

    #Getting the table names to be extracted and checking if it exists 
    while i < no_of_tables:
        table_name = input("Enter 3 part table name to be extracted: ")
        try:
            pd.read_sql_query(f"SELECT * FROM {table_name};", engine)
            list_of_tables.append(table_name)
            i+=1
        except Exception as e:
            print("Data extract error: " + str(e))
    return print(list_of_tables)

#Loading PostgreSQL connection details
pg_driver = os.getenv('postgre_driver') 
pg_database = os.getenv('postgre_database') 
pg_server = os.getenv('postgre_server') 
pg_port = os.getenv('postgre_port') 
pg_uid = os.getenv('postgre_uid') 
pg_pwd = os.getenv('postgre_pwd') 

#load data to postgres
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql+psycopg2://{pg_uid}:{pg_pwd}@{pg_server}:{pg_port}/{pg_database}')
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to postgres
        rows_imported += len(df)
        # add elapsed time to final print out
        print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))

#Executing the Pipeline
try:
    #call extract function
    src_tables = extract()
    for tbl in src_tables:
            #query and load save data to dataframe
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', src_conn)
            load(df, tbl[0])
except Exception as e:
    print("Error while extracting data: " + str(e))


