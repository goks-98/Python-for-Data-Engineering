import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base import BaseHook
import pandas as pd
from sqlalchemy import create_engine

#extract tasks
@task()
def get_src_tables():
    hook = MsSqlHook(mssql_conn_id="sqlserver")
    sql = """ SELECT  '[' + table_catalog + '].[' + TABLE_SCHEMA + '].[' + table_name + ']' as table_name FROM INFORMATION_SCHEMA.TABLES 
                                        WHERE TABLE_NAME IN ('Product', 'SalesPerson') """
    df = hook.get_pandas_df(sql)
    print(df)
    tbl_dict = df.to_dict('dict')
    return tbl_dict
#
@task()
def load_src_data(tbl_dict: dict):
    conn = BaseHook.get_connection('postgres')
    pg_engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    all_tbl_name = []
    start_time = time.time()
    #access the table_name element in dictionaries
    for k, v in tbl_dict['table_name'].items():
        #print(v)
        all_tbl_name.append(v)
        rows_imported = 0
        sql = f'select * FROM {v}'
        hook = MsSqlHook(mssql_conn_id="sqlserver")
        df = hook.get_pandas_df(sql)
        df.to_sql(f'src_{v[21:]}', pg_engine, if_exists='replace', index=False)
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {v} ')
        rows_imported += len(df)
        print(f'Done. {str(round(time.time() - start_time, 2))} total seconds elapsed')
    print("Data imported successfully !")
    return all_tbl_name

#Transformation tasks
@task()
def transform_srcProduct():
    conn = BaseHook.get_connection('postgres')
    pg_engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pdf = pd.read_sql_query('SELECT * FROM public."Product" ', pg_engine)
    #drop columns
    revised = pdf[['ProductID', 'Name', 'ProductNumber', 'Color', 'SafetyStockLevel', 'StandardCost',
                   'ListPrice','Size', 'ModifiedDate']]
    #replace nulls
    revised['StandardCost'].fillna('0', inplace=True)
    revised['ListPrice'].fillna('0', inplace=True)
    revised['Size'].fillna('Unknown', inplace=True)
    revised['Color'].fillna('Unknown', inplace=True)
    revised['ModifiedDate'] = datetime.now()
    # Rename columns with rename function
    revised.to_sql(f'stg_DimProduct', pg_engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported successfully"}

@task()
def transform_srcSalesPerson():
    conn = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pdf = pd.read_sql_query('SELECT * FROM public."SalesPerson"', engine)
    #replace nulls
    pdf['TerritoryID'].fillna('-1', inplace=True)
    pdf['SalesQuota'].fillna('0', inplace=True)
    pdf['ModifiedDate'] = datetime.now()
    pdf.to_sql(f'stg_SalesPerson', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported successful"}


# [START how_to_task_group]
with DAG(dag_id="etl_dag_srcproduct",schedule="0 9 * * *", start_date=datetime(2023, 1, 29),catchup=False,  tags=["product_model"]) as dag:

    with TaskGroup("src_extract_load", tooltip="Extract and load source data") as src_extract_load:
        src_product_tbls = get_src_tables()
        load_dimProducts = load_src_data(src_product_tbls)
        #define order
        src_product_tbls >> load_dimProducts

    # [START howto_task_group_section_2]
    with TaskGroup("transform_src_data", tooltip="Transform and stage data") as transform_src_data:
        transform_srcProduct = transform_srcProduct()
        transform_srcSalesPerson = transform_srcSalesPerson()
        #define task order
        [transform_srcProduct, transform_srcSalesPerson]

    src_extract_load >> transform_src_data 

