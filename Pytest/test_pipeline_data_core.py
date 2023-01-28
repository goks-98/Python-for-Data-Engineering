import pytest
import pandas as pd
import numpy as np
from numpy import nan
import os
from dotenv import load_dotenv
import urllib
from sqlalchemy import create_engine

@pytest.fixture
def df():
    #Connecting to MS SQL Server database
    load_dotenv()

    #get parameters from environmnet var
    pwd = os.getenv('mssql_pwd')
    uid = os.getenv('mssql_uid')
    #sql db details
    driver = os.getenv('mssql_driver')
    server = os.getenv('mssql_server')
    database = os.getenv("mssql_database")

    params = urllib.parse.quote_plus(f"DRIVER={driver};"
                                f"SERVER={server};"
                                f"DATABASE={database};"
                                "Trusted_Connection=yes;"
                                "TrustServerCertificate=yes")

    mssql_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    df = pd.read_sql_query(""" SELECT * FROM [Production].[Product] """, mssql_engine)
    return df

# check if column exists
def test_col_exists(df):
    name="ProductID"
    assert name in df.columns

# check for nulls
def test_null_check(df):
    assert np.where(df['ProductID'].isnull())

# check values are unique
def test_unique_check(df):
    assert pd.Series(df['ProductID']).is_unique

# check data type
def test_productkey_dtype_int(df):
    assert (df['ProductID'].dtype == int or df['ProductID'].dtype == np.int64)

# check data type
def test_productname_dtype_srt(df):
    assert (df['Name'].dtype == str or  df['Name'].dtype == 'O')

# check values in range
def test_range_val(df):
    assert df['SafetyStockLevel'].between(0,1000).any()

# check values in a list
def test_range_val_str(df):
    assert set(df['Color'].unique()) == {None, 'Black', 'Silver', 'Red', 'White', 'Blue', 'Multi', 'Yellow','Grey', 'Silver/Black'}

