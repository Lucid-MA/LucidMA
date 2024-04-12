import platform
from contextlib import contextmanager

import pandas as pd
import pyodbc
from sqlalchemy import create_engine

from Utils.Common import print_df
from Utils.SQL_queries import OC_query
import mysql.connector
import os


import pymssql

from Utils.database_utils import read_table_from_db

db_type = "postgres"
table_name = "bronze_oc_rates"
report_date = '2024-04-05'
valdate = pd.to_datetime(report_date)
fund_name = 'Prime'
series_name = 'Monthly'

df_bronze = read_table_from_db(table_name, db_type)

# Filter the DataFrame based on the conditions
df_bronze = df_bronze[(df_bronze['End Date'] > valdate) | (df_bronze['End Date'].isnull())]

# Create a mask for the conditions
mask = (df_bronze['fund'] == fund_name) & (df_bronze['Series'] == series_name) & (df_bronze['Start Date'] <= valdate)
# Use the mask to filter the DataFrame and calculate the sum
df_bronze = df_bronze[mask]

print_df(df_bronze.head())

#
# server = '172.31.32.100'
# database = 'Prod1'
# username = 'LUCID\\tony.hoang'
# password = os.getenv('MY_PASSWORD')
# def create_db_connection(server, database, username, password):
#     if platform.system() == 'Darwin':  # macOS
#         print(platform.system())
#         conn = pymssql.connect(server, username, password, database)
#     elif platform.system() == 'Windows':
#         conn_str = (
#             f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#             f"SERVER={server};"
#             f"DATABASE={database};"
#             f"UID={username};"
#             f"PWD={password};"
#             f"Trusted_Connection=yes;"
#         )
#         conn = pyodbc.connect(conn_str)
#     else:
#         raise Exception('Unsupported platform')
#     return conn
#
#
#
# # conn = pymssql.connect(server, username, password, database)
# conn = create_db_connection(server, database, username, password)
# cursor = conn.cursor()
#
# cursor.execute('SELECT TOP 10 * FROM dbo.counterparties')
# results = cursor.fetchall()
# print(results)
#
# server = '172.31.32.100'
# database = 'Prod1'
# username = 'LUCID\\tony.hoang'
# password = os.getenv('MY_PASSWORD')
#
# # Specify the driver and setup your connection string
# driver = '{ODBC Driver 18 for SQL Server}'  # or the specific driver version you have installed
# conn_str = (
#     f"DRIVER={driver};"
#     f"SERVER={server};"
#     f"DATABASE={database};"
#     f"UID={username};"
#     f"PWD={password};"
#     f"TrustServerCertificate=yes;"  # Disable SSL certificate verification
#
# )
#
# # Connect to your database
# conn = pyodbc.connect(conn_str)
# cursor = conn.cursor()
#
# # Execute SQL query
# cursor.execute('SELECT TOP 10 * FROM dbo.counterparties')
# results = cursor.fetchall()
#
# # Print results
# for row in results:
#     print(row)
#
# # Clean up
# cursor.close()
# conn.close()
#
#
# # config = {
# #     'host': '172.31.0.10',
# #     'database': 'your_database_name',
# #     'user': 'HELIXREPO_PROD_02',
# #     'password': os.getenv('MY_PASSWORD')
# # }
# #
# # try:
# #     mydb = mysql.connector.connect(**config)
# #     print("Connection successful")
# #
# # except mysql.connector.Error as err:
# #     print(f"Something went wrong: {err}")
#
# # Configuration
# DB_CONFIG = {
#     "postgres": {
#         "db_endpoint": "luciddb1.czojmxqfrx7k.us-east-1.rds.amazonaws.com",
#         "db_port": "5432",
#         "db_user": "dbmasteruser",
#         "db_password": "lnRz*(N_7aOf~7Hx6oRo8;,<vYp|~#PC",
#         "db_name": "reporting",
#     },
#     "sql_server_1": {
#         "driver": "{ODBC Driver 17 for SQL Server}",
#         "server": "172.31.0.10",
#         "database": "HELIXREPO_PROD_02",
#         "user": "tony.hoang",
#         "password": os.getenv('MY_PASSWORD'),
#         "domain": "LUCID",
#         "authentication": "SQL Server Authentication"
#     },
#     "sql_server_2": {
#         "driver": "{ODBC Driver 17 for SQL Server}",
#         "server": "172.31.32.100",
#         "database": "Prod1",
#         "user": "LUCID\\tony.hoang",
#         "password": os.getenv('MY_PASSWORD'),
#         # "authentication": "NTLM"
#     }
# }
#
#
# def get_database_engine(db_type):
#     if db_type == "postgres":
#         database_url = f"postgresql://{DB_CONFIG['postgres']['db_user']}:{DB_CONFIG['postgres']['db_password']}@{DB_CONFIG['postgres']['db_endpoint']}:{DB_CONFIG['postgres']['db_port']}/{DB_CONFIG['postgres']['db_name']}"
#         return create_engine(database_url)
#     elif db_type.startswith("sql_server"):
#         conn_str = (
#             f"DRIVER={DB_CONFIG[db_type]['driver']};"
#             f"SERVER={DB_CONFIG[db_type]['server']};"
#             f"DATABASE={DB_CONFIG[db_type]['database']};"
#             f"UID={DB_CONFIG[db_type]['user']};"
#             # f"UID={DB_CONFIG[db_type]['domain']}\\{DB_CONFIG[db_type]['user']};"
#             f"PWD={DB_CONFIG[db_type]['password']}"
#             f"Trusted_Connection=yes;"
#             # if DB_CONFIG[db_type]['authentication'] == "NTLM" else ""
#         )
#         return pyodbc.connect(conn_str)
#
#
# def read_table_from_db(table_name, db_type):
#     engine = get_database_engine(db_type)
#     if db_type.startswith("sql_server"):
#         query = f"SELECT * FROM {table_name}"
#         return pd.read_sql(query, con=engine)
#     elif db_type == "postgres":
#         return pd.read_sql_table(table_name, con=engine)
#
#
# def execute_sql_query(sql_query, db_type, params=None):
#     engine = get_database_engine(db_type)
#     if db_type.startswith("sql_server"):
#         return pd.read_sql(sql_query, con=engine, params=params)
#     elif db_type == "postgres":
#         return pd.read_sql(sql_query, con=engine, params=params)
#
#
# @contextmanager
# def DatabaseConnection(db_type):
#     engine = get_database_engine(db_type)
#     conn = engine.connect()
#     try:
#         yield conn
#     finally:
#         conn.close()
#
# # table_name = "dbo.TRADEPIECES"
# db_type = "sql_server_2"
#
# sql_query = "SELECT * FROM dbo.counterparties"
# df = execute_sql_query(sql_query, db_type, params=[])