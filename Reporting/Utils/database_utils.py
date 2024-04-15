import os
import platform
from contextlib import contextmanager

import pandas as pd
import pyodbc
from sqlalchemy import create_engine

import pymssql
# server = '172.31.32.100'
# database = 'Prod1'
# username = 'LUCID\\tony.hoang'
# password = os.getenv('MY_PASSWORD')
#
# conn = pymssql.connect(server, username, password, database)
# cursor = conn.cursor()

# Configuration
DB_CONFIG = {
    "postgres": {
        "db_endpoint": "luciddb1.czojmxqfrx7k.us-east-1.rds.amazonaws.com",
        "db_port": "5432",
        "db_user": "dbmasteruser",
        "db_password": "lnRz*(N_7aOf~7Hx6oRo8;,<vYp|~#PC",
        "db_name": "reporting",
    },
    "sql_server_1": {
        "driver": "{ODBC Driver 17 for SQL Server}",
        "server": "LUCIDSQL1",
        "database": "HELIXREPO_PROD_02",
        "trusted_connection": "yes"
    },
    "sql_server_2": {
        "driver": "{ODBC Driver 17 for SQL Server}",
        "server": "LUCIDSQL2",
        "database": "Prod1",
        "trusted_connection": "yes",
        "domain": "LUCID",
        "user": "tony.hoang",
        "password": os.getenv('MY_PASSWORD'),
    }
}


def get_database_engine(db_type):
    if db_type == "postgres":
        database_url = f"postgresql://{DB_CONFIG['postgres']['db_user']}:{DB_CONFIG['postgres']['db_password']}@{DB_CONFIG['postgres']['db_endpoint']}:{DB_CONFIG['postgres']['db_port']}/{DB_CONFIG['postgres']['db_name']}"
        return create_engine(database_url)
    elif db_type.startswith("sql_server"):
        if platform.system() == 'Darwin':  # macOS
            conn_str = (
                DB_CONFIG[db_type]['server'],
                DB_CONFIG[db_type]['user'],
                DB_CONFIG[db_type]['password'],
                DB_CONFIG[db_type]['database'],
            )
            return pymssql.connect(conn_str)
        elif platform.system() == 'Windows':
            conn_str = (
                f"DRIVER={DB_CONFIG[db_type]['driver']};"
                f"SERVER={DB_CONFIG[db_type]['server']};"
                f"DATABASE={DB_CONFIG[db_type]['database']};"
                f"Trusted_Connection={DB_CONFIG[db_type]['trusted_connection']};"
            )
            return pyodbc.connect(conn_str)
        else:
            raise Exception('Unsupported platform')


def read_table_from_db(table_name, db_type):
    engine = get_database_engine(db_type)
    if db_type.startswith("sql_server"):
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, con=engine)
    elif db_type == "postgres":
        return pd.read_sql_table(table_name, con=engine)


def execute_sql_query(sql_query, db_type, params=None):
    engine = get_database_engine(db_type)
    if db_type.startswith("sql_server"):
        return pd.read_sql(sql_query, con=engine, params=params)
    elif db_type == "postgres":
        return pd.read_sql(sql_query, con=engine, params=params)


@contextmanager
def DatabaseConnection(db_type):
    engine = get_database_engine(db_type)
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()
