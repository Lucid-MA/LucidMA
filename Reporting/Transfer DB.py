import os
import platform

import pandas as pd
from sqlalchemy import create_engine

# Configuration
DB_CONFIG = {
    "postgres": {
        "db_endpoint": "luciddb1.czojmxqfrx7k.us-east-1.rds.amazonaws.com",
        "db_port": "5432",
        "db_user": "dbmasteruser",
        "db_password": "lnRz*(N_7aOf~7Hx6oRo8;,<vYp|~#PC",
        "db_name": "spiral_prod",
    },
    "sql_server_1": {
        "driver": "ODBC+Driver+17+for+SQL+Server",
        "server_mac": "172.31.0.10",
        "server_windows": "LUCIDSQL1",
        "database": "HELIXREPO_PROD_02",
        "trusted_connection": "yes",
        "user_mac": "Lucid\\tony.hoang",
        "user_windows": "tony.hoang",
        "password": os.getenv("MY_PASSWORD"),
    },
    "sql_server_2": {
        "driver": "ODBC+Driver+17+for+SQL+Server",
        "server_mac": "172.31.32.100",
        "server_windows": "LUCIDSQL2",
        "database": "Prod1",
        "trusted_connection": "yes",
        "user_mac": "Lucid\\tony.hoang",
        "user_windows": "tony.hoang",
        "password": os.getenv("MY_PASSWORD"),
    },
}


def get_database_engine(db_type):
    if db_type == "postgres":
        database_url = f"postgresql://{DB_CONFIG['postgres']['db_user']}:{DB_CONFIG['postgres']['db_password']}@{DB_CONFIG['postgres']['db_endpoint']}:{DB_CONFIG['postgres']['db_port']}/{DB_CONFIG['postgres']['db_name']}"
        return create_engine(database_url)

    elif db_type.startswith("sql_server"):
        if platform.system() == "Darwin":  # macOS
            conn_str = f"mssql+pymssql://{DB_CONFIG[db_type]['user_mac']}:{DB_CONFIG[db_type]['password']}@{DB_CONFIG[db_type]['server_mac']}/{DB_CONFIG[db_type]['database']}"
            return create_engine(conn_str)

        elif platform.system() == "Windows":
            conn_str = (
                f"mssql+pyodbc://{DB_CONFIG[db_type]['user_windows']}:{DB_CONFIG[db_type]['password']}@"
                f"{DB_CONFIG[db_type]['server_windows']}/{DB_CONFIG[db_type]['database']}?driver={DB_CONFIG[db_type]['driver']}&Trusted_Connection={DB_CONFIG[db_type]['trusted_connection']}"
            )
            return create_engine(conn_str)

        else:
            raise Exception("Unsupported platform")


def read_table_from_db(table_name, db_type):
    engine = get_database_engine(db_type)
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, con=engine)


def insert_data_to_sql_server(df, table_name, db_type):
    engine = get_database_engine(db_type)
    df.to_sql(table_name, con=engine, if_exists="append", index=False)


def transfer_data(postgres_table, sql_table):
    # Step 1: Read data from PostgreSQL
    print("Reading data from PostgreSQL...")
    postgres_data = read_table_from_db(postgres_table, "postgres")

    # Step 2: Insert data into MS SQL Server
    print("Inserting data into MS SQL Server...")
    insert_data_to_sql_server(postgres_data, sql_table, "sql_server_2")
    print("Data transfer complete.")


# Example usage:
if __name__ == "__main__":
    transfer_data('"VAR_Model"."VAR_Results"', "VAR_Results")
