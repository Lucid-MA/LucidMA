import pandas as pd
from sqlalchemy import create_engine

# Configuration
DB_CONFIG = {
    "db_endpoint": "luciddb1.czojmxqfrx7k.us-east-1.rds.amazonaws.com",
    "db_port": "5432",
    "db_user": "dbmasteruser",
    "db_password": "lnRz*(N_7aOf~7Hx6oRo8;,<vYp|~#PC",
    "db_name": "reporting",
}


def get_database_engine():
    database_url = f"postgresql://{DB_CONFIG['db_user']}:{DB_CONFIG['db_password']}@{DB_CONFIG['db_endpoint']}:{DB_CONFIG['db_port']}/{DB_CONFIG['db_name']}"
    return create_engine(database_url)


def read_table_from_db(table_name):
    engine = get_database_engine()
    return pd.read_sql_table(table_name, con=engine)
