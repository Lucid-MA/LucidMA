import os
import sys
from sqlalchemy import (
    MetaData,
    Column,
    Date,
    DateTime,
    Table,
    Text,
    Float,
    Integer,
    CHAR,
    VARCHAR
)
from sqlalchemy.schema import CreateTable
import pandas as pd


# Get the absolute path of the current script
script_path = os.path.abspath(__file__)

# Get the directory of the script (Bronze_tables directory)
script_dir = os.path.dirname(script_path)

# Add the parent directory of the script to the Python module search path
sys.path.insert(0, os.path.dirname(script_dir))

from Utils.database_utils import get_database_engine

engine = get_database_engine('sql_server_2')
tb_name = 'bronze_CUSIP_Prices'

metadata = MetaData()
metadata.bind = engine

tb = Table(tb_name,
      metadata,
      Column('cusip', VARCHAR(100), primary_key=True, autoincrement=False),
      Column('pricing_source', VARCHAR(50)),
      Column('used_price', Float),
      Column('last_date_pulled', Date),
      Column('JPPD', Float),
      Column('IDC', Float)
)

create_table_query = str(CreateTable(tb).compile(engine))
print(create_table_query)

metadata.create_all(engine)
