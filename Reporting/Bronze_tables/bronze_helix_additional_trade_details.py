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
import pandas as pd


# Get the absolute path of the current script
script_path = os.path.abspath(__file__)

# Get the directory of the script (Bronze_tables directory)
script_dir = os.path.dirname(script_path)

# Add the parent directory of the script to the Python module search path
sys.path.insert(0, os.path.dirname(script_dir))

from Utils.database_utils import get_database_engine

engine = get_database_engine('sql_server_2')
tb_name = 'bronze_additional_trade_details'



metadata = MetaData()
metadata.bind = engine

Table(tb_name,
      metadata,
      Column('helix_trade_id', Integer, primary_key=True, autoincrement=False),
      Column('margin_cushion', Float, default=0),
      Column('bond_price', Float),
      Column('trade_category', CHAR(5)),
      Column('maturity_bucket', Text),
      Column('future_roll_notes', Text),
      Column('downgrade_terms', CHAR(5)),
      Column('risk_notes', Text),
      Column('username', VARCHAR(50))
)
       
metadata.create_all(engine)

