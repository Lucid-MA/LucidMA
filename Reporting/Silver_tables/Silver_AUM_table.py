from datetime import datetime

import pandas as pd
from sqlalchemy import text

from Utils.SQL_queries import (
    AUM_query,
)
from Utils.database_utils import get_database_engine

report_date = "2024-07-17"
TABLE_NAME = "lucid_aum"

engine = get_database_engine("sql_server_1")

params = {"valdate": datetime.strptime(report_date, "%Y-%m-%d")}
df_bronze_oc = pd.read_sql(
    text(AUM_query), con=engine, params=params
)
print(df_bronze_oc.head(10))
