from datetime import datetime

from Utils.SQL_queries import (
    AUM_query,
)
from Utils.database_utils import get_database_engine, execute_sql_query

report_date = "2024-07-17"
TABLE_NAME = "lucid_aum"

engine = get_database_engine("sql_server_1")

CustomDate = datetime.strptime(report_date, "%Y-%m-%d")

df_test = execute_sql_query(AUM_query, "sql_server_1", params=(CustomDate,))
print(df_test.head(10))
