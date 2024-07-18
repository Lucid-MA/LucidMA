from datetime import datetime

from Utils.SQL_queries import (
    AUM_query,
)
from Utils.database_utils import (
    get_database_engine,
    execute_sql_query,
    execute_sql_query_v2,
)

report_date = "2024-07-17"
TABLE_NAME = "lucid_aum"

engine = get_database_engine("sql_server_1")

report_date = datetime.strptime(report_date, "%Y-%m-%d")
df_test = execute_sql_query_v2(AUM_query, "sql_server_1", params=(report_date,))

print(df_test.head(10))
