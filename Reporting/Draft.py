from datetime import datetime

from Utils.SQL_queries import HELIX_price_and_factor_by_date
from Utils.database_utils import (
    read_table_from_db,
    prod_db_type,
    execute_sql_query_v2,
    helix_db_type,
)

report_date_dt = datetime.strptime("2024-09-23", "%Y-%m-%d").date()

df_price_and_factor_backup = read_table_from_db(
    "bronze_helix_price_and_factor", prod_db_type
)
df_price_and_factor_backup = df_price_and_factor_backup[
    df_price_and_factor_backup["data_date"] == report_date_dt
][["bond_id", "factor"]]

df_price_and_factor_backup = df_price_and_factor_backup.rename(
    columns={"bond_id": "BondID", "factor": "Helix_factor"}
)

print(df_price_and_factor_backup)


df_price_and_factor = execute_sql_query_v2(
    HELIX_price_and_factor_by_date, db_type=helix_db_type, params=(report_date_dt,)
)

df_factor = df_price_and_factor[["BondID", "Helix_factor"]]
