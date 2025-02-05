import os
import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import (
    inspect,
    text,
)

from Utils.Common import print_df
from Utils.SQL_queries import HELIX_price_and_factor_by_date, OC_query_historical_v2

# Get the absolute path of the current script
script_path = os.path.abspath(__file__)

# Get the directory of the script (Bronze_tables directory)
script_dir = os.path.dirname(script_path)

# Add the parent directory of the script to the Python module search path
sys.path.insert(0, os.path.dirname(script_dir))


from Utils.database_utils import (
    get_database_engine,
    execute_sql_query_v2,
    helix_db_type,
    engine_helix,
)

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PUBLISH_TO_PROD = True

if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
else:
    engine = get_database_engine("postgres")

inspector = inspect(engine)

report_date = "2021-06-07"
report_date_dt = datetime.strptime(report_date, "%Y-%m-%d").date()

df_factor = execute_sql_query_v2(
    HELIX_price_and_factor_by_date,
    db_type=helix_db_type,
    params=(report_date_dt,),
)

print_df(df_factor)
output_file = rf"S:\Users\THoang\Data\helix_factor_{report_date}.xlsx"
df_factor.to_excel(output_file, index=False)

params = {"valdate": datetime.strptime(report_date, "%Y-%m-%d")}
df_bronze_oc = pd.read_sql(
    text(OC_query_historical_v2), con=engine_helix, params=params
)

if df_bronze_oc.duplicated(subset="Trade ID").any():
    print("There are duplicates in the 'Trade ID' column.")
    print(df_bronze_oc[df_bronze_oc.duplicated(subset="Trade ID")]["Trade ID"].unique())
dtype_dict = {
    "fund": "string",
    "Series": "string",
    "TradeType": "string",
    "Counterparty": "string",
    "cp short": "string",
    "Comments": "string",
    "Product Type": "string",
    "Collateral Type": "string",
    "Start Date": "datetime64[ns]",
    "End Date": "datetime64[ns]",
    "Trade ID": "int64",
    "BondID": "string",
    "Money": "float64",
    "Orig. Rate": "float64",
    "Orig. Price": "float64",
    "Par/Quantity": "float64",
    "HairCut": "float64",
    "Spread": "float64",
    "End Money": "float64",
}
df_bronze_oc = df_bronze_oc.astype(dtype_dict).replace({pd.NaT: None})

output_file = rf"S:\Users\THoang\Data\bronze_oc_{report_date}.xlsx"
df_bronze_oc.to_excel(output_file, index=False)
print_df(df_bronze_oc)
