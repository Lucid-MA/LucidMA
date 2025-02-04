import os
import sys
from datetime import datetime

from sqlalchemy import (
    inspect,
)

from Utils.Common import print_df
from Utils.SQL_queries import HELIX_price_and_factor_by_date

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
