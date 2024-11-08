from Utils.Common import print_df, get_current_timestamp
from Utils.database_utils import execute_sql_query_v2, helix_db_type

matthew_query = """
select
case when tradepieces.company = 44 then 'USG' when tradepieces.company = 45 then 'Prime' when tradepieces.company = 46 then 'MMT' end fund,
tradepieces.statusmain as "Status",
Tradepieces.TRADEPIECE as "Trade ID",
RTRIM(ltrim(TRADETYPES.DESCRIPTION)) as "TradeType",
Tradepieces.STARTDATE as "Start Date",
CASE WHEN Tradepieces.CLOSEDATE is null then tradepieces.enddate else Tradepieces.CLOSEDATE 
END as "End Date",
Tradepieces.FX_MONEY as "Money",
RTRIM(LTRIM(Tradepieces.CONTRANAME)) as "Counterparty",
Tradepieces.REPORATE as "Orig. Rate",
Tradepieces.PRICE as "Orig. Price",
ltrim(rtrim(Tradepieces.ISIN)) as "BondID",
Tradepieces.PAR * case when tradepieces.tradetype in (0, 22) then -1 else 1 end as "Par/Quantity",
case when RTRIM(TRADETYPES.DESCRIPTION) in ('ReverseFree','RepoFree') then 0 else Tradepieces.HAIRCUT end as "HairCut",
Tradecommissionpieceinfo.commissionvalue Spread,
RTRIM(LTRIM(Tradepieces.ACCT_NUMBER)) 'cp short',
case when tradepieces.cusip = 'CASHUSD01' then 'USG' when tradepieces.tradepiece in (60320,60321,60258) then 'BBB' when tradepieces.comments = '' then ratings_tbl.rating else tradepieces.comments end as "Comments",
Tradepieces.FX_MONEY + TRADEPIECECALCDATAS.REPOINTEREST_UNREALIZED + TRADEPIECECALCDATAS.REPOINTEREST_NBD "End Money",
case when rtrim(ltrim(ISSUESUBTYPES3.DESCRIPTION)) = 'CLO CRE' then 'CMBS' else RTRIM(ltrim(CASE WHEN rtrim(ltrim(Tradepieces.cusip))='CASHUSD01' THEN 'USD Cash'
ELSE rtrim(ltrim(ISSUESUBTYPES2.DESCRIPTION))
END)) end "Product Type",
RTRIM(ltrim(CASE WHEN Tradepieces.cusip='CASHUSD01' THEN 'Cash'
ELSE ISSUESUBTYPES3.DESCRIPTION 
END)) "Collateral Type",
TRADEPIECECALCDATAS.CURRENTMARKETVALUE as "Market Cap"
from tradepieces 
INNER JOIN TRADEPIECECALCDATAS ON TRADEPIECECALCDATAS.TRADEPIECE=TRADEPIECES.TRADEPIECE
INNER JOIN TRADECOMMISSIONPIECEINFO ON TRADECOMMISSIONPIECEINFO.TRADEPIECE=TRADEPIECES.TRADEPIECE
INNER JOIN TRADETYPES ON TRADETYPES.TRADETYPE=TRADEPIECES.SHELLTRADETYPE
INNER JOIN ISSUES ON ISSUES.CUSIP=TRADEPIECEs.CUSIP
INNER JOIN CURRENCYS ON CURRENCYS.CURRENCY=TRADEPIECES.CURRENCY_MONEY
INNER JOIN STATUSDETAILS ON STATUSDETAILS.STATUSDETAIL=TRADEPIECES.STATUSDETAIL
INNER JOIN STATUSMAINS ON STATUSMAINS.STATUSMAIN=TRADEPIECES.STATUSMAIN
INNER JOIN ISSUECATEGORIES ON ISSUECATEGORIES.ISSUECATEGORY=TRADEPIECES.ISSUECATEGORY
INNER JOIN ISSUESUBTYPES1 ON ISSUESUBTYPES1.ISSUESUBTYPE1=ISSUECATEGORIES.ISSUESUBTYPE1
INNER JOIN ISSUESUBTYPES2 ON ISSUESUBTYPES2.ISSUESUBTYPE2=ISSUECATEGORIES.ISSUESUBTYPE2
INNER JOIN ISSUESUBTYPES3 ON ISSUESUBTYPES3.ISSUESUBTYPE3=ISSUECATEGORIES.ISSUESUBTYPE3
left join (
select distinct history_tradepieces.tradepiece, history_tradepieces.comments rating from history_tradepieces inner join (
select max(datetimeid) datetimeid, tradepiece from history_tradepieces inner join (select tradepiece tid from tradepieces where isvisible = 1) vistbl on vistbl.tid = history_tradepieces.tradepiece group by cast(datetimeid as date), tradepiece) maxtbl
on history_tradepieces.datetimeid = maxtbl.datetimeid and history_tradepieces.tradepiece = maxtbl.tradepiece
inner join (select tradepiece tid from tradepieces where isvisible = 1) vistbl on vistbl.tid = history_tradepieces.tradepiece
where cast(history_tradepieces.datetimeid as date) = cast(history_tradepieces.bookdate as date)
) ratings_tbl on ratings_tbl.tradepiece = tradepieces.tradepiece
where ((tradepieces.company = 44 and Tradepieces.LEDGERNAME =  'Monthly') or (tradepieces.company = 45 and Tradepieces.LEDGERNAME =  'Master')) and tradepieces.statusmain <> 6
and (tradetypes.description = 'Reverse' or tradetypes.description = 'ReverseFree' or tradetypes.description = 'RepoFree' or tradetypes.description = 'Repo')
order by Tradepieces.STARTDATE asc, tradepieces.contraname asc
"""

df = execute_sql_query_v2(matthew_query, helix_db_type)

print_df(df)

import os
import sys

import pandas as pd
from sqlalchemy import (
    inspect,
    MetaData,
    Column,
    String,
    DateTime,
    Table,
    text,
    Float,
)

# Get the absolute path of the current script
script_path = os.path.abspath(__file__)

# Get the directory of the script (Bronze_tables directory)
script_dir = os.path.dirname(script_path)

# Add the parent directory of the script to the Python module search path
sys.path.insert(0, os.path.dirname(script_dir))


from Utils.database_utils import (
    get_database_engine,
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

# tb_name_cash_security = "bronze_nexen_cash_and_security_transactions"
tb_name_helix_trades = "temp_helix_trades"
df_helix_trades = df
df_helix_trades["timestamp"] = get_current_timestamp()


def create_custom_bronze_table(engine, tb_name, df, include_timestamp=True):
    """
    Creates a new database table based on the columns in the given DataFrame.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine.
        tb_name (str): The name of the table to create.
        df (pd.DataFrame): The DataFrame containing the data.
        include_timestamp (bool, optional): Whether to include a timestamp column. Defaults to True.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If an error occurs while creating the table.
    """
    metadata = MetaData()
    metadata.bind = engine

    columns = []
    for col in df.columns:
        if col in ["Market Cap", "End Money", "Orig. Rate", "Orig. Price", "Par/Quantity", "hairCut", "Spread", "Money", "End Money"]:
            columns.append(Column(col, Float))  # Specify maximum length
        else:
            columns.append(Column(col, String))

    if include_timestamp:
        columns.append(Column("timestamp", DateTime))

    table = Table(tb_name, metadata, *columns, extend_existing=True)

    try:
        metadata.create_all(engine)
        print(f"Table {tb_name} created successfully or already exists.")
    except Exception as e:
        print(f"Failed to create table {tb_name}: {e}")
        raise


def clear_table_content(engine, tb_name):
    """
    Clears the content of the specified table.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine.
        tb_name (str): The name of the table to clear.
    """
    with engine.connect() as connection:
        try:
            connection.execute(text(f"DELETE FROM {tb_name}"))
            logger.info(f"Content of table {tb_name} cleared successfully.")
        except Exception as e:
            logger.error(f"Failed to clear content of table {tb_name}: {e}")
            raise


def get_table_columns(engine, table_name):
    """
    Get the column names from an existing database table.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table

    Returns:
        list: List of column names if table exists, None otherwise
    """
    try:
        inspector = inspect(engine)
        if table_name in inspector.get_table_names():
            return [col["name"] for col in inspector.get_columns(table_name)]
        return None
    except Exception as e:
        logger.error(f"Error getting columns for table {table_name}: {e}")
        raise


def align_dataframe_columns(df, table_columns):
    """
    Align DataFrame columns with table columns.

    Args:
        df: Source DataFrame
        table_columns: List of target table column names

    Returns:
        DataFrame: DataFrame with aligned columns
    """
    # Create a copy to avoid modifying the original DataFrame
    aligned_df = df.copy()

    # Remove extra columns that aren't in the table
    extra_columns = set(aligned_df.columns) - set(table_columns)
    if extra_columns:
        logger.warning(f"Removing extra columns from DataFrame: {extra_columns}")
        aligned_df = aligned_df.drop(columns=extra_columns)

    # Add missing columns with NULL values
    missing_columns = set(table_columns) - set(aligned_df.columns)
    if missing_columns:
        logger.warning(f"Adding missing columns to DataFrame: {missing_columns}")
        for col in missing_columns:
            aligned_df[col] = None

    # Reorder columns to match table schema
    aligned_df = aligned_df.reindex(columns=table_columns)

    return aligned_df


def process_dataframe(engine, tb_name, df):
    """
    Modified process_dataframe function with column validation and alignment.

    Args:
        engine: SQLAlchemy engine
        tb_name: Target table name
        df: Source DataFrame
    """
    if df is None:
        logger.warning(f"DataFrame for table {tb_name} is None. Skipping processing.")
        return

    try:
        # Create table if it doesn't exist
        create_custom_bronze_table(engine, tb_name, df)

        # Get existing table columns
        table_columns = get_table_columns(engine, tb_name)
        if not table_columns:
            logger.error(f"Could not get columns for table {tb_name}")
            return

        # Align DataFrame columns with table schema
        aligned_df = align_dataframe_columns(df, table_columns)

        # Check if table has data and clear it if necessary
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {tb_name}"))
            count = result.scalar()
            if count > 0:
                clear_table_content(engine, tb_name)

        # Insert aligned data
        aligned_df.to_sql(
            tb_name,
            engine,
            if_exists="append",
            index=False,
        )
        logger.info(f"Data inserted into table {tb_name} successfully.")

    except Exception as e:
        logger.error(f"Error processing data for table {tb_name}: {e}")
        raise


process_dataframe(engine, tb_name_helix_trades, df_helix_trades)


logger.info("All data processing completed successfully.")
