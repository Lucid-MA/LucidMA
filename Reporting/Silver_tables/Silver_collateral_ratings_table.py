import os
import sys

# Get the absolute path of the current script
script_path = os.path.abspath(__file__)

# Get the directory of the script (Bronze_tables directory)
script_dir = os.path.dirname(script_path)

# Get the Reporting directory (parent of Bronze_tables)
reporting_dir = os.path.dirname(script_dir)

# Add the Reporting directory to sys.path so Python can find 'Utils' and 'Price'
sys.path.insert(0, reporting_dir)

import logging

from sqlalchemy import MetaData, Table, String, Column, Date, DateTime, inspect

from Bronze_tables.Price.bloomberg_utils import reverse_diff_cusip_map
from Utils.Common import (
    get_repo_root,
    get_current_timestamp,
    read_processed_files,
    mark_file_processed,
)
from Utils.Hash import hash_string_v2
from Utils.database_utils import (
    read_table_from_db,
    get_database_engine,
    prod_db_type,
    staging_db_type,
    upsert_data_multiple_keys,
)


# Get the repository root directory
repo_path = get_repo_root()
silver_tracker_dir = repo_path / "Reporting" / "Silver_tables" / "File_trackers"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PUBLISH_TO_PROD = True
tb_name = "silver_collateral_rating"

if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
    db_type = prod_db_type
    Collateral_rating_TRACKER = silver_tracker_dir / "Silver Collateral Rating PROD"
else:
    engine = get_database_engine("postgres")
    db_type = staging_db_type
    Collateral_rating_TRACKER = silver_tracker_dir / "Silver Collateral Rating"

bronze_bloomberg_collateral_table_name = "bronze_daily_bloomberg_collateral_data"
df_bronze_bloomberg_collateral_data = read_table_from_db(
    bronze_bloomberg_collateral_table_name, db_type
)


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("data_id", String(255), primary_key=True),
        Column("date", Date),
        Column("bond_id", String),
        Column("name", String),
        Column("rtg_sp", String),
        Column("rtg_moody", String),
        Column("rtg_fitch", String),
        Column("rtg_kbra", String),
        Column("rtg_dbrs", String),
        Column("rtg_egan_jones", String),
        Column("rating", String),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    try:
        metadata.create_all(engine)
        print(f"Table {tb_name} created successfully or already exists.")
    except Exception as e:
        print(f"Failed to create table {tb_name}: {e}")
        raise


def lucid_rating(row):
    # Extract values from the DataFrame row
    sp_rating = row["rtg_sp"]
    moodys_rating = row["rtg_moody"]
    fitch_rating = row["rtg_fitch"]
    kroll_rating = row["rtg_kbra"]
    dbrs_rating = row["rtg_dbrs"]
    ej_rating = row["rtg_egan_jones"]
    issuer = row["issuer"]
    sec_type = row["security_type"]
    industry_sector = row["industry_sector"]
    bond_id = row["bond_id"]

    ratings = ["AAA", "AA", "A", "BBB", "BB", "NR"]
    ratings_index = [6] * 6

    # Remove provisional ratings
    def remove_provisional(rating):
        if rating is None:
            return None
        return rating[3:] if rating.startswith("(P)") else rating

    sp_rating = remove_provisional(sp_rating)
    moodys_rating = remove_provisional(moodys_rating)
    fitch_rating = remove_provisional(fitch_rating)
    kroll_rating = remove_provisional(kroll_rating)
    dbrs_rating = remove_provisional(dbrs_rating)
    ej_rating = remove_provisional(ej_rating)

    # Additional checks
    if industry_sector == "Government":
        return "USG"
    if bond_id == "13080TAU6":
        return "A"

    # Check for USG Risk
    if sec_type and (
        sec_type.startswith("Agncy CMO") or sec_type.startswith("Agncy CMBS")
    ):
        return "USGCMO"

    if (
        sec_type and (sec_type.startswith("US GOVERNM") or sec_type.startswith("SBA"))
    ) or (
        issuer
        and (
            issuer.startswith("Fannie Mae")
            or issuer.startswith("Freddie Mac")
            or issuer.startswith("Government National Mortgage")
        )
    ):
        if not (
            issuer
            and (
                issuer.endswith("STACR")
                or issuer.endswith("CAS")
                or issuer.endswith("CRT")
            )
        ):
            return "USG"

    def get_rating_index(rating, agency):
        if rating is None:
            return 6
        rating = rating.upper()
        if rating == "WR" or rating == "NR":
            return 6
        elif rating.startswith("AAA"):
            return 1
        elif rating.startswith("AA"):
            return 2
        elif rating.startswith("A"):
            return 3
        elif rating.startswith("BBB") or (
            agency == "Moodys" and rating.startswith("BAA")
        ):
            return 4
        elif rating.startswith("BB") or (
            agency == "Moodys" and rating.startswith("BA")
        ):
            return 5
        else:
            if agency != "Moodys" and rating[-1] in "123":
                return f"Error: it appears that a Moodys Rating was put where and {agency} Rating should go"
            return 6

    ratings_index[0] = get_rating_index(sp_rating, "S&P")
    ratings_index[1] = get_rating_index(moodys_rating, "Moodys")
    ratings_index[2] = get_rating_index(fitch_rating, "Fitch")
    ratings_index[3] = get_rating_index(kroll_rating, "Kroll")
    ratings_index[4] = get_rating_index(dbrs_rating, "DBRS")
    ratings_index[5] = get_rating_index(ej_rating, "Egan Jones")

    # Check for any errors
    for index in ratings_index:
        if isinstance(index, str):
            return index

    lucid_index = min(ratings_index)
    return ratings[lucid_index - 1]


# Read the processed dates from the tracker file
processed_dates = read_processed_files(Collateral_rating_TRACKER)

# Filter out the already processed dates
df_silver_bloomberg_data = df_bronze_bloomberg_collateral_data[
    ~df_bronze_bloomberg_collateral_data["date"].astype(str).isin(processed_dates)
]

if not df_silver_bloomberg_data.empty:
    # Only select the latest factor and interest_accrued per day for each bond_id
    df_silver_bloomberg_data = df_silver_bloomberg_data.sort_values(
        "timestamp", ascending=False
    )

    df_silver_bloomberg_data["rating"] = df_silver_bloomberg_data.apply(
        lucid_rating, axis=1
    )

    df_silver_bloomberg_data = (
        df_silver_bloomberg_data.groupby(["date", "bond_id"]).first().reset_index()
    )
    df_silver_bloomberg_data = df_silver_bloomberg_data[
        [
            "date",
            "bond_id",
            "name",
            "rtg_sp",
            "rtg_moody",
            "rtg_fitch",
            "rtg_kbra",
            "rtg_dbrs",
            "rtg_egan_jones",
            "rating",
        ]
    ]

    df_silver_bloomberg_data["bond_id"] = df_silver_bloomberg_data["bond_id"].map(
        lambda x: reverse_diff_cusip_map.get(x, x)
    )

    df_silver_bloomberg_data["data_id"] = df_silver_bloomberg_data.apply(
        lambda row: hash_string_v2(f"{row['date']}{row['bond_id']}"),
        axis=1,
    )

    # Reorder the columns to place 'data_id' first
    df_silver_bloomberg_data = df_silver_bloomberg_data[
        ["data_id"]
        + [col for col in df_silver_bloomberg_data.columns if col != "data_id"]
    ]

    df_silver_bloomberg_data["timestamp"] = get_current_timestamp()

    inspector = inspect(engine)

    if not inspector.has_table(tb_name):
        create_table_with_schema(tb_name)

    upsert_data_multiple_keys(
        engine, tb_name, df_silver_bloomberg_data, ["date", "bond_id"], PUBLISH_TO_PROD
    )
    # Mark the processed dates in the tracker file
    for date in df_silver_bloomberg_data["date"].unique():
        mark_file_processed(str(date), Collateral_rating_TRACKER)

else:
    logger.info("Nothing to update - data has already been processed for latest day")
