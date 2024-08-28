import logging

import pandas as pd
from sqlalchemy import MetaData, Table, String, Column, Date, Float, DateTime, inspect

from Utils.Common import print_df, get_current_timestamp
from Utils.database_utils import (
    read_table_from_db,
    get_database_engine,
    prod_db_type,
    staging_db_type,
    upsert_data,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

PUBLISH_TO_PROD = True
tb_name = "silver_bloomberg_factor_rating_interest_accrued"

if PUBLISH_TO_PROD:
    engine = get_database_engine("sql_server_2")
    db_type = prod_db_type
else:
    engine = get_database_engine("postgres")
    db_type = staging_db_type

bronze_tb_name = "bronze_daily_bloomberg_data"
df_bronze_bloomberg_data = read_table_from_db(bronze_tb_name, db_type)


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
        Column("factor", Float),
        Column("interest_accrued", Float),
        Column("rating", String),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


def lucid_rating(row):
    # Extract values from the DataFrame row
    sec_type = row["security_type"]
    sp_rating = row["rtg_sp"]
    moody_rating = row["rtg_moody"]
    fitch_rating = row["rtg_fitch"]
    kroll_rating = row["rtg_kbra"]
    dbrs_rating = row["rtg_dbrs"]
    ej_rating = row["rtg_egan_jones"]
    issuer = row["issuer"]

    # Helper function to remove provisional rating prefix or set to "NR" if None
    def remove_provisional(rating):
        if rating is None:
            return "NR"
        return rating[3:] if rating.startswith("(P)") else rating

    # Clean provisional ratings or set them to "NR" if None
    sp_rating = remove_provisional(sp_rating)
    moody_rating = remove_provisional(moody_rating)
    fitch_rating = remove_provisional(fitch_rating)
    kroll_rating = remove_provisional(kroll_rating)
    dbrs_rating = remove_provisional(dbrs_rating)
    ej_rating = remove_provisional(ej_rating)

    # Check for USG Risk only if sec_type is not None
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

    # Define rating categories and initialize ratings index
    ratings = ["AAA", "AA", "A", "BBB", "BB", "NR"]
    ratings_index = [6] * 6  # Default index corresponding to "NR"

    # Function to determine rating index based on prefix
    def get_rating_index(rating, agency):
        rating = rating.upper() if rating else "NR"
        if rating == "WR" or rating == "NR":
            return 6
        elif rating.startswith("AAA"):
            return 1
        elif rating in ["AA2U", "AA2"]:
            return 2
        elif rating.startswith("AA"):
            return 2
        elif rating == "AH" or rating.startswith("A"):
            return 3
        elif rating == "BAA1" or rating.startswith("BBB"):
            return 4
        elif rating.startswith("BA"):
            return 5
        elif rating.startswith("BB"):
            return 5
        else:
            if agency in ["SP", "Fitch", "Kroll", "DBRS", "EJ"] and rating[-1] in "123":
                return f"Error: it appears that a Moodys Rating was put where and {agency} Rating should go"
            return 6

    # Get index values for each rating
    ratings_index[0] = get_rating_index(sp_rating, "SP")
    ratings_index[1] = get_rating_index(moody_rating, "Moodys")
    ratings_index[2] = get_rating_index(fitch_rating, "Fitch")
    ratings_index[3] = get_rating_index(kroll_rating, "Kroll")
    ratings_index[4] = get_rating_index(dbrs_rating, "DBRS")
    ratings_index[5] = get_rating_index(ej_rating, "EJ")

    # Check if there was any error in the rating indexes
    if any(isinstance(index, str) for index in ratings_index):
        return next(index for index in ratings_index if isinstance(index, str))

    # Determine the minimum rating index
    lucid_index = min(ratings_index)

    # Return the corresponding Lucid Rating
    return ratings[lucid_index - 1]


def get_factor(row):
    if pd.notnull(row["mtg_factor"]):
        return float(row["mtg_factor"])
    elif pd.notnull(row["principal_factor"]):
        return float(row["principal_factor"])
    else:
        logging.warning(f"No factor found for row: {row}")
        return 1.0


df_bronze_bloomberg_data["rating"] = df_bronze_bloomberg_data.apply(
    lucid_rating, axis=1
)

df_bronze_bloomberg_data["factor"] = df_bronze_bloomberg_data.apply(get_factor, axis=1)

df_bronze_bloomberg_data["interest_accrued"] = df_bronze_bloomberg_data[
    "interest_accrued"
].apply(lambda x: float(x) if pd.notnull(x) else 0.0)

df_silver_bloomberg_data = df_bronze_bloomberg_data[
    ["data_id", "date", "bond_id", "name", "factor", "interest_accrued", "rating"]
]

df_silver_bloomberg_data["timestamp"] = get_current_timestamp()


inspector = inspect(engine)

if not inspector.has_table(tb_name):
    create_table_with_schema(tb_name)

upsert_data(engine, tb_name, df_silver_bloomberg_data, "data_id", PUBLISH_TO_PROD)
