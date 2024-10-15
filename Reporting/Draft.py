import logging

from Utils.Common import print_df
from Utils.database_utils import (
    read_table_from_db,
    get_database_engine,
    prod_db_type,
    staging_db_type,
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

bronze_tb_name = "bronze_daily_bloomberg_collateral_data"
df_bronze_bloomberg_data = read_table_from_db(bronze_tb_name, db_type)
df_bronze_bloomberg_data = df_bronze_bloomberg_data[
    df_bronze_bloomberg_data["bond_id"].isin(["83607HAG0", "92331MAD0"])
]


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


df_bronze_bloomberg_data["rating"] = df_bronze_bloomberg_data.apply(
    lucid_rating, axis=1
)

print_df(df_bronze_bloomberg_data)
