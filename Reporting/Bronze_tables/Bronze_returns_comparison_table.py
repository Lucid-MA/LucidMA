import pandas as pd
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Integer,
    Float,
    Date,
    inspect,
)

from Utils.Common import get_file_path
from Utils.database_utils import engine_prod

# Database connection setup (update with your connection details)
metadata = MetaData()
engine = engine_prod
# Define the table schema
tb_name = "returns_comparison_table"


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("series_id", String(255), primary_key=True),
        Column("pool_name", String(255)),
        Column("start_date", Date),
        Column("end_date", Date),
        Column("day_count", Integer),
        Column("return_360", Float),
        Column("3m_return", Float),
        Column("6m_return", Float),
        Column("12m_return", Float),
        Column("1m_A1_P1_CP", Float),
        Column("1m_A1_P1_CP_3m_return", Float),
        Column("1m_A1_P1_CP_6m_return", Float),
        Column("1m_A1_P1_CP_12m_return", Float),
        Column("1m_SOFR", Float),
        Column("1m_SOFR_3m_return", Float),
        Column("1m_SOFR_6m_return", Float),
        Column("1m_SOFR_12m_return", Float),
        Column("1m_Tbills", Float),
        Column("1m_Tbills_3m_return", Float),
        Column("1m_Tbills_6m_return", Float),
        Column("1m_Tbills_12m_return", Float),
        Column("1m_CRANE", Float),
        Column("1m_CRANE_3m_return", Float),
        Column("1m_CRANE_6m_return", Float),
        Column("1m_CRANE_12m_return", Float),
        Column("3m_A1_P1_CP", Float),
        Column("3m_A1_P1_CP_6m_return", Float),
        Column("3m_A1_P1_CP_12m_return", Float),
        Column("3m_SOFR", Float),
        Column("3m_SOFR_6m_return", Float),
        Column("3m_SOFR_12m_return", Float),
        Column("3m_Tbills", Float),
        Column("3m_Tbills_6m_return", Float),
        Column("3m_Tbills_12m_return", Float),
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


# Create the table if it does not exist
inspector = inspect(engine)

if not inspector.has_table(tb_name):
    create_table_with_schema(tb_name)

# Load Excel data
FILE_PATH = get_file_path("S:/Users/THoang/Data/benchmark_by_series.xlsx")
df = pd.read_excel(FILE_PATH, sheet_name="Main", engine="openpyxl")

# Ensure proper data formatting
df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
df["end_date"] = pd.to_datetime(df["end_date"]).dt.date
df["day_count"] = df["day_count"].fillna(0).astype(int)

# Fill missing values with 0 for float columns
float_columns = [
    col
    for col in df.columns
    if col not in ["series_id", "pool_name", "start_date", "end_date", "day_count"]
]
df[float_columns] = df[float_columns].fillna(0).astype(float)

# Insert data into the database
with engine.connect() as conn:
    df.to_sql(tb_name, conn, if_exists="replace", index=False)
    print(f"Data successfully inserted into {tb_name}.")
