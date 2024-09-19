import pandas as pd
from sqlalchemy import (
    Table,
    MetaData,
    Column,
    String,
    Float,
    Date,
    DateTime,
    text,
    Integer,
    inspect,
)
from sqlalchemy.exc import SQLAlchemyError

from Utils.Common import get_file_path
from Utils.Hash import hash_string
from Utils.database_utils import get_database_engine, engine_prod, engine_staging

PUBLISH_TO_PROD = True
if PUBLISH_TO_PROD:
    engine = engine_prod

else:
    engine = engine_staging


def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        tb_name,
        metadata,
        Column("start_date", Date, primary_key=True),
        Column("end_date", Date),
        Column("series", String(255), primary_key=True),
        Column("target", String),
        Column("benchmark", String),
        Column("net_return", String),
        Column("net_spread", String),
        Column("change_in_spread", String),
        Column("change_in_rate", String),
        Column("status", String),
        Column("user", String),
        Column("timestamp", DateTime),
        extend_existing=True,
    )
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully or already exists.")


tb_name = "bronze_target_returns"
inspector = inspect(engine)

if not inspector.has_table(tb_name):
    create_table_with_schema(tb_name)
