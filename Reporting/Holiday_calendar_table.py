from sqlalchemy import text, Table, MetaData, Column, Date, String
from sqlalchemy.exc import SQLAlchemyError

from Utils.Constants import holiday_data
from Utils.database_utils import get_database_engine

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine('postgres')
tb_name = "holiday_calendar"


def create_table_with_schema(tb_name, holiday_data):
    metadata = MetaData()
    metadata.bind = engine

    # Drop the table if it exists
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {tb_name}"))

    # Create the table
    table = Table(tb_name, metadata,
                  Column("date", Date, primary_key=True),
                  Column("description", String(255)),
                  extend_existing=True)
    metadata.create_all(engine)
    print(f"Table {tb_name} created successfully.")

    # Insert data from the dictionary
    with engine.connect() as conn:
        try:
            with conn.begin():
                for date, description in holiday_data.items():
                    insert_sql = text(f"INSERT INTO {tb_name} (date, description) VALUES (:date, :description)")
                    conn.execute(insert_sql, {"date": date, "description": description})
            print(f"Data inserted into {tb_name} successfully.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise

create_table_with_schema(tb_name, holiday_data)
