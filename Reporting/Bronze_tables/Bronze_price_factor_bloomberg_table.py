from sqlalchemy import text, Table, MetaData, Column, String
from sqlalchemy.exc import SQLAlchemyError
from Utils.database_utils import get_database_engine

# Assuming get_database_engine is already defined and returns a SQLAlchemy engine
engine = get_database_engine('postgres')
tb_name = "bronze_bond_price_factor_bloomberg"

def create_table_with_schema(tb_name):
    metadata = MetaData()
    metadata.bind = engine
    table = Table(tb_name, metadata,
                  Column("bond_data_id", String, primary_key=True),
                  Column("bond_id", String),
                  Column("factor", String),
                  Column("source", String),
                  extend_existing=True)
    metadata.create_all(engine)

    # Create an index on the bond_data_id column
    with engine.connect() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{tb_name}_bond_data_id ON {tb_name} (bond_data_id)"))

    print(f"Table {tb_name} created successfully or already exists.")


def insert_new_rows():

    create_table_with_schema(tb_name)
    with engine.connect() as conn:
        try:
            with conn.begin():  # Start a transaction
                insert_sql = text(
                    f"""
                    INSERT INTO {tb_name} (bond_data_id, bond_id, factor, source)
                    SELECT bond_data_id, bond_id, factor, source
                    FROM public.bronze_bond_data_bloomberg
                    WHERE is_am = 0
                    ON CONFLICT (bond_data_id) DO NOTHING;
                    """
                )

                # Execute the INSERT statement
                result = conn.execute(insert_sql)
                print(f"{result.rowcount} new rows inserted into {tb_name}.")
        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")
            raise

# Insert new rows into the PM table
insert_new_rows()

print("Process completed.")