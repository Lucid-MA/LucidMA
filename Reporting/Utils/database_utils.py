import logging
import os
import platform
from contextlib import contextmanager

import pandas as pd
from sqlalchemy import MetaData, String, Column, Table, DateTime, select, func, inspect
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

###### CONSTANTS ######
staging_db_type = "postgres"
prod_db_type = "sql_server_2"
helix_db_type = "sql_server_1"

# Configuration
DB_CONFIG = {
    "postgres": {
        "db_endpoint": "luciddb1.czojmxqfrx7k.us-east-1.rds.amazonaws.com",
        "db_port": "5432",
        "db_user": "dbmasteruser",
        "db_password": "lnRz*(N_7aOf~7Hx6oRo8;,<vYp|~#PC",
        "db_name": "reporting",
    },
    "sql_server_1": {
        "driver": "ODBC+Driver+17+for+SQL+Server",
        "server_mac": "172.31.0.10",
        "server_windows": "LUCIDSQL1",
        "database": "HELIXREPO_PROD_02",
        "trusted_connection": "yes",
        "user_mac": "Lucid\\tony.hoang",
        "user_windows": "tony.hoang",
        "password": os.getenv("MY_PASSWORD"),
    },
    "sql_server_2": {
        "driver": "ODBC+Driver+17+for+SQL+Server",
        "server_mac": "172.31.32.100",
        "server_windows": "LUCIDSQL2",
        "database": "Prod1",
        "trusted_connection": "yes",
        "user_mac": "Lucid\\tony.hoang",
        "user_windows": "tony.hoang",
        "password": os.getenv("MY_PASSWORD"),
    },
}


def get_database_engine(db_type):
    if db_type == "postgres":
        database_url = f"postgresql://{DB_CONFIG['postgres']['db_user']}:{DB_CONFIG['postgres']['db_password']}@{DB_CONFIG['postgres']['db_endpoint']}:{DB_CONFIG['postgres']['db_port']}/{DB_CONFIG['postgres']['db_name']}"
        return create_engine(database_url)

    elif db_type.startswith("sql_server"):
        if platform.system() == "Darwin":  # macOS
            conn_str = f"mssql+pymssql://{DB_CONFIG[db_type]['user_mac']}:{DB_CONFIG[db_type]['password']}@{DB_CONFIG[db_type]['server_mac']}/{DB_CONFIG[db_type]['database']}"
            return create_engine(conn_str)

        elif platform.system() == "Windows":
            conn_str = (
                f"mssql+pyodbc://{DB_CONFIG[db_type]['user_windows']}:{DB_CONFIG[db_type]['password']}@"
                f"{DB_CONFIG[db_type]['server_windows']}/{DB_CONFIG[db_type]['database']}?"
                f"driver={DB_CONFIG[db_type]['driver']}&Trusted_Connection={DB_CONFIG[db_type]['trusted_connection']}"
            )
            return create_engine(conn_str)

        else:
            raise Exception("Unsupported platform")


def read_table_from_db(table_name, db_type):
    engine = get_database_engine(db_type)
    if db_type.startswith("sql_server"):
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, con=engine)
    elif db_type == "postgres":
        return pd.read_sql_table(table_name, con=engine)


# TODO: deprecate this - use v2 instead
def execute_sql_query(sql_query, db_type, params=None):
    engine = get_database_engine(db_type)
    if db_type.startswith("sql_server"):
        return pd.read_sql(sql_query, con=engine, params=params)
    elif db_type == "postgres":
        return pd.read_sql(sql_query, con=engine, params=params)


def execute_sql_query_v2(sql_query, db_type, params=None):
    engine = get_database_engine(db_type)

    if platform.system() == "Windows":
        date_placeholder = "?"
    else:  # Assuming Mac or other Unix-based systems
        date_placeholder = "%s"

    sql_query = sql_query.format(date_placeholder=date_placeholder)

    if db_type.startswith("sql_server"):
        return pd.read_sql_query(sql_query, con=engine, params=params)
    elif db_type == "postgres":
        return pd.read_sql_query(sql_query, con=engine, params=params)


@contextmanager
def DatabaseConnection(db_type):
    engine = get_database_engine(db_type)
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()


# Database engines
engine_staging = get_database_engine(staging_db_type)
engine_prod = get_database_engine(prod_db_type)
engine_helix = get_database_engine(helix_db_type)





def upsert_data(
    engine,
    table_name: str,
    df: pd.DataFrame,
    primary_key_name: str,
    publish_to_prod: bool,
):
    with engine.connect() as connection:
        try:
            with connection.begin():
                # Construct the INSERT statement dynamically
                column_names = ", ".join([f'"{col}"' for col in df.columns])
                value_placeholders = ", ".join(
                    [
                        f":{col.replace(' ', '_').replace('/', '_').replace('&','').replace('#','').replace('*','').replace("'", "").replace("?", "").replace(".","").replace("-","")}"
                        for col in df.columns
                    ]
                )

                # Convert 'nan' data to None for MS SQL
                df = df.astype(object).where(pd.notnull(df), None)

                if publish_to_prod:
                    # Using MERGE statement for MS SQL Server
                    update_clause = ", ".join(
                        [
                            f'"{col}" = SOURCE."{col}"'
                            for col in df.columns
                            if col != primary_key_name
                        ]
                    )

                    upsert_sql = text(
                        f"""
                        MERGE INTO {table_name} AS TARGET
                        USING (SELECT {','.join(f'SOURCE."{col}"' for col in df.columns)} FROM (VALUES ({value_placeholders})) AS SOURCE ({column_names})) AS SOURCE
                        ON TARGET."{primary_key_name}" = SOURCE."{primary_key_name}"
                        WHEN MATCHED THEN
                            UPDATE SET {update_clause}
                        WHEN NOT MATCHED THEN
                            INSERT ({column_names}) VALUES ({','.join(f'SOURCE."{col}"' for col in df.columns)});
                        """
                    )
                else:
                    update_clause = ", ".join(
                        [
                            f'"{col}"=EXCLUDED."{col}"'
                            for col in df.columns
                            if col != primary_key_name
                        ]
                    )

                    upsert_sql = text(
                        f"""
                        INSERT INTO {table_name} ({column_names})
                        VALUES ({value_placeholders})
                        ON CONFLICT ("{primary_key_name}")
                        DO UPDATE SET {update_clause};
                        """
                    )

                df.columns = [
                    col.replace(" ", "_")
                    .replace("/", "_")
                    .replace("&", "")
                    .replace("#", "")
                    .replace("*", "")
                    .replace("'", "")
                    .replace("?", "")
                    .replace(".","").replace("-","")
                    for col in df.columns
                ]

                # Execute the upsert statement
                connection.execute(upsert_sql, df.to_dict(orient="records"))
            logger.info(f"Latest data upserted successfully into {table_name}.")
        except SQLAlchemyError as e:
            logger.error(f"An error occurred: {e}")
            raise

    logger.info(f"Data upserted successfully into {table_name}.")


def upsert_data_multiple_keys(
    engine,
    table_name: str,
    df: pd.DataFrame,
    primary_key_names: list,
    publish_to_prod: bool,
):
    with engine.connect() as connection:
        try:
            with connection.begin():
                # Construct the INSERT statement dynamically
                column_names = ", ".join([f'"{col}"' for col in df.columns])
                value_placeholders = ", ".join(
                    [
                        f":{col.replace(' ', '_').replace('/', '_').replace('&', '').replace('#', '').replace('*', '').replace("'", "").replace("?", "").replace(".", "").replace("-", "")}"
                        for col in df.columns
                    ]
                )

                # Convert 'nan' data to None for MS SQL
                df = df.astype(object).where(pd.notnull(df), None)

                if publish_to_prod:
                    # Using MERGE statement for MS SQL Server
                    update_clause = ", ".join(
                        [
                            f'"{col}" = SOURCE."{col}"'
                            for col in df.columns
                            if col not in primary_key_names
                        ]
                    )

                    match_condition = " AND ".join([f'TARGET."{key}" = SOURCE."{key}"' for key in primary_key_names])

                    upsert_sql = text(
                        f"""
                                        MERGE INTO {table_name} AS TARGET
                                        USING (SELECT {','.join(f'SOURCE."{col}"' for col in df.columns)} FROM (VALUES ({value_placeholders})) AS SOURCE ({column_names})) AS SOURCE
                                        ON {match_condition}
                                        WHEN MATCHED THEN
                                            UPDATE SET {update_clause}
                                        WHEN NOT MATCHED THEN
                                            INSERT ({column_names}) VALUES ({','.join(f'SOURCE."{col}"' for col in df.columns)});
                                        """
                    )
                else:
                    update_clause = ", ".join(
                        [
                            f'"{col}"=EXCLUDED."{col}"'
                            for col in df.columns
                            if col not in primary_key_names
                        ]
                    )

                    conflict_targets = ", ".join([f'"{key}"' for key in primary_key_names])

                    upsert_sql = text(
                        f"""
                                        INSERT INTO {table_name} ({column_names})
                                        VALUES ({value_placeholders})
                                        ON CONFLICT ({conflict_targets})
                                        DO UPDATE SET {update_clause};
                                        """
                    )

                df.columns = [
                    col.replace(" ", "_")
                    .replace("/", "_")
                    .replace("&", "")
                    .replace("#", "")
                    .replace("*", "")
                    .replace("'", "")
                    .replace("?", "")
                    .replace(".", "").replace("-", "")
                    for col in df.columns
                ]

                # Execute the upsert statement
                connection.execute(upsert_sql, df.to_dict(orient="records"))
            logger.info(f"Latest data upserted successfully into {table_name}.")
        except SQLAlchemyError as e:
                logger.error(f"An error occurred: {e}")
                raise

    logger.info(f"Data upserted successfully into {table_name}.")


def upsert_data_multiple_keys_v2(
        engine,
        table_name: str,
        df: pd.DataFrame,
        primary_key_names: list,
        publish_to_prod: bool,
):
    # Remove duplicates based on primary key columns
    df = df.drop_duplicates(subset=primary_key_names, keep='last')

    # Normalize column names
    df.columns = [
        col.replace(" ", "_").replace("/", "_").replace("&", "").replace("#", "")
        .replace("*", "").replace("'", "").replace("?", "").replace(".", "").replace("-", "")
        for col in df.columns
    ]

    with engine.connect() as connection:
        try:
            with connection.begin():
                column_names = ", ".join([f'"{col}"' for col in df.columns])
                value_placeholders = ", ".join([f":{col}" for col in df.columns])

                # Convert 'nan' data to None for MS SQL
                df = df.astype(object).where(pd.notnull(df), None)

                if publish_to_prod:
                    update_clause = ", ".join(
                        [f'"{col}" = SOURCE."{col}"' for col in df.columns if col not in primary_key_names]
                    )
                    match_condition = " AND ".join([f'TARGET."{key}" = SOURCE."{key}"' for key in primary_key_names])

                    change_condition = ' OR '.join([
                        f'SOURCE."{col}" <> TARGET."{col}" OR (SOURCE."{col}" IS NULL AND TARGET."{col}" IS NOT NULL) OR (SOURCE."{col}" IS NOT NULL AND TARGET."{col}" IS NULL)'
                        for col in df.columns if col not in primary_key_names
                    ])

                    upsert_sql = text(f"""
                        MERGE INTO {table_name} AS TARGET
                        USING (SELECT {column_names} FROM (VALUES ({value_placeholders})) AS SOURCE ({column_names})) AS SOURCE
                        ON {match_condition}
                        WHEN MATCHED AND ({change_condition}) THEN
                            UPDATE SET {update_clause}
                        WHEN NOT MATCHED BY TARGET THEN
                            INSERT ({column_names}) 
                            VALUES ({','.join(f'SOURCE."{col}"' for col in df.columns)})
                        WHEN NOT MATCHED BY SOURCE AND ({match_condition}) THEN
                            DELETE;
                    """)
                else:
                    update_clause = ", ".join(
                        [f'"{col}"=EXCLUDED."{col}"' for col in df.columns if col not in primary_key_names]
                    )
                    conflict_targets = ", ".join([f'"{key}"' for key in primary_key_names])

                    upsert_sql = text(f"""
                        INSERT INTO {table_name} ({column_names})
                        VALUES ({value_placeholders})
                        ON CONFLICT ({conflict_targets})
                        DO UPDATE SET {update_clause};
                    """)

                # Execute the upsert statement
                connection.execute(upsert_sql, df.to_dict(orient="records"))

            logger.info(f"Latest data upserted successfully into {table_name}.")
        except IntegrityError as ie:
            logger.error(f"IntegrityError occurred: {ie}")
            logger.error("This might be due to unexpected duplicate keys in the target table.")
            raise
        except SQLAlchemyError as e:
            logger.error(f"An error occurred: {e}")
            raise

    logger.info(f"Data upsert operation completed for {table_name}.")

def create_custom_bronze_table(
    engine,
    tb_name,
    primary_column_name,
    string_columns_list,
    include_timestamp=True,
):
    """
    Creates a new database table based on predefined list of columns.
    Also adds an index on the 'TransactionID' column for efficient updates.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine.
        tb_name (str): The name of the table to create.
        primary_column_name (str): The name of the primary key column.
        string_columns_list (list): List of column names to create as string columns.
        include_timestamp (bool, optional): Whether to include a timestamp column. Defaults to True.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If an error occurs while creating the table.
    """
    metadata = MetaData()
    metadata.bind = engine

    main_columns = [Column(primary_column_name, String(255), primary_key=True)]


    string_columns = [Column(col, String) for col in string_columns_list]
    columns = main_columns + string_columns

    if include_timestamp:
        columns.append(Column("timestamp", DateTime))

    table = Table(tb_name, metadata, *columns, extend_existing=True)

    try:
        metadata.create_all(engine)
        print(f"Table {tb_name} created successfully or already exists.")
    except Exception as e:
        print(f"Failed to create table {tb_name}: {e}")
        raise


# Check if the table exists and is empty
def is_table_empty(engine, table_name):
    if inspect(engine).has_table(table_name):
        with engine.connect() as conn:
            result = conn.execute(select(func.count()).select_from(text(table_name)))
            count = result.scalar()
            return count == 0
    return False


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
