# from sqlalchemy import create_engine
#
# def get_database_engine():
#     connection_string = (
#         "mssql+pyodbc://tony.hoang:Ar0undthe$un@LUCIDSQL2/Prod1"
#         "?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
#     )
#     engine = create_engine(connection_string)
#     return engine
#
# # Testing the SQLAlchemy connection
# try:
#     engine = get_database_engine()
#     with engine.connect() as conn:
#         print("SQLAlchemy connection successful")
# except Exception as e:
#     print(f"SQLAlchemy connection failed: {e}")


