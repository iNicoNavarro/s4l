import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

def get_sql_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('SQLSERVER_HOST')},{os.getenv('SQLSERVER_PORT')};"
        f"DATABASE={os.getenv('SQLSERVER_DB')};"
        f"UID={os.getenv('SQLSERVER_USER')};"
        f"PWD={os.getenv('SQLSERVER_PASSWORD')}"
    )
    return pyodbc.connect(conn_str)

def write_spark_df_to_sql(df, table_name, mode="overwrite"):
    df.write \
      .format("jdbc") \
      .option("url", f"jdbc:sqlserver://{os.getenv('SQLSERVER_HOST')}:{os.getenv('SQLSERVER_PORT')};databaseName={os.getenv('SQLSERVER_DB')}") \
      .option("dbtable", table_name) \
      .option("user", os.getenv('SQLSERVER_USER')) \
      .option("password", os.getenv('SQLSERVER_PASSWORD')) \
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
      .mode(mode) \
      .save()
