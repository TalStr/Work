import pyodbc
import pandas as pd
import openpyxl as pyxl
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv, dotenv_values
load_dotenv()

RUN_DATE = datetime.today()
# Get the last Friday
QUERY_DATE = RUN_DATE - timedelta(days=(1+(RUN_DATE.weekday() + 2) % 7))

# Get the connection string from the environment variables
CONN_STRING = os.getenv("CONN_STRING")
EQUITY_ID = os.getenv("EQUITY_ID")

# Connect to the database
conn = pyodbc.connect(CONN_STRING)

# Get Exposures Stored Procedure
EXPOSURES_STORED_PROC = os.getenv("EXPOSURES_STORED_PROC")
params = (EQUITY_ID, 0.0, QUERY_DATE.strftime("%Y-%m-%d"))
exposures_df = pd.read_sql(EXPOSURES_STORED_PROC, conn, params=params)

# Get VaR Stored Procedure
VaR_STORED_PROC = os.getenv("VAR_STORED_PROC")
params = (QUERY_DATE.strftime("%Y-%m-%d"), EQUITY_ID)
var_df = pd.read_sql(VaR_STORED_PROC, conn, params=params)

# Get Margin Stored Procedure
MARGIN_STORED_PROC = os.getenv("MARGIN_STORED_PROC")
params = (QUERY_DATE.strftime("%Y-%m-%d"), EQUITY_ID)
margin_df = pd.read_sql(MARGIN_STORED_PROC, conn, params=params)

# Get Position Changes Stored Procedure
POS_CHANGES_STORED_PROC = os.getenv("POS_CHANGES_STORED_PROC")
params = ((QUERY_DATE - timedelta(days=7)).strftime("%Y-%m-%d"), QUERY_DATE.strftime("%Y-%m-%d"), EQUITY_ID)
pos_changes_df = pd.read_sql(POS_CHANGES_STORED_PROC, conn, params=params)

conn.close()
# for each dataframe print it and save it to csv
print(exposures_df)
exposures_df.to_csv("exposures.csv", index=False)
print(var_df)
var_df.to_csv("var.csv", index=False)
print(margin_df)
margin_df.to_csv("margin.csv", index=False)
print(pos_changes_df)
pos_changes_df.to_csv("pos_changes.csv", index=False)