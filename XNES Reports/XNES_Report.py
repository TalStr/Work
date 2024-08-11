import pyodbc
import pandas as pd
import openpyxl as pyxl
from openpyxl import styles
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

if __name__ == "__main__":
    # Connect to the database
    conn = pyodbc.connect(CONN_STRING)

    wb = pyxl.open("TEMPLATE.xlsx")
    ws = wb.active
    ws.Name = "SnapShot"

    ws['J2'] = QUERY_DATE.strftime("%Y-%m-%d")
    # Get Exposures Stored Procedure
    EXPOSURES_STORED_PROC = os.getenv("EXPOSURES_STORED_PROC")
    params = (EQUITY_ID, 0.0, QUERY_DATE.strftime("%Y-%m-%d"))
    exposures_df = pd.read_sql(EXPOSURES_STORED_PROC, conn, params=params)

    grouped_exposures_df = exposures_df[['סקטור', 'חשיפה', 'חשיפה נטו']].groupby('סקטור').sum()
    grouped_exposures_df.sort_index(ascending=False, inplace=True)
    ws['I3'] = exposures_df['חשיפה'].sum()
    ws['I4'] = exposures_df['חשיפה נטו'].sum()
    ws['I5'] = exposures_df['חשיפה נטו'].where(exposures_df['חשיפה נטו'] > 0, 0).sum()
    ws['I6'] = exposures_df['חשיפה נטו'].where(exposures_df['חשיפה נטו'] < 0, 0).sum() * -1

    grouped_exposures_df['ofTotal'] = grouped_exposures_df['חשיפה'] / grouped_exposures_df['חשיפה'].sum()

    for i, (index, row) in enumerate(grouped_exposures_df.iterrows()):
        ws[f'J{i+9}'] = index
        ws[f'I{i+9}'] = row['חשיפה נטו']
        ws[f'H{i+9}'] = row['חשיפה']
        ws[f'G{i+9}'] = row['ofTotal']

    for i, (index, row) in enumerate(exposures_df.iterrows()):
        if i == 5:
            break
        ws[f'D{i+3}'] = row['שוק']
        ws[f'C{i+3}'] = row['חשיפה']
        ws[f'B{i+3}'] = row['צד']

    # Get Margin Stored Procedure
    MARGIN_STORED_PROC = os.getenv("MARGIN_STORED_PROC")
    params = (QUERY_DATE.strftime("%Y-%m-%d"), EQUITY_ID)
    curr = conn.cursor()
    curr.execute(MARGIN_STORED_PROC, params)
    rows = curr.fetchall()
    margin_df = pd.DataFrame.from_records(rows, columns=["Date", "VarType", "Margin", "System", "Account", "Sector",
                                                         "Total M2E"])
    curr.close()
    margin_df = margin_df.loc[~margin_df['Sector'].isin(['סה"כ', 'ריביות'])]
    margin_df.sort_values(by='Sector', ascending=False, inplace=True, ignore_index=True)
    ws["I14"] = margin_df['Margin'].sum()
    for i, (index, row) in enumerate(margin_df.iterrows()):
        ws[f'J{i+15}'] = row['Sector']
        ws[f'I{i+15}'] = row['Margin']

    # Get VaR Stored Procedure
    VaR_STORED_PROC = os.getenv("VAR_STORED_PROC")
    params = (QUERY_DATE.strftime("%Y-%m-%d"), EQUITY_ID)
    var_df = pd.read_sql(VaR_STORED_PROC, conn, params=params)
    totalVaR = var_df['TotalVaR'].sum() / 10000
    var_df = var_df.loc[~var_df['Sector'].isin(['סה"כ', 'ריביות'])]
    var_df['VaR'] = var_df['VaR'] * totalVaR
    var_df.sort_values(by='Sector', ascending=False, inplace=True, ignore_index=True)
    ws["F14"] = var_df['VaR'].sum()
    for i, (index, row) in enumerate(var_df.iterrows()):
        ws[f'G{i+15}'] = row['Sector']
        ws[f'F{i+15}'] = row['VaR']

    # Get Position Changes Stored Procedure
    POS_CHANGES_STORED_PROC = os.getenv("POS_CHANGES_STORED_PROC")
    params = ((QUERY_DATE - timedelta(days=7)).strftime("%Y-%m-%d"), QUERY_DATE.strftime("%Y-%m-%d"), EQUITY_ID)
    curr = conn.cursor()
    curr.execute(POS_CHANGES_STORED_PROC, params)
    rows = curr.fetchall()
    pos_changes_df = pd.DataFrame.from_records(rows, columns=["Code", "Sector", "Name", "qty", "dsc", "date", "PnL"])
    curr.close()
    conn.close()
    for i, (index, row) in enumerate(pos_changes_df.iterrows()):
        ws[f'E{i+23}'] = row['Code']
        ws[f'F{i+23}'] = row['Sector']
        ws[f'G{i+23}'] = row['Name']
        ws[f'H{i+23}'] = row['qty']
        ws[f'I{i+23}'] = row['dsc']
        ws[f'J{i+23}'] = row['date']
    # Save the workbook
    wb.save(f"XNES-COMM-WEEKLY_{QUERY_DATE.strftime("%Y%m%d")}.xlsx")
    print("Report generated successfully.")