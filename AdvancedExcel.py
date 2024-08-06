import win32com.client as win32
import os


if __name__ == "__main__":
    # Create a new Excel application
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    excel.Visible = True
    # Open the Excel file
    wb = excel.Workbooks.Open(r"C:\Users\tal\Documents\Simulation-RESULTS.xlsx")
    # Create the connection string and command text
    connection_string = r"WORKSHEET;C:\Users\tal\Documents\Simulation-RESULTS.xlsx"
    command_text = f"Simulation-RESULTS.xlsx!Table1"

    # Create a connection to the table in the workbook
    connection = wb.Connections.Add2(
        f"WorksheetConnection_Simulation-RESULTS.xlsx!Table1",  # Connection name
        "",  # Description
        connection_string,  # Connection string
        command_text,  # Command text
        7,  # Command type (7 = xlCmdTable)
        True,  # Refresh on open
        False  # Background refresh
    )
    # # Access the model
    # model = wb.Model
    #
    # # Add a measure to the model
    # table_name = "Table1"
    # measure_name = "MaskedEngineMeasure"
    # dax_expression = " IF(ISBLANK(Table1[Engine]), \"Nothing\", Table1[Engine])"
    #
    # test = wb.Model.ModelMeasures.Add(MeasureName=measure_name,AssociatedTable=wb.Model.ModelTables("Table1"),Formula=dax_expression,FormatInformation=model.ModelFormatGeneral,Description="Masked Engine Measure")

    sheet_name = "PivotTableSheet"
    try:
        pivot_ws = wb.Worksheets(sheet_name)
        pivot_ws.Delete()
    except:
        pass

    # Add a new worksheet
    pivot_ws = wb.Worksheets.Add()
    pivot_ws.Name = sheet_name

    # Define the pivot table location
    pivot_table_range = pivot_ws.Cells(1, 1)

    # Create the pivot table cache using the connection4
    pivot_cache = wb.PivotCaches().Create(
        SourceType=win32.constants.xlExternal,
        SourceData=connection,
    )

    # Get the pivot table
    pivot_table = pivot_cache.CreatePivotTable(
        TableDestination=pivot_ws.Cells(1, 1),
        TableName='PivotTable'
    )

    # Define pivot table fields (you need to adjust the field names based on your data)
    # Add col fields
    pivot_table.CubeFields("[Table1].[Engine]").Orientation = win32.constants.xlColumnField
    # Add row fields
    pivot_table.CubeFields("[Table1].[Symbol]").Orientation = win32.constants.xlRowField
    # Add row fields
    pivot_table.CubeFields("[Table1].[Offset]").Orientation = win32.constants.xlRowField
    # Add data fields
    f1 = pivot_table.AddDataField(pivot_table.CubeFields.GetMeasure("[Table1].[Average PnL]", win32.constants.xlAverage), "Average PnL")
    f2 = pivot_table.AddDataField(pivot_table.CubeFields.GetMeasure("[Table1].[Average PnL]", win32.constants.xlSum), "Sum PnL")
    # Get the first worksheet
    ws = wb.Worksheets(1)
    # Get the range of cells containing data
    data_range = ws.UsedRange
    # Get the values from the data range
    data = data_range.Value
    # Print the data
    # for row in data:
    #     print(row)
    # Close the workbook
    # wb.Close()
    # # Quit Excel
    # excel.Quit()