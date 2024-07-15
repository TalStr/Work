import requests
import os
import shutil
import datetime
import pandas as pd

ETF_Download_Links = {
    "XBI": {
        "Name": "SPDR S&P Biotech ETF",
        "Link": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xbi.xlsx"
    },
    "XLB": {
        "Name": "Materials Select Sector SPDR Fund (The)",
        "Link": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlb.xlsx"
    },
    "XLC": {
        "Name": "Communication Services Select Sector SPDR Fund",
        "Link": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlc.xlsx"
    },
    "XLE": {
        "Name": "The Select Sector SPDR Trust - The Energy Select S",
        "Link": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xle.xlsx"
    },
    "XLF": {
        "Name": "The Select Sector SPDR Trust - The Financial Selec",
        "Link": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlf.xlsx"
    },
    "XLI": {
        "Name": "Industrial Select Sector SPDR Fund (The)",
        "Link": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xli.xlsx"
    },
    "XLK": {
        "Name": "Technology Select Sector SPDR Fund (The)",
        "Link": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlk.xlsx"
    },
    "XLP": {
        "Name": "Consumer Staples Select Sector SPDR Fund (The)",
        "Link": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlp.xlsx"
    },
    "XLRE": {
        "Name": "Real Estate Select Sector SPDR Fund (The)",
        "Link": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlre.xlsx"
    },
    "XLU": {
        "Name": "Utilities Select Sector SPDR Fund (The)",
        "Link": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlu.xlsx"
    },
    "XLV": {
        "Name": "Health Care Select Sector SPDR Fund (The)",
        "Link": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlv.xlsx"
    },
    "XLY": {
        "Name": "Consumer Discretionary Select Sector SPDR Fund (Th)",
        "Link": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xly.xlsx"
    }
}


def download_file(download_url: str, download_directory: str, download_name: str) -> str:
    """
    Downloads a file from a given URL and saves it to a specified directory with a specified file name.

    Parameters:
    - download_url (str): The URL from which to download the file.
    - download_directory (str): The directory path where the file will be saved.
    - download_name (str): The name of the file to be saved, including the extension.

    Returns:
    - str: The full path to the downloaded file if successful, an empty string otherwise.
    """
    # Construct the full path where the file will be saved.
    full_download_path = os.path.join(download_directory, f"{download_name}.xlsx")
    # Make a GET request to download the file, streaming the content.
    response = requests.get(download_url, stream=True)
    # Raise an exception for bad responses.
    response.raise_for_status()
    # Write the content to the file in chunks.
    with open(full_download_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=128):
            file.write(chunk)

    # Verify the file exists to confirm successful download.
    if os.path.exists(full_download_path):
        return full_download_path
    else:
        return ""


def get_excel_metadata(file_path: str) -> dict:
    """
    Extracts metadata from an Excel file.

    Parameters:
    - file_path (str): The path to the Excel file.

    Returns:
    - dict: A dictionary containing metadata about the Excel file, including the ticker symbol and date.
    """
    # Read the first three rows of the Excel file to extract metadata.
    workbook_metadata_df = pd.read_excel(file_path, nrows=3, header=None, names=["key", "value"])
    # Extract values from the dataframe.
    metadata_values = workbook_metadata_df['value']
    # Construct and return the metadata dictionary.
    file_metadata = {
        'Ticker': metadata_values[1],
        'Date': pd.to_datetime(metadata_values[2], format='As of %d-%b-%Y').strftime('%Y-%m-%d')
    }
    return file_metadata


def generate_dataframe(file_path: str, file_metadata: dict) -> pd.DataFrame:
    """
    Generates a DataFrame from an Excel file.

    Parameters:
    - file_path (str): The path to the Excel file.
    - file_metadata (dict): Metadata about the Excel file, used to enrich the DataFrame.

    Returns:
    - pd.DataFrame: A DataFrame containing the data from the Excel file, enriched with metadata.
    """
    # Read the Excel file into a DataFrame, skipping the first four rows.
    df = pd.read_excel(file_path, skiprows=4)
    # Find the first entirely empty row, indicating the end of the data.
    last_index = df[df.isnull().all(axis=1)].index[0]
    # Trim the DataFrame to only include rows with data.
    df = df.iloc[:last_index]
    # Insert metadata columns at the beginning of the DataFrame.
    df.insert(0, "Ticker Symbol", file_metadata['Ticker'])
    df.insert(0, "Date", file_metadata['Date'])
    return df


if __name__ == "__main__":
    # Setup directories for downloading and storing results.
    download_dir = 'ETF_ExcelToCSV.TempDownloads'
    result_dir = 'ETF_ExcelToCSV.Results'
    # Create the download directory, failing if it already exists.
    os.makedirs(download_dir, exist_ok=True)
    # Get the current date for naming purposes.
    run_date = datetime.datetime.now().strftime("%Y-%m-%d")
    # Initialize a list to hold DataFrames for each ETF.
    dataframe_list = []
    # Iterate over the ETFs, download, and process each file.
    for code, data in ETF_Download_Links.items():
        download_path = download_file(data["Link"], download_dir, f"{code}_{run_date}")
        if download_path == "":
            print(f"Failed to download file for {code}")
            continue
        metadata = get_excel_metadata(download_path)
        parsed_df = generate_dataframe(download_path, metadata)
        dataframe_list.append(parsed_df)
        print(f"Successfully parsed file for {code}")
    # Clean up the download directory.
    shutil.rmtree(download_dir)
    # Concatenate all DataFrames into a single DataFrame.
    result_df = pd.concat(dataframe_list, ignore_index=True)
    # Ensure the result directory exists.
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    # Save the concatenated DataFrame to a CSV file.
    result_df.to_csv(os.path.join(result_dir, f'ETF Holdings {run_date}.csv'), index=False)
