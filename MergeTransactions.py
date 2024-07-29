import os
import pandas as pd

def find_csv_files(root_dir, pattern="transactions_2*.csv"):
    csv_files = []
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            file = file.lower()
            if file.startswith("transactions_2") and file.endswith(".csv"):
                csv_files.append(os.path.join(subdir, file))
    return csv_files


def merge_csv_files(file_list):
    dataframes = []
    for file in file_list:
        df = pd.read_csv(file, sep="|")
        dataframes.append(df)
    merged_df = pd.concat(dataframes, ignore_index=True)
    return merged_df


def main():
    root_dir = r"\\rdfibi\Kasefet"
    csv_files = find_csv_files(root_dir)
    if csv_files:
        merged_df = merge_csv_files(csv_files)
        merged_df.to_excel("merged_transactions.xlsx", index=False)
        print("Merged CSV saved as 'merged_transactions.xlsx'")
    else:
        print("No CSV files found with the specified pattern.")


if __name__ == "__main__":
    main()
