# python pClose_To_Open.py -b[Optional] Base Path -f[Required] Folder
# python pClose_To_Open.py -f DS

import argparse
import os
import glob

import pandas as pd

if __name__ == "__main__":
    # Define Arguments
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Close to open"
    )
    parser.add_argument(
        "-b",
        "--base_path",
        help="base path",
        metavar="base_path",
        required=False,
        default=r"/home/EAGLERD/gils/fdi.git/intradaydata/FDGI-CNX/",
        type=str,
    )
    parser.add_argument(
        "-f", "--folder", help="input folder", metavar="folder", required=True, type=str
    )
    args: argparse.Namespace = parser.parse_args()

    input_path: str = os.path.join(args.base_path, args.folder)
    prev_close_path: str = os.path.join(args.base_path, "PC", args.folder)
    recolumned_path: str = os.path.join(args.base_path, "PC", "Recolumned", args.folder)

    # If Result Folder Does Not Exist, Create It
    if not os.path.exists(prev_close_path):
        os.makedirs(prev_close_path)
    if not os.path.exists(recolumned_path):
        os.makedirs(recolumned_path)

    for file in glob.glob(f"{input_path}/*.csv"):
        base_name: str = os.path.basename(file)
        # Read csv File
        df: pd.DataFrame = pd.read_csv(
            file, header=None, names=["Date", "Close", "Open"]
        )
        first_row: str = ",".join(df.iloc[0].fillna("").to_list())
        if not ("/" in first_row and ":" in first_row):
            df.drop(index=0, inplace=True)
        if first_row != "Date,Close,Open":
            # Write Original File Back With Renamed Columns
            df.to_csv(f"{prev_close_path}/{base_name}", index=False)

        # Create Prev Close Column
        df["pClose"] = df["Close"].shift(1)
        # Remove Old Open Column
        del df["Open"]
        # Remove Rows With Empty Values (First And Last Rows)
        df.dropna(inplace=True)
        # Write New File With pClose Column
        df.to_csv(f"{prev_close_path}/{base_name}", index=False)
