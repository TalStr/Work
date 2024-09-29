from collections import defaultdict
import math
from datetime import datetime
import glob
import os
from typing import Dict, Optional, Tuple
import pandas as pd
import sys

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)


output = []
vol_output = []

os.chdir(sys.argv[1])
for file in glob.glob("*.{}".format("csv")):
    df = pd.read_csv(rf"{file}")
    engine = file.split(".")[0]
    if engine == "stats" or engine == "volumes":
        continue
    current_positions_mkt: Dict[str, Tuple[int, datetime]] = defaultdict(
        lambda: (0, datetime.now())
    )
    positions = []
    overnight_positions = 0
    save = 0
    trades = df["ClOrdID"].nunique()
    vol: Dict[str, int] = defaultdict(int)
    a = df["UTCTime"].str.split(
        " ",
        expand=True,
    )
    df["Date"] = pd.to_datetime(a[0], format="%Y-%m-%d")
    df["UTCTime"] = pd.to_datetime(df["UTCTime"], format="%Y-%m-%d %H:%M:%S.%f")
    # df = df.loc[(df["Date"] >= "2022-01-01") & (df["Date"] <= "2025-01-01")]
    for index, row in df.iterrows():
        mkt = row["Instrument"].split(".")[-1][:-1]
        vol[mkt] += abs(row["Amount"])
        current_position = current_positions_mkt[mkt]
        if current_position[0] == 0 and row["Amount"] != 0:
            current_positions_mkt[mkt] = (row["Amount"], row["UTCTime"])
        elif current_position[0] * (current_position[0] + row["Amount"]) <= 0:
            entry = current_position[1]
            exit = row["UTCTime"]
            positions.append(
                [
                    mkt,
                    current_position[1],
                    row["UTCTime"],
                    (exit - entry).total_seconds() / 60,
                    exit > entry.replace(hour=22, minute=0, second=0, microsecond=0),
                ]
            )
            current_positions_mkt[mkt] = (
                current_position[0] + row["Amount"],
                row["UTCTime"],
            )
            # if(current_position[0]+row["Amount"] != 0):
            #     positions.loc[closed_positions] = [mkt, current_position[1], row["UTCTime"], "Switch"]
            #     closed_positions+=1
            #     current_positions_mkt[mkt] = [row["Amount"], row["UTCTime"]]
            # else:
            #     positions.loc[closed_positions] = [mkt, current_position[1], row["UTCTime"], "Zero"]
            #     closed_positions+=1
            #     current_positions_mkt[mkt][0] += row["Amount"]
        else:
            current_positions_mkt[mkt] = (
                current_position[0] + row["Amount"],
                current_position[1],
            )

    positions_df = pd.DataFrame(
        positions, columns=["Market", "Open", "Close", "Duration", "OverNight"]
    )
    overnight_positions = positions_df["OverNight"].sum()
    average_duration = positions_df["Duration"].mean()
    # print(f"Engine: {engine}")
    # print(f"Trading Days: {a[0].nunique()}")
    # print(f"Positions: {closed_positions}")
    # print(f"Over Night Positions: {overnight_positions}")
    # print(f"Average Position Duration: {average_duration} Minutes")
    # print(f"Trades: {trades}")
    # print(f"Volume: {vol}")
    output.append(
        [
            engine,
            df["Date"].nunique(),
            len(positions_df),
            overnight_positions,
            average_duration,
            trades,
            sum(vol.values()),
        ]
    )
    vol_output.append({"engine": engine, **vol})
output_df = pd.DataFrame(
    output,
    columns=[
        "Engine",
        "Trading Days",
        "Positions",
        "Over Night Positions",
        "Average Position Duration(Minutes)",
        "Trades",
        "Volume",
    ],
)
print(output_df)
vol_output_df = pd.DataFrame(vol_output)
print(vol_output_df)
output_df.to_csv(r"stats.csv", index=False)
vol_output_df.to_csv(r"volumes.csv", index=False)
