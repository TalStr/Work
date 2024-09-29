#!/usr/bin/python3

from calendar import c
import os
from pdb import run
import subprocess
import multiprocessing
import argparse
from typing import List, Tuple

# python ./gen_forecasts_incr.py -t 60-0 60-20 60-40
# python ./gen_forecasts_incr.py -t BTCUSDT-60-0

# Argument parser setup
parser = argparse.ArgumentParser(description="Process intraday data files.")
parser.add_argument(
    "-t",
    "--timeframes",
    nargs="+",
    required=True,
    help="List of time frames to filter files.",
)
parser.add_argument(
    "-s",
    "--skip_rows",
    required=False,
    default=500,
    help="Number of rows to skip in the input file.",
)
parser.add_argument(
    "-c",
    "--continue_prev",
    action="store_true",
    help="Continue using files existing in Temp directory",
)
parser.add_argument(
    "-m",
    "--max_processes",
    default=8,
    type=int,
    help="Maximum number of processes to run concurrently",
)
args = parser.parse_args()
intraday_dir = "bars/"
incremental_temp_data_dir = os.path.join(intraday_dir, "Temp")

incremental_mats_dir = "incr/"
incremental_mats_temp_dir = os.path.join(incremental_mats_dir, "Temp")


def run_command(command):
    # print(command)
    # result = subprocess.run(command, shell=True, capture_output=True, text=True)
    # return result
    return command


def create_or_clean_directory(full_path, timeframes, clean=True):
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    elif clean:
        for tf in timeframes:
            # subprocess.run(
            #     f"rm -rf {full_path}/*-{tf}.csv",
            #     shell=True,
            #     capture_output=True,
            #     text=True,
            # )
            for file in os.listdir(full_path):
                if file.endswith(f"-{tf}.csv"):
                    os.remove(os.path.join(full_path, file))


models = ["FDS2"]
tfs = args.timeframes
skip_rows: int = args.skip_rows
continue_prev: bool = args.continue_prev
max_processes: int = args.max_processes

create_or_clean_directory(incremental_temp_data_dir, tfs, clean=(not continue_prev))
create_or_clean_directory(incremental_mats_temp_dir, tfs, clean=(not continue_prev))

# with multiprocessing.Pool() as pool:
for intraday_file in os.listdir(intraday_dir):
    continue_from: int = 0
    if not os.path.isfile(os.path.join(intraday_dir, intraday_file)):
        continue
    if os.path.exists(os.path.join(incremental_temp_data_dir, intraday_file)):
        if not continue_prev:
            continue
        # Find row count of existing file
        with open(os.path.join(incremental_temp_data_dir, intraday_file), "r") as f:
            continue_from = sum(1 for _ in f)
    if not any(intraday_file.endswith("-" + tf + ".csv") for tf in tfs):
        continue
    with open(os.path.join(intraday_dir, intraday_file), "r") as input_file:
        inc_filename = os.path.join(incremental_temp_data_dir, intraday_file)
        file_mode = "w" if continue_from == 0 else "a"
        with open(inc_filename, mode=file_mode) as output_file:
            if continue_from != 0:
                output_file.write("\n")
            for line_index, line in enumerate(input_file):
                if line_index < continue_from:
                    continue
                output_file.write(line)
                output_file.flush()
                if line_index < skip_rows:
                    continue
                commands: List[Tuple[int, str]] = []
                for model in models:
                    fds_outfile = "{}x{}.csv".format(
                        os.path.splitext(intraday_file)[0], model
                    )
                    command = "/home/EAGLERD/gils/fdi.git/FDISignalGeneratorCpp1.0.1.20/FDISignalGenerator {} {} {} False; tail -1 {} >> {}".format(
                        inc_filename,
                        os.path.join(incremental_mats_temp_dir, fds_outfile),
                        model,
                        os.path.join(incremental_mats_temp_dir, fds_outfile),
                        os.path.join(incremental_mats_dir, fds_outfile),
                    )
                    commands.append(c ommand)
                # pool.map(run_command, commands)
                for command in commands:
                    run_command(command)
