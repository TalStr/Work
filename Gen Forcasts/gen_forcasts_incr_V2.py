#!/usr/bin/python3

from calendar import c
from collections import defaultdict
from hmac import new
import os
from pdb import run
import random
import shutil
import subprocess
import multiprocessing
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, Future
import argparse
import time
import pathlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
intraday_dir2: Path = Path("bars/")
incremental_temp_data_dir = os.path.join(intraday_dir, "Temp")
incremental_data_buffer_dir = os.path.join(intraday_dir, "buffer")
incremental_mats_dir = "incr/"
incremental_mats_temp_dir = os.path.join(incremental_mats_dir, "Temp")


models = ["FDS2", "FDS1"]
tfs = args.timeframes
skip_rows: int = args.skip_rows
continue_prev: bool = args.continue_prev
max_processes: int = args.max_processes


def run_command(command: str, intraday_file: str, model: str) -> List[str]:
    # result = subprocess.run(command, shell=True, capture_output=True, text=True)
    # return result
    return [intraday_file, model]


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


def move_lines_to_temp(intraday_file: str, rows: int = 1) -> bool:
    buffer_path = os.path.join(incremental_data_buffer_dir, intraday_file)
    with open(buffer_path, "r") as buffer_file:
        inc_filename = os.path.join(incremental_temp_data_dir, intraday_file)
        with open(inc_filename, mode="a") as incremental_file:
            for _ in range(rows):
                line = buffer_file.readline()
                if not line:
                    return False
                incremental_file.write(line)
        with open(buffer_path, "r+") as buffer_writer:
            for line in buffer_file:
                buffer_writer.write(line)
            buffer_writer.truncate()
    return True


# def append_line_and_run(
#     intraday_file: str,
# ) -> Optional[List[concurrent.futures.Future]]:
#     new_futures = []
#     if move_lines_to_temp(intraday_file):
#         for model in models:
#             fds_outfile = "{}x{}.csv".format(os.path.splitext(intraday_file)[0], model)
#             command = "/home/eaglerd/gils/fdi.git/fdisignalgeneratorcpp1.0.1.20/fdisignalgenerator {} {} {} false; tail -1 {} >> {}".format(
#                 os.path.join(incremental_temp_data_dir, intraday_file),
#                 os.path.join(incremental_mats_temp_dir, fds_outfile),
#                 model,
#                 os.path.join(incremental_mats_temp_dir, fds_outfile),
#                 os.path.join(incremental_mats_dir, fds_outfile),
#             )
#             future = process_executor.submit(run_command, command, intraday_file, model)
#             new_futures.append(future)
#             future.add_done_callback(future_callback)
#         return new_futures
#     else:
#         return None


# def future_callback(future: Future):
#     result = future.result(10)
#     result_file = result[0]
#     process_manager[result_file].remove(future)
#     if all(future.done() for future in process_manager[result_file]):
#         new_futures = append_line_and_run(result_file)
#         process_manager[result_file] = new_futures


def file_thread_runner(
    process_executor: ProcessPoolExecutor, file_name: str, models: List[str]
):
    base_name = file_name.split(".")[0]
    futures: List[Future] = []
    line_counter: int = 0
    buffer_file = intraday_dir2 / "buffer" / file_name
    temp_file = intraday_dir2 / "Temp" / file_name
    with open(buffer_file, "r") as buffer_reader:
        with open(buffer_file, "r+") as buffer_writer:
            with open(temp_file, "a") as temp_writer:
                while line := buffer_reader.readline():
                    temp_writer.write(line)
                    temp_writer.flush()
                    # Generate Futures
                    for model in models:
                        fds_outfile = "{}x{}.csv".format(
                            os.path.splitext(file_name)[0], model
                        )
                        command = f"/home/eaglerd/gils/fdi.git/fdisignalgeneratorcpp1.0.1.20/fdisignalgenerator {temp_file} {incremental_mats_temp_dir}/{fds_outfile} {model} false; tail -1 {incremental_mats_temp_dir}/{fds_outfile} >> {incremental_mats_dir}/{fds_outfile}"
                        future = process_executor.submit(
                            run_command, command, file_name, model
                        )
                        futures.append(future)
                    buffer_writer.seek(0)
                    for line in buffer_reader:
                        buffer_writer.write(line)
                    buffer_writer.truncate()
                    buffer_reader.seek(0)
                    concurrent.futures.wait(
                        futures, return_when=concurrent.futures.ALL_COMPLETED
                    )
                    if line_counter % 100 == 0:
                        print(f"{base_name} - Processed Line {line_counter}")
                    line_counter += 1
    return file_name


# with multiprocessing.Pool() as pool:
if __name__ == "__main__":
    create_or_clean_directory(incremental_temp_data_dir, tfs, clean=(not continue_prev))
    create_or_clean_directory(incremental_mats_temp_dir, tfs, clean=(not continue_prev))
    create_or_clean_directory(
        incremental_data_buffer_dir, tfs, clean=(not continue_prev)
    )

    with ProcessPoolExecutor(max_workers=max_processes) as process_executor:
        thread_manager: List[Future] = []
        with ThreadPoolExecutor() as thread_executor:
            for intraday_file in os.listdir(intraday_dir2):
                if not os.path.isfile(os.path.join(intraday_dir, intraday_file)):
                    continue
                if not os.path.exists(
                    os.path.join(incremental_temp_data_dir, intraday_file)
                ):
                    open(
                        os.path.join(incremental_temp_data_dir, intraday_file), "w"
                    ).close()
                if not os.path.exists(
                    os.path.join(incremental_data_buffer_dir, intraday_file)
                ):
                    shutil.copy(
                        os.path.join(intraday_dir, intraday_file),
                        os.path.join(incremental_data_buffer_dir, intraday_file),
                    )
                    move_lines_to_temp(intraday_file, rows=skip_rows)
                if not any(intraday_file.endswith("-" + tf + ".csv") for tf in tfs):
                    continue
                future = thread_executor.submit(
                    file_thread_runner, process_executor, intraday_file, models
                )
                thread_manager.append(future)
                # process_manager[intraday_file].extend(
                #     append_line_and_run(intraday_file)
                # )
        concurrent.futures.wait(
            thread_manager, return_when=concurrent.futures.ALL_COMPLETED
        )
        # all_done = False
        # running_now = []
        # while not all_done:
        #     all_done = True
        #     temp = []
        #     for intraday_file, futures in process_manager.items():
        #         if futures is not None:
        #             all_done = False
        #         for future in futures:
        #             if not future.done():
        #                 temp.append(intraday_file)
        #     if temp != running_now:
        #         running_now = temp
        #         print(f"Running {set(temp)}")
        # while any([future.running() for future in [futures for futures in process_manager.items()]]):
    #     pass
    #     if os.path.exists(os.path.join(incremental_temp_data_dir, intraday_file)):
    #         if not continue_prev:
    #             continue
    #         # find row count of existing file
    #         with open(os.path.join(incremental_temp_data_dir, intraday_file), "r") as f:
    #             continue_from = sum(1 for _ in f)
    #     if not any(intraday_file.endswith("-" + tf + ".csv") for tf in tfs):
    #         continue
    #     with open(os.path.join(intraday_dir, intraday_file), "r") as input_file:
    #         inc_filename = os.path.join(incremental_temp_data_dir, intraday_file)
    #         file_mode = "w" if continue_from == 0 else "a"
    #         with open(inc_filename, mode=file_mode) as output_file:
    #             if continue_from != 0:
    #                 output_file.write("\n")
    #             for line_index, line in enumerate(input_file):
    #                 if line_index < continue_from:
    #                     continue
    #                 output_file.write(line)
    #                 output_file.flush()
    #                 if line_index < skip_rows:
    #                     continue
    #                 commands: list[tuple[int, str]] = []
    #                 for model in models:
    #                     fds_outfile = "{}x{}.csv".format(
    #                         os.path.splitext(intraday_file)[0], model
    #                     )
    #                     command = "/home/eaglerd/gils/fdi.git/fdisignalgeneratorcpp1.0.1.20/fdisignalgenerator {} {} {} false; tail -1 {} >> {}".format(
    #                         inc_filename,
    #                         os.path.join(incremental_mats_temp_dir, fds_outfile),
    #                         model,
    #                         os.path.join(incremental_mats_temp_dir, fds_outfile),
    #                         os.path.join(incremental_mats_dir, fds_outfile),
    #                     )
    #                     commands.append(c ommand)
    #                 # pool.map(run_command, commands)
    #                 for command in commands:
    #                     run_command(command)
