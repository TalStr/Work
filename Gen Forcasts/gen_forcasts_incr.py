#!/usr/bin/env python3

import argparse
import subprocess
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor, wait
from pathlib import Path
from threading import Lock
from typing import List
from tqdm import tqdm

# Argument parser setup
parser = argparse.ArgumentParser(description="Process intraday data files.")
parser.add_argument(
    "-t",
    "--time_frames",
    nargs="+",
    required=True,
    help="List of time frames to filter files.",
)
parser.add_argument(
    "-s",
    "--skip_rows",
    type=int,
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
    type=int,
    default=8,
    help="Maximum number of processes to run concurrently",
)
args = parser.parse_args()

# Define directories
INTRADAY_DIR = Path(r"/home/EAGLERD/gils/crypt/Bars")
INCREMENTAL_TEMP_DATA_DIR = INTRADAY_DIR / "Temp"
INCREMENTAL_MATS_DIR = Path(r"/home/EAGLERD/gils/crypt/Forecasts/Incr")
INCREMENTAL_MATS_TEMP_DIR = INCREMENTAL_MATS_DIR / "Temp"

# Parameters
MODELS = ["FDS2", "FDS1"]
TIME_FRAMES = args.time_frames
SKIP_ROWS: int = args.skip_rows
CONTINUE_PREV: bool = args.continue_prev
MAX_PROCESSES: int = args.max_processes

lock = Lock()


def increment_progress(thread_index: int, n: int) -> None:
    with lock:
        p_bars[thread_index].update(n)


def progress_complete(thread_index: int) -> None:
    with lock:
        p_bars[thread_index].bar_format = "{l_bar}{bar}| Completed In {elapsed}"


def run_command(command: str) -> subprocess.CompletedProcess[str]:
    """Run a shell command."""
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    return result


def create_or_clean_directory(
    directory: Path, time_frames: List[str], clean: bool = True
) -> None:
    """Create the directory if it doesn't exist, or clean it if it does."""
    if not directory.exists():
        directory.mkdir(parents=True)
    elif clean:
        for tf in time_frames:
            for file in directory.glob(f"*-{tf}.csv"):
                file.unlink()


def init_temp_file(intraday_file: str, rows: int = 1) -> bool:
    """Move the specified number of lines from the buffer file to the temp file."""
    source_path = INTRADAY_DIR / intraday_file
    temp_file = INCREMENTAL_TEMP_DATA_DIR / intraday_file

    with source_path.open("r") as source_reader, temp_file.open("w") as temp_writer:
        for _ in range(rows):
            line = source_reader.readline()
            if not line:
                return False
            temp_writer.write(line)

    return True


def process_intraday_file(
    thread_index: int,
    file_name: str,
    start_index: int,
    models: List[str],
    process_executor: ProcessPoolExecutor,
) -> int:
    """
    Process a single intraday file line by line, updating the temp file,
    and for each line, run the external fdisignalgenerator command for each model.
    """
    base_name = Path(file_name).stem
    source_file = INTRADAY_DIR / file_name
    temp_file = INCREMENTAL_TEMP_DATA_DIR / file_name
    futures: List[Future] = []
    buffer = 0
    with source_file.open("r") as source_reader, temp_file.open("a") as temp_writer:
        for index, line in enumerate(source_reader):
            if index < start_index:
                continue
            # Append the line to the temp file
            temp_writer.write(line)
            temp_writer.flush()

            # For each model, construct and run the command
            for model in models:
                fds_outfile = f"{base_name}x{model}.csv"
                temp_output_file = INCREMENTAL_MATS_TEMP_DIR / fds_outfile
                final_output_file = INCREMENTAL_MATS_DIR / fds_outfile

                command = "/home/EAGLERD/gils/fdi.git/FDISignalGeneratorCpp1.0.1.20/FDISignalGenerator {} {} {} False; tail -1 {} >> {}".format(
                    INCREMENTAL_TEMP_DATA_DIR / file_name,
                    temp_output_file,
                    model,
                    temp_output_file,
                    final_output_file,
                )
                future = process_executor.submit(run_command, command)
                futures.append(future)

            # Wait for all submitted commands to complete
            wait(futures, return_when="ALL_COMPLETED")
            if buffer != 0 and buffer % 25 == 0:
                increment_progress(thread_index, buffer)
                buffer = 1
            else:
                buffer += 1
            futures.clear()
    increment_progress(thread_index, buffer)
    progress_complete(thread_index)
    return thread_index


if __name__ == "__main__":
    # Create or clean directories
    create_or_clean_directory(
        INCREMENTAL_TEMP_DATA_DIR, TIME_FRAMES, clean=not CONTINUE_PREV
    )
    create_or_clean_directory(
        INCREMENTAL_MATS_TEMP_DIR, TIME_FRAMES, clean=not CONTINUE_PREV
    )

    with ProcessPoolExecutor(max_workers=MAX_PROCESSES) as process_executor:
        with ThreadPoolExecutor() as thread_executor:
            futures: List[Future] = []
            p_bars: List[tqdm] = []
            for intraday_file in INTRADAY_DIR.iterdir():
                if not intraday_file.is_file():
                    continue
                if not any(
                    intraday_file.name.endswith(f"-{tf}.csv") for tf in TIME_FRAMES
                ):
                    continue
                # Ensure temp files exist
                (INCREMENTAL_TEMP_DATA_DIR / intraday_file.name).touch(exist_ok=True)
                if not (
                    CONTINUE_PREV
                    and (INCREMENTAL_TEMP_DATA_DIR / intraday_file.name).exists()
                ):
                    init_temp_file(intraday_file.name, rows=SKIP_ROWS)
                    temp_rows = SKIP_ROWS
                else:
                    temp_rows = sum(
                        1
                        for _ in (INCREMENTAL_TEMP_DATA_DIR / intraday_file.name).open(
                            "r"
                        )
                    )
                source_rows = sum(
                    1 for _ in (INTRADAY_DIR / intraday_file.name).open("r")
                )
                thread_index = len(futures)
                p_bars.append(
                    tqdm(
                        total=source_rows,
                        initial=temp_rows,
                        desc=intraday_file.stem.ljust(26),
                        position=thread_index,
                        unit="row",
                        ascii=True,
                        dynamic_ncols=True,
                    )
                )
                future = thread_executor.submit(
                    process_intraday_file,
                    thread_index,
                    intraday_file.name,
                    temp_rows,
                    MODELS,
                    process_executor,
                )
                futures.append(future)
            wait(futures, return_when="ALL_COMPLETED")
