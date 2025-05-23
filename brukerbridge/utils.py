import hashlib
import inspect
import logging
import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from time import time
from typing import Optional, Tuple

import numpy as np

import brukerbridge

logger = logging.getLogger(__name__)


def package_path() -> Path:
    """Returns the absolute path to this package base directory"""
    return Path(inspect.getfile(brukerbridge)).parent.parent


def parse_malformed_json_bool(field) -> bool:
    """Parses 'boolean' values from a parsed json that might have been written
    accidentally as strings, as could happen if json was written manually.
    returns the intended value
    """
    if isinstance(field, bool):
        return field
    elif isinstance(field, str):
        if field.lower() in ("true", "false"):
            return field.lower() == "true"
        else:
            raise RuntimeError(
                f"String field with value '{field}' cannot be interpreted as a boolean. Refer to JSON spec"
            )
    else:
        raise RuntimeError(
            f"Field with type {type(field)} cannot be interpreted as a boolean"
        )


def sec_to_hms(t):
    secs = f"{np.floor(t%60):02.0f}"
    mins = f"{np.floor((t/60)%60):02.0f}"
    hrs = f"{np.floor((t/3600)%60):02.0f}"
    return ":".join([hrs, mins, secs])


def print_progress_table(
    start_time, current_iteration, total_iterations, current_mem, total_mem, mode
):
    print_iters = list()
    if mode == "server":
        print_iters = [
            1,
            2,
            4,
            8,
            16,
            32,
            50,
            75,
            100,
            125,
            150,
            175,
            200,
            225,
            250,
            275,
            300,
            325,
            350,
            375,
            400,
            500,
            600,
            700,
            800,
            900,
            1000,
            5000,
            10000,
            10000,
        ]
    if mode == "tiff_convert":
        print_iters = [
            1,
            2,
            4,
            8,
            16,
            32,
            64,
            128,
            256,
            512,
            1064,
            2128,
            4256,
            8512,
            17024,
            34048,
            68096,
        ]

    fraction_complete = current_iteration / total_iterations
    elapsed = time() - start_time
    elapsed_hms = sec_to_hms(elapsed)
    try:
        remaining = elapsed / fraction_complete - elapsed
    except ZeroDivisionError:
        remaining = 0
    remaining_hms = sec_to_hms(remaining)

    ### PRINT TABLE TITLE ###
    if current_iteration == 1:
        title_string = "| Current Time |  Print Frequency  |     Num / Total   |         GB / Total      | Elapsed Time / Remaining   |"
        # if mode == 'server':
        #     title_string += "  MB / SEC  |"
        print(title_string, flush=True)

    now = datetime.now()
    current_time_string = "   {}   ".format(now.strftime("%H:%M:%S"))
    print_freq_string = "       {:05d}       ".format(current_iteration)
    iteration_string = "   {:05d} / {:05d}   ".format(
        current_iteration, total_iterations
    )
    memory_string = "        {:03d} / {:03d}        ".format(current_mem, total_mem)
    time_string = f"     {elapsed_hms} / {remaining_hms}    "
    full_string = "|".join(
        [
            "",
            current_time_string,
            print_freq_string,
            iteration_string,
            memory_string,
            time_string,
            "",
        ]
    )
    # if mode == 'server':
    #     speed =
    #     full_string =+ '{}'.format()

    if current_iteration in print_iters:
        print(full_string, flush=True)

    if current_iteration == total_iterations:
        print(full_string, flush=True)


def get_num_files(directory):
    num_files = 0
    for _, _, files in os.walk(directory):
        num_files += len(files)
    return num_files


def get_dir_size(directory: str, suffix_whitelist: Optional[Tuple[str]] = None):
    """recursive dir size"""
    total_size = 0
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                if suffix_whitelist is not None:
                    if not fp.endswith(suffix_whitelist):
                        continue

                total_size += os.path.getsize(fp)
    return total_size * 10**-9  # report in GB


def get_checksum(filename):
    with open(filename, "rb") as f:
        bytes = f.read()  # read file as bytes
        readable_hash = hashlib.md5(bytes).hexdigest()
    return readable_hash


def touch(fp):
    with open(fp, "w+"):
        pass


def format_acq_path(acq_path: Path) -> str:
    user_name = acq_path.parts[1]
    return f"{user_name}: {'/'.join(acq_path.parts[2:])}"


@contextmanager
def log_worker_exception(
    worker_name: str,
    work_label: str,
    exc_info: bool,
    reraise: bool,
    success_msg: Optional[str] = None,
):
    """Logs an exception that occurs within this context, if one occurs. Only specific to worker exceptions as so far as the log message

    success_msg is logged at INFO level if no errors are encountered
    """
    try:
        yield
        if success_msg is not None:
            logger.info(success_msg)
    except:
        logger.critical(
            "Fatal exceptions raised from %s %s worker",
            work_label,
            worker_name,
            exc_info=exc_info,
        )
        if reraise:
            raise
