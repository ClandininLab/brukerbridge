import hashlib
import inspect
import json
import os
import smtplib
import sys
from datetime import datetime
from email.mime.text import MIMEText
from functools import wraps
from pathlib import Path
from time import time

import numpy as np

import brukerbridge


def package_path():
    """Returns the absolute path to this package base directory"""
    return Path(inspect.getfile(brukerbridge)).parent.parent


def sec_to_hms(t):
    secs = f"{np.floor(t%60):02.0f}"
    mins = f"{np.floor((t/60)%60):02.0f}"
    hrs = f"{np.floor((t/3600)%60):02.0f}"
    return ":".join([hrs, mins, secs])


def print_progress_table(
    start_time, current_iteration, total_iterations, current_mem, total_mem, mode
):
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


def progress_bar(iteration, total, length, fill="#"):
    if total == 0:
        total = 1
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + "-" * (length - filledLength)
    fraction = f"{str(iteration):^4}" + "/" + f"{str(total):^4}"
    bar_string = f"{bar}"
    return bar_string


def get_num_files(directory):
    num_files = 0
    for path, dirs, files in os.walk(directory):
        num_files += len(files)
    return num_files


def get_dir_size(directory):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size * 10**-9  # report in GB


def get_checksum(filename):
    with open(filename, "rb") as f:
        bytes = f.read()  # read file as bytes
        readable_hash = hashlib.md5(bytes).hexdigest()
    return readable_hash


def get_json_data(file_path):
    with open(file_path) as f:
        data = json.load(f)
    return data


def send_email(subject="", message="", recipient="brezovec@stanford.edu"):
    """Sends emails!

    Parameters
    ----------
    subject: email subject heading (str)
    message: body of text (str)

    Returns
    -------
    Nothing."""
    print("Sending email to {} ({})".format(recipient, subject))
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login("python.notific@gmail.com", "uxnglqrswphwtdsf")

    msg = MIMEText(message)
    msg["Subject"] = subject

    server.sendmail(recipient, recipient, msg.as_string())
    server.quit()


def timing(f):
    """Wrapper function to time how long functions take (and print function name)."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        start = time()
        print("\nFUNCTION CALL: {}".format(f.__name__))
        sys.stdout.flush()
        result = f(*args, **kwargs)
        end = time()
        duration = end - start

        # Make units nice (originally in seconds)
        if duration < 1:
            duration = duration * 1000
            suffix = "ms"
        elif duration < 60:
            duration = duration
            suffix = "sec"
        elif duration < 3600:
            duration = duration / 60
            suffix = "min"
        else:
            duration = duration / 3600
            suffix = "hr"

        print(
            "FUNCTION {} done. DURATION: {:.2f} {}".format(f.__name__, duration, suffix)
        )
        sys.stdout.flush()
        return result

    return wrapper


class Logger_stdout(object):
    def __init__(self, full_log_file):
        # self.terminal = sys.stdout
        # log_folder = 'C:/Users/User/Desktop/dataflow_logs'
        # log_file = 'dataflow_log_' + strftime("%Y%m%d-%H%M%S") + '.txt'
        # self.full_log_file = os.path.join(log_folder, log_file)
        self.log = open(full_log_file, "a")
        self.log = sys.stdout

    def write(self, message):
        #  self.terminal.write(message)
        # self.terminal.write('boo')
        # self.log.write('boo2')
        self.log.write(message)

    def flush(self):
        # this flush method is needed for python 3 compatibility.
        # this handles the flush command by doing nothing.
        # you might want to specify some extra behavior here.
        pass


# class Logger_stdout(object):
#     def __init__(self, full_log_file):
#         self.terminal = sys.stdout
#         # log_folder = 'C:/Users/User/Desktop/dataflow_logs'
#         # log_file = 'dataflow_log_' + strftime("%Y%m%d-%H%M%S") + '.txt'
#         # self.full_log_file = os.path.join(log_folder, log_file)
#         self.log = open(full_log_file, "a")

#     def write(self, message):
#         self.terminal.write(message)
#         #self.terminal.write('boo')
#         #self.log.write('boo2')
#         self.log.write(message)

#     def flush(self):
#         #this flush method is needed for python 3 compatibility.
#         #this handles the flush command by doing nothing.
#         #you might want to specify some extra behavior here.
#         pass


class Logger_stderr(object):
    def __init__(self, full_log_file):
        self.terminal = sys.stderr
        # log_folder = 'C:/Users/User/Desktop/dataflow_error'
        # log_file = 'dataflow_log_' + strftime("%Y%m%d-%H%M%S") + '.txt'
        # self.full_log_file = os.path.join(log_folder, log_file)
        self.log = open(full_log_file, "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        # this flush method is needed for python 3 compatibility.
        # this handles the flush command by doing nothing.
        # you might want to specify some extra behavior here.
        pass
