import os
import subprocess
import sys


def convert_raw_to_tiff(full_target):
    # Start ripper watcher, which will kill bruker conversion utility when complete
    print("Starting Ripper Watcher. Will watch {}".format(full_target))
    subprocess.Popen(
        [
            sys.executable,
            "C:/Users/User/projects/brukerbridge/scripts/ripper_killer.py",
            full_target,
        ]
    )

    # Start Bruker conversion utility by calling ripper.bat
    print("Converting raw to tiff...")
    os.system(
        "C:/Users/User/projects/brukerbridge/scripts/ripper.bat {}".format(
            '"' + full_target + '"'
        )
    )
