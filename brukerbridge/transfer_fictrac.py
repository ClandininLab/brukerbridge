import os
from pathlib import Path

import ftputil

import brukerbridge as bridge


def transfer_fictrac(user: str):

    ip = "171.65.17.246"
    username = "clandininlab"
    passwd = "jointhelab@"
    fictrac_target = "H:/fictrac/{}".format(user)
    # fictrac_source = 'fictrac_data/{}'.format(user)
    # fictrac_source = 'fictrac_data'
    # fictrac_source = 'luke_data'
    fictrac_source = "../../data/luke_data"
    allowable_extensions = [".log", ".avi", ".dat", ".txt"]
    oak_target = "X:/data/fictrac"

    print("Starting download of fictrac files.")
    ftp_host = ftputil.FTPHost(ip, username, passwd)
    all_fictrac_files = ftp_host.listdir(fictrac_source)
    # of the form:#
    # fictrac-20181116_172030.log
    # fictrac-20181116_172038-debug.avi
    # fictrac-20181116_172038-raw.avi
    # fictrac-20181116_172030.dat

    # Should download all fictrac files
    # today = datetime.today().strftime('%Y%m%d')

    for file in all_fictrac_files:
        if file[-4:] in allowable_extensions:
            target_path = fictrac_target + "/" + file  # type: ignore
            source_path = fictrac_source + "/" + file  # type: ignore
            if os.path.isfile(target_path):
                pass
                # print('File already exists. Skipping.  {}'.format(target_path))
            else:
                print("Downloading {}".format(target_path))
                ftp_host.download(source_path, target_path)

    # Send fictrac files to oak
    bridge.start_oak_transfer(
        Path(fictrac_target),
        Path(oak_target),
        allowable_extensions=None,
        add_to_build_que=False,
    )

    print("Finished upload of fictrac files to oak.")
