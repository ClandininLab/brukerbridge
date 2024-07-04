import os
import shutil
from socket import *
from tkinter import Tk  # from tkinter import Tk for Python 3.x
from tkinter.filedialog import askdirectory

import brukerbridge as bridge

CHUNKSIZE = 1_000_000

### Luke testing ###
# host = 'localhost'
# port = 5000
# source_directory = "/Users/luke/Desktop/test_send"

### Brukerbridge computer ###
host = "171.65.17.84"
port = 5001

##################################
### WHAT DIRECTORY TO PROCESS? ###
##################################

Tk().withdraw()  # we don't want a full GUI, so keep the root window from appearing
source_directory = askdirectory(
    initialdir="G:/"
)  # show an "Open" dialog box and return the path to the selected file
source_directory = str(os.sep).join(
    source_directory.split("/")
)  # replace slashes with backslashes for windows
print(source_directory)

#########################
### CONNECT TO SERVER ###
#########################

sock = socket()
sock.connect((host, port))

##########################
### GET DIRECTORY SIZE ###
##########################

print("Calculating directory size... ", end="")
source_directory_size = bridge.get_dir_size(source_directory)
num_files = bridge.get_num_files(source_directory)
print("Done   |  {} GB   |   {} Files".format(source_directory_size, num_files))

sock.sendall(str(source_directory_size).encode() + b"\n")
sock.sendall(str(num_files).encode() + b"\n")

######################
### BEGIN TRASNFER ###
######################

num_files_sent = 0
for path, dirs, files in os.walk(source_directory):
    for file in files:

        filename = os.path.join(path, file)
        relpath = str(os.sep).join(filename.split(os.sep)[1:])
        filesize = os.path.getsize(filename)
        print(f"Sending {relpath}")

        checksum = bridge.get_checksum(filename)

        with open(filename, "rb") as f:
            sock.sendall(relpath.encode() + b"\n")
            sock.sendall(str(filesize).encode() + b"\n")
            sock.sendall(str(checksum).encode() + b"\n")

            # Send the file in chunks so large files can be handled.
            while True:
                data = f.read(CHUNKSIZE)
                if not data:
                    break
                sock.sendall(data)

        num_files_sent += 1

#########################
### TRANSFER COMPLETE ###
#########################

sock.sendall("ALL_FILES_TRANSFERED".encode() + b"\n")
message = sock.recv(1024).decode()
num_of_files_recieved = int(message.split(".")[0])
all_checksums_true = bool(message.split(".")[1])

#############################
### FINAL CLIENT PRINTING ###
#############################

if num_files_sent == num_of_files_recieved:
    if all_checksums_true:
        print("Confirmed correct number of files recieved and all checksums match.")
        print("DELETING BRUKER DATA...")
        shutil.rmtree(source_directory)
    else:
        print(
            "!!! Correct number of files but at least one checksum does not match !!!"
        )
else:
    print("!!! Number of files sent and recieve do not match !!!")
    print(f"Sent: {num_files_sent}; Recieved: {num_of_files_recieved}")

print("Done.")
