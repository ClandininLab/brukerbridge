# from socket import *
# import os
# import sys
# import subprocess
# import brukerbridge as bridge
# import time
# from time import strftime

# log_folder = 'C:/Users/User/Desktop/dataflow_logs'
# log_file = 'dataflow_log_' + strftime("%Y%m%d-%H%M%S") + '.txt'
# full_log_file = os.path.join(log_folder, log_file)
# sys.stdout = bridge.Logger_stdout(full_log_file)

# print('good afternoon.')
# sys.stdout.flush()


import os
import sys
from socket import *
from time import strftime

import brukerbridge as bridge

verbose = False
CHUNKSIZE = 1_000_000
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 5001
target_directory = "H:/"

####
# SERVER_HOST = ""
# SERVER_PORT = 5000
# target_directory = "/Users/luke/Desktop/test_recieve"
####

log_folder = "C:/Users/User/Desktop/dataflow_logs"
log_file = "dataflow_log_" + strftime("%Y%m%d-%H%M%S") + ".txt"
full_log_file = os.path.join(log_folder, log_file)
print("hi")
sys.stdout = bridge.Logger_stdout(full_log_file)
sys.stderr = bridge.Logger_stderr(full_log_file)
print("oatmeal")
sys.stdout.flush()
