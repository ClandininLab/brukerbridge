import json
import os
import subprocess
import sys
import time
import warnings

import brukerbridge as bridge

warnings.filterwarnings("ignore", category=DeprecationWarning)

extensions_for_oak_transfer = [
    ".nii",
    ".csv",
    ".xml",
    "json",
    "tiff",
    "hdf5",
]  # needs to be 4 char
users_directory = "C:/Users/User/projects/brukerbridge/users"


def main(args):

    ############################
    ### Get target directory ###
    ############################

    # user = args[0].lower().strip('"')
    # directory = args[1].strip('"')
    # full_target = os.path.join(root_directory, user, directory)
    # print("full target: {}".format(full_target))

    dir_to_process = args[0].lower().strip('"')
    dir_to_process = os.path.normpath(dir_to_process)
    user, directory = dir_to_process.split(os.sep)[1], dir_to_process.split(os.sep)[2]
    print("Directory to process: {}".format(dir_to_process))

    #########################
    ### Get user settings ###
    #########################

    if user + ".json" in [x.lower() for x in os.listdir(users_directory)]:
        json_file = os.path.join(users_directory, user + ".json")
        with open(json_file) as file:
            settings = json.load(file)

    oak_target = settings["oak_target"]
    convert_to = settings["convert_to"]
    email = settings.get("email", False)
    add_to_build_que = settings.get("add_to_build_que", False)
    transfer_fictrac = settings.get("transfer_fictrac", False)
    split = settings.get("split", False)

    ######################################
    ### Save email for error reporting ###
    ######################################

    # email_file = 'C:/Users/User/projects/brukerbridge/scripts/email.txt'
    # with open(email_file, 'w') as f:
    # 	f.write(email)

    #################################
    ### Convert from raw to tiffs ###
    #################################

    t0 = time.time()
    bridge.convert_raw_to_tiff(dir_to_process)
    print("RAW TO TIFF DURATION: {} MIN".format(int((time.time() - t0) / 60)))

    #########################################
    ### Convert tiff to nii or tiff stack ###
    #########################################

    if convert_to == "nii":
        if split:
            bridge.convert_tiff_collections_to_nii_split(dir_to_process)
        else:
            bridge.convert_tiff_collections_to_nii(dir_to_process)
    elif convert_to == "tiff":
        bridge.convert_tiff_collections_to_stack(dir_to_process)
    else:
        print(
            "{} is an invalid convert_to variable from user metadata.".format(
                convert_to
            )
        )
        print("Must be nii or tiff, with no period")

    #######################
    ### Transfer to Oak ###
    #######################
    start_time = time.time()
    size_transfered = bridge.start_oak_transfer(
        dir_to_process, oak_target, extensions_for_oak_transfer, add_to_build_que
    )
    print("OAK TRANSFER DURATION: {} MIN".format(int((time.time() - start_time) / 60)))

    ##############################
    ### Transfer fictrac files ###
    ##############################
    if transfer_fictrac:
        try:
            bridge.transfer_fictrac(user)
        except:
            print("-----------> FICTRAC TRANSFER FAILED <-----------")

    # ### Delete files locally
    # if delete_local:
    #     bridge.delete_local(full_target)


if __name__ == "__main__":
    main(sys.argv[1:])
