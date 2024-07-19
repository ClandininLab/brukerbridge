import logging
import os
import time
from datetime import datetime
from shutil import copyfile

logger = logging.getLogger(__name__)


def transfer_to_oak(source, target, allowable_extensions, verbose):
    print(source)
    for item in os.listdir(source):
        # Create full path to item
        source_path = source + "/" + item
        target_path = target + "/" + item

        # Check if item is a directory
        if os.path.isdir(source_path):
            # Create same directory in target
            try:
                os.mkdir(target_path)
                print("Creating directory {}".format(os.path.split(target_path)[-1]))
            except FileExistsError:
                if verbose:
                    print("WARNING: Directory already exists  {}".format(target_path))
                    print("Skipping Directory.")

            # RECURSE!
            transfer_to_oak(
                source_path,
                target_path,
                allowable_extensions,
                verbose,
            )

        # If the item is a file
        else:
            if os.path.isfile(target_path):

                if verbose:
                    print("File already exists. Skipping.  {}".format(target_path))

            else:

                if allowable_extensions is not None:
                    if source_path[-4:] not in allowable_extensions:
                        continue

                #####################
                ### TRANSFER FILE ###
                #####################

                file_size = os.path.getsize(source_path)
                file_size_MB = file_size * 10**-6
                file_size_GB = file_size * 10**-9

                now = datetime.now()
                current_time = now.strftime("%H:%M:%S")

                print(
                    "{} | Transfering file {}; size = {:.2f} GB".format(
                        current_time, target_path, file_size_GB
                    ),
                    end="",
                )

                t0 = time.time()
                copyfile(source_path, target_path)
                duration = time.time() - t0
                duration += 0.1

                print(
                    "done. duration: {} sec; {} MB/SEC".format(
                        int(duration), int(file_size_MB / duration)
                    )
                )


def start_oak_transfer(
    directory_from, oak_target, allowable_extensions, add_to_build_que, verbose=True
):
    directory_to = os.path.join(oak_target, os.path.split(directory_from)[-1])
    try:
        os.mkdir(directory_to)
    except FileExistsError:
        if verbose:
            print("WARNING: Directory already exists  {}".format(directory_to))
        # print('Skipping directory.')

    print("Moving from  {}".format(directory_from))
    print("Moving to  {}".format(directory_to))
    transfer_to_oak(directory_from, directory_to, allowable_extensions, verbose)

    # if directory_to.endswith('__queue__'):
    #     os.rename(directory_to, directory_to[:-9])
    #     print('removed __queue__ flag')
    # if directory_to.endswith('__lowqueue__'):
    #     os.rename(directory_to, directory_to[:-12])
    #     print('removed __lowqueue__ flag')

    print("*** Oak Upload Complete ***")
    if add_to_build_que in ["True", "true"]:
        folder = os.path.split(directory_to)[-1]
        queue_file = os.path.join(oak_target, "build_queue", folder)
        try:
            _touch(queue_file)
        except FileNotFoundError:
            os.mkdir(os.path.join(oak_target, "build_queue"))
            logger.warning(
                "Created missing build_queue dir: %s. You should review its permissions",
                os.path.join(oak_target, "build_queue"),
            )

            _touch(queue_file)

        print("Added {} to build queue.".format(folder))
    else:
        print("Add to build queue is False.")
        # os.rename(directory_to, directory_to + '__done__')
        # print('Added __done__ flag')


def _touch(fp):
    with open(fp, "w+"):
        pass
