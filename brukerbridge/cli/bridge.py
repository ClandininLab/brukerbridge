""" Main process managing everything except for file transfers from the bruker computer
"""
import concurrent.futures
import json
import logging
import logging.config
import logging.handlers
import multiprocessing
import os
import subprocess
import threading
import time
from collections import deque
from glob import glob
from pathlib import Path
from xml.etree import ElementTree

from brukerbridge import (convert_tiff_collections_to_nii,
                          convert_tiff_collections_to_nii_split,
                          convert_tiff_collections_to_stack)
from brukerbridge.logging import (configure_logging, logger_thread,
                                  worker_process)
from brukerbridge.transfer_fictrac import transfer_fictrac
from brukerbridge.transfer_to_oak import start_oak_transfer
from brukerbridge.utils import package_path

logger = logging.getLogger()

# can be overridden by argument to module main
DEFAULT_ROOT_DIR = "H:/test-data"
LOG_DIR = "C:/Users/User/logs"

EXTENSION_WHITELIST = [
    ".nii",
    ".csv",
    ".xml",
    "json",
    "tiff",
    "hdf5",
]

# this is set by what Image-Block Ripping utility is used and can be easily changed
SUPPORTED_PRAIREVIEW_VERSION = "5.5.64.600"

RIPPER_EXECUTABLE = r"C:\Program Files\Prairie 5.5.64.600\Prairie View\Utilities\Image-Block Ripping Utility.exe"

# max concurrent processes
MAX_RIPPERS = 8
MAX_TIFF_WORKERS = 4
MAX_OAK_WORKERS = 4


def main(root_dir=None):
    configure_logging(LOG_DIR)
    log_queue = multiprocessing.Manager().Queue(-1)
    log_thread = threading.Thread(target=logger_thread, args=(log_queue,))
    log_thread.start()

    rip_queue = deque()
    tiff_queue = deque()
    oak_io_queue = deque()
    fictrac_io_queue = deque()

    # acquisitions in any stage of processing
    in_process_acqs = set()

    # acq_path: Popen
    ripper_processes = dict()
    # acq_path: Future
    tiff_futures = dict()
    # acq_path: Future
    oak_io_futures = dict()
    # user: Future
    fictrac_io_futures = dict()

    try:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=MAX_TIFF_WORKERS
        ) as tiff_executor, concurrent.futures.ProcessPoolExecutor(
            max_workers=MAX_OAK_WORKERS
        ) as io_executor:
            while True:

                # ====================================================
                # ============ LOOK FOR NEW ACQUISITIONS  ============
                # ====================================================

                marked_acqs = find_marked_acquisitions(
                    root_dir or DEFAULT_ROOT_DIR, in_process_acqs
                )
                rip_queue.extend(marked_acqs)
                for marked_acq in marked_acqs:
                    logger.info("Queued %s for processing", format_acq_path(marked_acq))

                # =================================================
                # ============ MANAGE RIPPER PROCESSES ============
                # =================================================

                # kill any rippers that have finished
                for acq_path, ripper_process in list(ripper_processes.items()):
                    if ripping_complete(acq_path):
                        ripper_process.kill()

                    logger.info(
                        "Killed completed ripper process for %s",
                        format_acq_path(acq_path),
                    )
                    logger.debug("Ripper pid %s", ripper_process.pid)
                    del ripper_processes[acq_path]

                    tiff_queue.append(acq_path)
                    logger.info(
                        "%s added to tiff conversion queue", format_acq_path(acq_path)
                    )

                # start new rippers up to the limit
                while len(ripper_processes) <= MAX_RIPPERS:
                    if not rip_queue:
                        break

                    acq_path = rip_queue.popleft()
                    in_process_acqs.add(acq_path)

                    # TODO: review ripper arguments
                    # on windows args is converted to a string anyways
                    ripper_processes[acq_path] = subprocess.Popen(
                        f"{RIPPER_EXECUTABLE} -isf -arfwsf {acq_path} -cnv"
                    )

                    logger.info(
                        "Spawned ripper process for %s", format_acq_path(acq_path)
                    )
                    logger.debug("Ripper pid %s", ripper_processes[acq_path].pid)

                # =======================================
                # ============ CONVERT TIFFS ============
                # =======================================

                # submit conversion futures
                while len(tiff_queue) > 0:
                    acq_path = tiff_queue.popleft()
                    config = acq_config(acq_path)
                    conversion_target = config["convert_to"]

                    if conversion_target == "nii":
                        if config.get("split", False):
                            tiff_futures[acq_path] = tiff_executor.submit(
                                worker_process,
                                convert_tiff_collections_to_nii_split,
                                log_queue,
                                str(acq_path),
                            )
                        else:
                            tiff_futures[acq_path] = tiff_executor.submit(
                                worker_process,
                                convert_tiff_collections_to_nii,
                                log_queue,
                                str(acq_path),
                            )

                        # more precisely, a process has been submitted to the
                        # queue and will be spawned when the executor has an
                        # idle worker slot
                        logger.info(
                            "Spawned NIfTI conversion process for %s",
                            format_acq_path(acq_path),
                        )
                    elif conversion_target == "tiff":
                        tiff_futures[acq_path] = tiff_executor.submit(
                            worker_process,
                            convert_tiff_collections_to_stack,
                            log_queue,
                            str(acq_path),
                        )

                        logger.info(
                            "Spawned tiff conversion process for %s",
                            format_acq_path(acq_path),
                        )
                    else:
                        logger.error(
                            "Invalid 'convert_to' value for %s config",
                            acq_path.parent.parent.name,
                        )

                # queue completed conversions for io
                for acq_path, tiff_future in list(tiff_futures.items()):
                    if tiff_future.done():
                        # raise possible remote exception
                        tiff_future.result()

                        del tiff_futures[acq_path]
                        oak_io_queue.append(acq_path)
                        fictrac_io_queue.append(acq_path)

                        logger.info(
                            "%s tiff conversion complete, queued for oak upload",
                            format_acq_path(acq_path),
                        )

                # =======================================
                # ============ UPLOAD TO OAK ============
                # =======================================

                while len(oak_io_queue) > 0:
                    acq_path = oak_io_queue.popleft()
                    config = acq_config(acq_path)
                    oak_io_futures[acq_path] = io_executor.submit(
                        worker_process,
                        start_oak_transfer,
                        log_queue,
                        str(acq_path),
                        config["oak_target"],
                        EXTENSION_WHITELIST,
                        config["add_to_build_que"],
                    )

                    logger.info(
                        "Spawned oak upload process for %s", format_acq_path(acq_path)
                    )

                for acq_path, oak_io_future in list(oak_io_futures.items()):
                    if oak_io_future.done():
                        oak_io_future.result()
                        del oak_io_futures[acq_path]

                        logger.info("%s oak upload complete", format_acq_path(acq_path))

                # ====================================
                # ============ FICTRAC IO ============
                # ====================================

                # NOTE: it's unclear whether this is actively in use as only a
                # few people have the transfer_fictrac setting enabled in their
                # config

                # NOTE: as the fictrac IO logic is indexed by user instead of
                # acquisition, different bookkeeping logic is required here to
                # prevent starting multiple simultaneous sessions for a single user

                while len(fictrac_io_queue) > 0:
                    acq_path = fictrac_io_queue.popleft()
                    user_name = acq_path.parent.parent.name
                    config = acq_config(acq_path)

                    # if user_name is in fictrac_io_futures then we have very
                    # recently done io for fictrac and can safely move on
                    # without trying again
                    if (
                        config.get("transfer_fictrac", False)
                        and user_name not in fictrac_io_futures
                    ):
                        # TODO: test multiple simultaneous FTP instances
                        fictrac_io_futures[user_name] = io_executor.submit(
                            worker_process, transfer_fictrac, log_queue, user_name
                        )
                        logger.info(
                            "Spawned fictrac io process for %s",
                            format_acq_path(acq_path),
                        )

                for user_name, fictrac_io_future in list(fictrac_io_futures.items()):
                    if fictrac_io_future.done():
                        fictrac_io_future.result()

                        del fictrac_io_futures[user_name]

                        logger.info("fictrac io for %s complete", user_name)

                # ====================================
                # ============ BOOKKEEPING ===========
                # ====================================

                for acq_path in list(in_process_acqs):
                    user_name = acq_path.parent.parent.name

                    if acq_path in ripper_processes or acq_path in rip_queue:
                        continue

                    if acq_path in tiff_futures:
                        continue

                    if acq_path in oak_io_futures:
                        continue

                    if user_name in fictrac_io_futures:
                        continue

                    # we're all done and can remove the __queue__ suffix
                    os.rename(acq_path, acq_path.parent / acq_path_prefix(acq_path))

                    in_process_acqs.remove(acq_path)

                    logger.info("All work complete for %s", format_acq_path(acq_path))

                for in_process_acq in in_process_acqs:
                    logger.info("Still processing %s", format_acq_path(in_process_acq))
                time.sleep(30)
    except Exception as unhandled_exception:
        logger.critical("Fatal exception", exc_info=True)
        raise unhandled_exception
    finally:
        # flush log queue before exiting
        log_queue.put(None)
        log_thread.join()


def acq_config(acquisition_path):
    """Look up the user config for this acquisition"""
    user_name = acquisition_path.parent.parent.name
    with open(f"{package_path()}/users/{user_name}.json", "r") as handle:
        return json.load(handle)


def ripping_complete(acquisition_path):
    """Checks whether raw data has been deleted, indicative of ripping halting"""
    return len(glob(f"{acquisition_path}/*_RAWDATA_*")) == 0


def find_marked_acquisitions(root_dir, in_process_acqs):
    """Searches for acquisitions under ROOT_DIR marked for processing

    A marked acquisition is a directory suffixed by '__queue__' or
    '__lowqueue__' that contains a valid PraireView XML file

    Those suffixed by '__lowqueue__' are added to the back of the queue,
    although in practice lowqueue is deprecated since acquisitions are
    processed in parallel. Don't use it.

    Args:
      in_process_acqs: acquisitions which have already been queued and can be ignored - [Path]

    Returns:
      marked_acquisitions - [str]

    """
    # recursive glob is expensive due to the large number of .tiffs, so marked
    # directories must be at fixed depth
    marked_dirs = glob(f"{root_dir}/*/*__queue__") + glob(f"{root_dir}/*/*__lowqueue__")
    marked_dirs = [Path(m).resolve() for m in marked_dirs]
    logger.debug("Marked dirs: %s", marked_dirs)

    marked_acquisitions = []

    for marked_dir in marked_dirs:
        # users who have provided a config file
        user_names = [f.split(".")[0] for f in os.listdir(package_path() / "users")]

        # check that marked dirs have a user config
        if not marked_dir.parent.name in user_names:
            logging.error(
                "Cannot process marked directory due to missing user config: %s",
                marked_dir,
            )

        for acq_path in marked_dir.iterdir():
            if acq_path in in_process_acqs:
                continue

            if contains_valid_xml(acq_path):
                marked_acquisitions.append(acq_path)

    return marked_acquisitions


def contains_valid_xml(acquisition_path):
    """Checks that acquisition dir contains a parsable PVScan XML file made with the
    correct version of PrarieView.
    """
    for xml_file in glob(f"{acquisition_path}/*.xml"):
        try:
            tree = ElementTree.parse(xml_file)
            root = tree.getroot()

            if root.attrib["version"] != SUPPORTED_PRAIREVIEW_VERSION:
                logger.error(
                    "XML created by unsupported version of PraireView: %s",
                    xml_file,
                )
                continue

            if root.tag == "PVScan":
                first_seq = root[2]
                assert first_seq.tag == "Sequence"

                # ZSeries and single plane Tseries have types 'TSeries ZSeries Element'
                # and 'TSeries Timed Element' respectively
                if first_seq.attrib["type"] == "Single":
                    logger.debug(
                        "Ignoring SingleImage acquisition %s", acquisition_path
                    )
                    return False

                return True

        except ElementTree.ParseError:
            logger.exception(
                "Unparseable XML file %s in acquisition %s",
                Path(xml_file).name,
                acquisition_path,
            )
        except (KeyError, AssertionError):
            logger.exception(
                "XML %s does not have the expected structure. Are you sure you've got the right files?",
                xml_file,
            )
    else:
        logger.error(
            "Missing or unparseable PVScan XML file in acquisition %s. Cannot process acquisition.",
            acquisition_path,
        )
        return False


def format_acq_path(acq_path):
    return f"{acq_path.parent.name}/{acq_path.name}"


def acq_path_prefix(acq_path):
    if acq_path.name.endswith("__queue__"):
        return acq_path.name[:-9]
    elif acq_path.name.endswith("__lowqueue__"):
        return acq_path.name[:-12]
    else:
        raise RuntimeError(f"Invalid suffix: {acq_path}")
