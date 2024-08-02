import logging
import os
import time
from pathlib import Path
from shutil import copyfile
from typing import List, Optional

from brukerbridge.utils import format_acq_path, get_dir_size, touch

logger = logging.getLogger(__name__)


def transfer_to_oak(
    source: str,
    target: str,
    allowable_extensions: Optional[List[str]],
    transferred: float,
):
    for item in os.listdir(source):
        source_path = source + "/" + item
        target_path = target + "/" + item

        if os.path.isdir(source_path):
            # Create same directory in target
            try:
                os.mkdir(target_path)
                logger.debug("creating %s", os.path.split(target_path)[-1])
            except FileExistsError:
                logger.debug("%s already exists", target_path)

            transferred += transfer_to_oak(
                source_path, target_path, allowable_extensions, transferred
            )
        else:
            if os.path.isfile(target_path):
                logger.debug("file already exists %s", target_path)
            else:
                if allowable_extensions is not None:
                    if source_path[-4:] not in allowable_extensions:
                        continue

                t_s = time.time()
                copyfile(source_path, target_path)
                t_d = time.time() - t_s

                file_size = os.path.getsize(source_path)
                file_size_mb = 1e-6 * file_size
                file_size_gb = 1e-9 * file_size

                transferred += file_size_gb

                try:
                    rate = file_size_mb / t_d
                    logger.debug(
                        "Transferred %s, %.1fGB, %.1f MB/s",
                        source_path,
                        file_size_gb,
                        rate,
                    )
                except ZeroDivisionError:
                    logger.error(
                        "Spacetime is broken or this is now the fastest computer in history"
                    )

    return transferred


def start_oak_transfer(
    acq_path: Path,
    oak_target: Path,
    allowable_extensions: Optional[List[str]],
    add_to_build_que: bool,
):
    sess_relative_acq_path = acq_path.relative_to(acq_path.parent.parent)
    target_path = oak_target / sess_relative_acq_path
    logger.debug("target path: %s", target_path)

    target_path.mkdir(parents=True, exist_ok=True)

    acq_size_gb = get_dir_size(acq_path)

    start_time = time.time()
    transferred_gb = transfer_to_oak(
        str(acq_path), str(target_path), allowable_extensions, 0
    )
    t_d = time.time() - start_time

    try:
        if transferred_gb <= acq_size_gb:
            logger.warning(
                "%s is %.1fGB but only %.1fGB was uploaded (%.1f MB/s). This means that existing files of the same name were already on oak.",
                format_acq_path(acq_path),
                acq_size_gb,
                transferred_gb,
                1e3 * transferred_gb / t_d,
            )
        elif transferred_gb > acq_size_gb:
            logger.error(
                "%s is %f GB but %f GB was uploaded. Best case scenario: get_dir_size is bugged"
            )
        else:
            logger.info(
                "%s upload complete. Transferred %.1fGB in %.fs (%.1f MB/s)",
                format_acq_path(acq_path),
                transferred_gb,
                t_d,
                1e3 * transferred_gb / t_d,
            )
    except ZeroDivisionError:
        logger.error(
            "Spacetime is broken or this is now the fastest computer in history"
        )

    if add_to_build_que:
        sess_name = acq_path.parent.name
        queue_sentinel_path = oak_target / "build_queue" / sess_name
        try:
            if not queue_sentinel_path.exists():
                touch(queue_sentinel_path)
                logger.info("Wrote build queue sentinel for %s", sess_name)
        except FileNotFoundError:
            (oak_target / "build_queue").mkdir()
            logger.warning(
                "Created missing build_queue dir: %s. You should review its permissions",
                os.path.join(oak_target, "build_queue"),
            )

            touch(queue_sentinel_path)
            logger.info("Wrote build queue sentinel for %s", sess_name)
