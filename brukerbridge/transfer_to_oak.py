import logging
import os
import time
from shutil import copyfile

logger = logging.getLogger(__name__)


def transfer_to_oak(source, target, allowable_extensions, transferred):
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
                    pass

    return transferred


def start_oak_transfer(
    directory_from, oak_target, allowable_extensions, add_to_build_que
):
    directory_to = os.path.join(oak_target, os.path.split(directory_from)[-1])
    try:
        os.mkdir(directory_to)
    except FileExistsError:
        logger.debug("%s already exists", directory_to)

    start_time = time.time()
    transferred = transfer_to_oak(directory_from, directory_to, allowable_extensions, 0)
    t_d = time.time() - start_time

    try:
        logger.info(
            "%s upload complete. Transferred %.1fGB in %.fs (%.1f MB/s)",
            directory_from,
            transferred,
            t_d,
            1e-3 * transferred / t_d,
        )
    except ZeroDivisionError:
        logger.warning(
            "%s upload: no files transferred, remote already up to date",
            directory_from,
        )

    if add_to_build_que:
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

        logger.info("%s added to build queue", folder)


def _touch(fp):
    with open(fp, "w+"):
        pass
