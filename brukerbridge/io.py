import gzip
import json
import logging
import math
import shutil
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import nibabel as nib
from numpy.typing import NDArray

from brukerbridge.utils import package_path

logger = logging.getLogger(__name__)


# TODO: docstring
def write_nifti_streaming(
    header: nib.nifti1.Nifti1Header, frame_gen: Iterator[NDArray], output_path: Path
):
    """it output_path is suffixed by .gz  output will be compressed. otherwise it is written uncompressed"""
    with guarded_output_file(output_path) as guarded_output_path:
        expected_vals = math.prod(header.get_data_shape())
        yielded_vals = 0

        file_interface_kwargs = dict()
        file_interface_kwargs["mode"] = "wb"

        if output_path.suffix == ".gz":
            file_interface = gzip.GzipFile
            # required to make gzip deterministic. if mtime is omitted current time is included in output file
            file_interface_kwargs["mtime"] = 0
        else:
            file_interface = open

        with file_interface(guarded_output_path, **file_interface_kwargs) as img_fh:
            header.write_to(img_fh)

            img_fh.seek(header.get_data_offset())
            assert img_fh.tell() == header.get_data_offset()

            for frame in frame_gen:
                yielded_vals += math.prod(frame.shape)
                if yielded_vals > expected_vals:
                    break

                # NOTE: the net result of this transposition and the frame
                # generators is Fortran style column-major order
                for slc in frame.T:
                    img_fh.write(slc.tobytes())

            if yielded_vals != expected_vals:
                # NOTE: generator is terminated if it yields more than the
                # expected number of vals, so that case is probably
                # undercounted
                raise RuntimeError(
                    f"frame_gen did not yield expected number of values. Expected: {expected_vals}, Yielded: {yielded_vals}"
                )


@contextmanager
def guarded_output_file(output_path):
    """Context manager that provides a temp path to write to, which it renames
    to the desired output_path if the context is exited normally. The
    guarded/temp output file is deleted if any exceptions are encountered with
    the context
    """
    # write to a temp output path and copy tp final destination only when writing succesfully completes, to avoid leaving partial files if unexpectedly killed
    tmp_output_path = output_path.parent / str(uuid.uuid4())
    logger.debug("Temp output path for %s: %s ", output_path, tmp_output_path)

    try:
        yield tmp_output_path
        tmp_output_path.rename(output_path)
    except Exception as exc:
        logger.debug(
            "Removing guarded output for %s due. Exception caught within context.",
            output_path,
        )
        tmp_output_path.unlink(missing_ok=True)
        raise exc


def copy_session_metadata(session_path: Path):
    """Copy any session level files to the oak target, presumed to be metadata"""
    logger.debug("copy_session_metadata session_path: %s", session_path)

    user_name = session_path.parent.name
    with open(f"{package_path()}/users/{user_name}.json", "r") as handle:
        user_config = json.load(handle)

    target_path = Path(user_config["oak_target"]) / session_path.name

    for path in session_path.iterdir():
        if path.is_file():
            shutil.copyfile(path, target_path / path.name)
            logger.debug(
                "Copied session metadata: %s -> %s", path, target_path / path.name
            )
