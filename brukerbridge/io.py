import gzip
import json
import logging
import math
import shutil
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Tuple, Union, cast

import nibabel as nib
import numpy as np
from numpy.typing import NDArray

from brukerbridge.utils import package_path

logger = logging.getLogger(__name__)


def calculate_max_chunk_size(
    acq_shape: Union[Tuple[int, int, int, int], Tuple[int, int, int]],
    max_chunk_size: int,
) -> int:
    """Calculate the maximum chunk size  (along the last axis) for an acquisition of shape acq_shape
    that is sliced along the last axis and is less than max_chunk_size bytes

    Args
      acq_shape: (n_x, n_y, n_t), (n_x, n_y, n_z, n_t)
      max_chunk_size: bytes
    """
    # x2 comes from the uint16 type (16 bits) divided by 8 bits/byte
    bytes_per_sample = 2 * math.prod(acq_shape[:-1])
    max_samples = max_chunk_size // bytes_per_sample

    if max_samples == 0:
        raise RuntimeError(f"max_chunk_size {max_chunk_size} too small")

    return min(acq_shape[-1], max_samples)


# TODO: docstring
def write_nifti_streaming(
    header: nib.nifti2.Nifti2Header, frame_gen: Iterator[NDArray], output_path: Path
):
    """if output_path is suffixed by .gz  output will be compressed. otherwise it is written uncompressed"""
    with guarded_output_file(output_path) as guarded_output_path:
        required_frames = math.prod(header.get_data_shape()[2:])

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

            frame_idx = 0
            for frame_idx, frame in enumerate(frame_gen):
                assert frame.dtype == np.uint16

                # NOTE: the net result of this transposition and the frame
                # generators is Fortran style column-major order
                for slc in frame.T:
                    img_fh.write(slc.tobytes())

                if frame_idx + 1 == required_frames:
                    break

            if frame_idx + 1 < required_frames:
                raise RuntimeError(
                    f"frame_gen did not yield required number of frames. Expected: {required_frames}, Got: {frame_idx}"
                )


# TODO: docstring
def write_nifti_streaming_chunked(
    header: nib.nifti2.Nifti2Header,
    frame_gen: Iterator[NDArray],
    output_path: Path,
    max_image_size: int,
):
    """if output_path is suffixed by .gz  output will be compressed. otherwise it is written uncompressed

    max_image_size gives max size images will be written as. acquisitions that exceed that will be broken up into chunks to fit the limit. Note that the precise size could be larger than this by a few hundred bytes due to the header. -1 max_image_size disables chunking
    """
    # more java bullshit with no runtime effect, don't worry about it
    acq_shape = cast(
        Union[Tuple[int, int, int, int], Tuple[int, int, int]], header.get_data_shape()
    )
    chunk_size = calculate_max_chunk_size(acq_shape, max_image_size)

    for chunk_idx, start_idx in enumerate(range(0, acq_shape[-1], chunk_size)):
        end_idx = min(start_idx + chunk_size, acq_shape[-1])
        chunk_shape = acq_shape[:-1] + (end_idx - start_idx,)

        chunk_header = cast(
            nib.nifti2.Nifti2Header, nib.nifti2.Nifti2Header.from_header(header)
        )
        chunk_header.set_data_shape(chunk_shape)

        prefix, *suffixes = output_path.name.split(".")
        chunk_output_path = (
            output_path.parent / f"{prefix}_chunk_{chunk_idx}.{'.'.join(suffixes)}"
        )

        write_nifti_streaming(chunk_header, frame_gen, chunk_output_path)


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
