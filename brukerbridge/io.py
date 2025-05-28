import json
import logging
import shutil
from pathlib import Path
from typing import Iterator

import nibabel as nib
from numpy.typing import NDArray

from brukerbridge.utils import package_path

logger = logging.getLogger(__name__)


def write_nifti_streaming(
    header: nib.nifti1.Nifti1Header, frame_gen: Iterator[NDArray], output_path: Path
):
    with open(output_path, "wb") as img_fh:
        header.write_to(img_fh)

        img_fh.seek(header.get_data_offset())
        assert img_fh.tell() == header.get_data_offset()

        for frame in frame_gen:
            # NOTE: the net result of this transposition and the frame
            # generators is Fortran style column-major order
            for slc in frame.T:
                img_fh.write(slc.tobytes())


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
