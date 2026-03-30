import sys
import logging
import uuid
from pathlib import Path
from contextlib import contextmanager
import math
import nibabel as nib
import numpy as np
import tifffile
import warnings
from enum import Enum
from xml.etree import ElementTree
import time
from functools import wraps

def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        logger.info(f"'{func.__name__}' executed in {elapsed_time} seconds")
        return result
    return wrapper

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

def general_frame_gen(tiff, channel_idx, z_pages_per_channel, channel_amount, bidirZ):
    empty_page = np.zeros(tiff.series[0].pages[0].shape).astype(np.uint16).T
    start = z_pages_per_channel * channel_idx
    flip = False
    for i, page in enumerate(tiff.series[0].pages):
        if i >= start and i < start + z_pages_per_channel:
            if flip:
                p = tiff.series[0].pages[start+list(range(z_pages_per_channel-1, -1, -1))[i-start]]
                if not p:
                    yield empty_page
                else:
                    yield p.asarray().T
            else:
                if not page:
                    yield empty_page
                else:
                    yield page.asarray().T
        if i == start + z_pages_per_channel - 1:
            start += z_pages_per_channel * channel_amount
            if bidirZ:
                flip = not flip

# convert from ome tiff to nii, with minimum dependency on bruker XML file
@timing_decorator
def convert_to_nii(xml_path: Path):
    companion = next(xml_path.parent.glob("*.companion.ome"), None)
    if companion:
        companion = open(companion, 'r').read()
    # find the main tiff with overall information
    channel_info = parse_acquisition_channel_info(xml_path)
    tiff_path = next(xml_path.parent.glob(f"*Cycle00001_Ch{channel_info[0]}_000001.ome.tif"), None)
    tiff = tifffile.TiffFile(tiff_path, omexml=companion)
    logger.info("axes from tifffile: %s", str(tiff.series[0].axes))
    logger.info("shape from tifffile: %s", str(tiff.series[0].shape))
    assert len(tiff.series) == 1

    #NOTE: volume-series is XYZCT, time-series is XYCT
    #NOTE: a transpose is introduced by image to numpy
    assert tiff.series[0].axes in ("TCZYX", "TZYX", "TCYX", "TYX", "CZYX", "ZYX")
    tiff_shape = tiff.series[0].shape #tuple
    shape_without_channel = list(tiff_shape)
    if tiff.series[0].axes == "TCZYX":
        pages_per_channel = tiff_shape[2]
        channel_amount = tiff_shape[1]
        shape_without_channel.pop(1)
        #NOTE: if this is aborted during acquisition, discard last incomplete Z sequence
        valid_page = 0
        for i, page in enumerate(tiff.series[0].pages):
            if page:
                valid_page += 1
            else:
                break
        if len(tiff.series[0].pages) != valid_page:
            shape_without_channel[0] = shape_without_channel[0] - 1
    if tiff.series[0].axes == "TZYX":
        pages_per_channel = tiff_shape[1]
        channel_amount = 1
        #NOTE: if this is aborted during acquisition, discard last incomplete Z sequence
        valid_page = 0
        for i, page in enumerate(tiff.series[0].pages):
            if page:
                valid_page += 1
            else:
                break
        if len(tiff.series[0].pages) != valid_page:
            shape_without_channel[0] = shape_without_channel[0] - 1
    elif tiff.series[0].axes == "TCYX":
        pages_per_channel = 1
        channel_amount = tiff_shape[1]
        shape_without_channel.pop(1)
    elif tiff.series[0].axes == "TYX":
        pages_per_channel = 1
        channel_amount = 1
    elif tiff.series[0].axes == "CZYX":
        pages_per_channel = tiff_shape[1]
        channel_amount = tiff_shape[0]
        shape_without_channel.pop(0)
    elif tiff.series[0].axes == "ZYX":
        pages_per_channel = tiff_shape[0]
        channel_amount = 1
    #suppress warning caused by not loading the entire dataset to memory
    warnings.filterwarnings("ignore", message=".*reading array from closed file.*", category=UserWarning)
    for channel_idx in range(channel_amount):
        bidirZ = parse_acquisition_is_bidirectional(xml_path)
        frame_gen = general_frame_gen(tiff, channel_idx, pages_per_channel, channel_amount, bidirZ)
        acq_path = xml_path.parent
        output_path = (
            acq_path / f"{acq_path.name}_channel_{channel_info[channel_idx]}.nii"
        )
        header = create_acquisition_nifti_header(xml_path, shape_without_channel[::-1])
        write_nifti_streaming(header, frame_gen, output_path)
    warnings.resetwarnings()
    tiff.close()

def parse_acquisition_is_bidirectional(xml_path: Path) -> bool:
    """Determines whether acquisition volumes were acquired bidirectionally in z. Only relevant to volumes.

    Checks only a single Sequence element.
    """
    acq_root = ElementTree.parse(xml_path).getroot()
    sequence = acq_root.find("./Sequence")
    if "bidirectionalZ" in sequence.attrib:
      return sequence.attrib["bidirectionalZ"] == "True"
    else:
      return False

def parse_acquisition_channel_info(xml_path: Path):
    acq_root = ElementTree.parse(xml_path).getroot()
    ch_files = acq_root.find("./Sequence/Frame").findall("./File")
    channel_info = []
    for ch_file in ch_files:
        channel_info.append(int(ch_file.attrib["channel"]))
    channel_info.sort()
    return channel_info

def create_acquisition_nifti_header(xml_path: Path, acq_shape) -> nib.nifti2.Nifti2Header:

    # NOTE: nifti2 is required as some fields in the nifti1 header are not large
    # enough. in particular, the data shape field is a short and it is very
    # easy to have an image with a dimension larger than 32k in practice.
    header = nib.nifti2.Nifti2Header()
    header.set_data_dtype(np.uint16)

    # NOTE: nifti (or at least nibabel) expects Fortran style column-major
    # order for the data block, but evidently expects C style row-major order for the shape
    header.set_data_shape(acq_shape)

    header.set_sform(np.eye(4))

    # NOTE: NIfTI supports data scaling, which nibabel uses to maximize
    # precision. We have no use for this since we're saving 13-bit data as uint16s.
    # TODO: worthwhile to check the .env and confirm this
    assert header.get_slope_inter() == (1.0, 0.0)

    return header

@contextmanager
def guarded_output_file(output_path):
    """Context manager that provides a temp path to write to, which it renames
    to the desired output_path if the context is exited normally. The
    guarded/temp output file is deleted if any exceptions are encountered with
    the context
    """
    # write to a temp output path and copy tp final destination only when writing succesfully completes, to avoid leaving partial files if unexpectedly killed
    tmp_output_path = output_path.parent / str(uuid.uuid4())
    logger.info("Temp output path for %s: %s ", output_path, tmp_output_path)

    try:
        yield tmp_output_path
        tmp_output_path.rename(output_path)
    except Exception as exc:
        logger.info(
            "Removing guarded output for %s due. Exception caught within context.",
            output_path,
        )
        tmp_output_path.unlink(missing_ok=True)
        raise exc

def write_nifti_streaming(
    header: nib.nifti2.Nifti2Header, frame_gen, output_path: Path
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

