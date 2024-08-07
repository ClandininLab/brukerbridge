import logging
from pathlib import Path
from typing import Dict, Tuple, Union
from xml.etree import ElementTree

import nibabel as nib
import numpy as np
from PIL import Image

from brukerbridge.utils import format_acq_path

logger = logging.getLogger(__name__)

# NOTE: berger 8/6/24
# I do not have a schema from Bruker so all of the assumptions about the
# structure of acquistion xmls here are really just educated guesses

AXIS_NAMES = ["XAxis", "YAxis", "ZAxis"]


# type hinting getting a little ridiculous.. get in boys, we're writing java
def parse_acquisition_shape(
    xml_path: Path,
) -> Union[Tuple[int, int, int], Tuple[int, int, int, int]]:
    """Parses shape of acquisition from acquisition xml

    More or less assumes that this is a well-formed acquisition xml

    Returns:
      acq_shape: (n_x, n_y, n_t) or (n_x, n_y, n_z, n_t) for single plane and volume acquisitions respectively
    """
    acq_root = ElementTree.parse(xml_path).getroot()

    n_x = int(
        acq_root.find("./PVStateShard/PVStateValue[@key='pixelsPerLine']").attrib[  # type: ignore
            "value"
        ]
    )
    n_y = int(
        acq_root.find("./PVStateShard/PVStateValue[@key='linesPerFrame']").attrib[  # type: ignore
            "value"
        ]
    )

    seqs = acq_root.findall("Sequence")
    assert len(seqs) >= 1

    seq_type = seqs[0].get("type")

    # single plane acquisition
    if seq_type == "TSeries Timed Element":
        # single plane acquisitions appear to have only a single Sequence which
        # contains one Frame for each plane
        assert len(seqs) == 1
        n_t = len(seqs[0].findall("Frame"))

        return (n_x, n_y, n_t)

    # volume acquisition
    elif seq_type == "TSeries ZSeries Element":
        # volume acquisitions appear to have a Sequence for each volume, each
        # of which contains a Frame for every plane in the volume
        n_t = len(seqs)
        n_z = len(seqs[0].findall("Frame"))

        return (n_x, n_y, n_z, n_t)
    else:
        raise RuntimeError(f"Unsupported Sequence type: '{seq_type}'")


def parse_acquisition_channel_info(xml_path: Path) -> Dict[int, str]:
    """Parses index and name of channels from acquisition xml

    Assumes that this is a well-formed acquisition xml and (implicitly) that the number of
    channels is fixed throughout the acquisition

    Channels are one indexed. For most acquisitions channel_info will be

    {
        1: "Red",
        2: "Green"
    }
    """
    acq_root = ElementTree.parse(xml_path).getroot()

    # raises attribute error on malformed xmls
    ch_files = acq_root.find("Sequence").find("Frame").findall("File")  # type: ignore

    channel_info = {}
    for ch_file in ch_files:
        channel_info[int(ch_file.attrib["channel"])] = ch_file.attrib["channelName"]

    return channel_info


def parse_acquisition_resolution(xml_path: Path) -> Tuple[float, float, float]:
    """Extract resolution from acquisition xml. Assumes well-behaved xml

    Returns:
      resolution: (x_res, y_res, z_res), microns
    """
    acq_root = ElementTree.parse(xml_path).getroot()

    resolution = []

    for axis_name in AXIS_NAMES:
        res_rec = acq_root.find(
            f"./PVStateShard/PVStateValue[@key='micronsPerPixel']/IndexedValue[@index='{axis_name}']"
        ).attrib[
            "value"
        ]  # type: ignore

        resolution.append(float(res_rec))

    return tuple(resolution)


def single_plane_acquisition_frame_gen(xml_path: Path, channel: int):
    acq_path = xml_path.parent
    acq_root = ElementTree.parse(xml_path).getroot()

    # (n_x, n_y, n_t)
    acq_shape = parse_acquisition_shape(xml_path)
    assert len(acq_shape) == 3

    acq_channel_info = parse_acquisition_channel_info(xml_path)
    assert channel in acq_channel_info

    frames = acq_root.findall("./Sequence/Frame")

    # parsed xml order is unreliable so the index attrib of Frames is assumed
    # to give ordering. again, we're just guessing at the spec here
    frames = sorted(frames, key=lambda frame: int(frame.attrib["index"]))
    assert len(frames) == acq_shape[2]

    for frame_rec in frames:
        # relative
        frame_path = frame_rec.find(f"./File[@channel='{channel}']").attrib["filename"]  # type: ignore

        frame = np.array(Image.open(acq_path / frame_path))
        assert frame.shape == acq_shape[:2]
        assert frame.dtype == np.uint16

        yield frame


def volume_acquisition_frame_gen(xml_path: Path, channel: int):
    acq_path = xml_path.parent
    acq_root = ElementTree.parse(xml_path).getroot()

    # (n_x, n_y, n_z, n_t)
    acq_shape = parse_acquisition_shape(xml_path)
    assert len(acq_shape) == 4

    acq_channel_info = parse_acquisition_channel_info(xml_path)
    assert channel in acq_channel_info

    sequences = acq_root.findall("./Sequence")
    bidirectional = sequences[0].attrib["bidirectionalZ"] == "True"

    if bidirectional:
        # NOTE: berger 2024/08/06
        # Although support for this could be easily cheesed, I have declined to
        # do so for the moment due to some mysteries in the acquisition xml
        # that I am not confident enough to guess at right now. Specifically, the
        # subtrees for the last frame of the downstroke and the first frame of
        # the upstroke do not record the depth at which those frames were
        # acquired. All other frames do.
        #
        # In the example acquisition I was using to develop this, the user set
        # the bottom plane as 100.5um, the top as 340.5 and set the volume to
        # contain 49 planes with 5um increments. Each Sequence subtree indeed
        # contains 49 frames, but (for the downstroke) the 49th does not record
        # z pos. The 48th records a z pos of 335.5. It would not be
        # unreasonable to infer that the 49th plane was at z=340.5um, but that
        # is, ultimately, cowboy shit.
        #
        # In the past, Bella supported bidirectional z scans by simply flipping
        # the order of the frames every other volume. Definitely cowboy shit,
        # but this is what you would want to do to naively support
        # bidirectional scans: take the *sorted* frames and reverse the order
        # the list is traversed by the generator for Sequences with an even
        # cycle attribute
        raise NotImplementedError(
            (
                "Support for bidirectional scans not supported due to Bruker sketchiness. "
                "See the source where this error was thrown for an explanation."
            )
        )

    # cycle attribute is assumed to give ordering of sequences
    sequences = sorted(sequences, key=lambda sequence: int(sequence.attrib["cycle"]))

    for vol_idx, sequence_rec in enumerate(sequences):
        frames = sequence_rec.findall("./Frame")

        # index attribute again assumed to give ordering within a sequence
        frames = sorted(frames, key=lambda frame: int(frame.attrib["index"]))

        if len(frames) < acq_shape[2]:
            logger.warning(
                "%s: volume %s of %s had %s planes of expected %s. Discarding remaining volumes",
                format_acq_path(acq_path),
                vol_idx,
                acq_shape[3],
                len(frames),
                acq_shape[2],
            )
            break

        for frame_rec in frames:
            # relative
            frame_path = frame_rec.find(f"./File[@channel='{channel}']").attrib["filename"]  # type: ignore

            frame = np.array(Image.open(acq_path / frame_path))
            assert frame.shape == acq_shape[:2]
            assert frame.dtype == np.uint16

            yield frame


def write_nifti(xml_file: Path, channel: int, legacy: bool):
    acq_path = xml_file.parent
    output_path = acq_path / f"{acq_path.name}_channel_{channel}.nii"

    acq_shape = parse_acquisition_shape(xml_file)

    if len(acq_shape) == 3:
        frame_gen = single_plane_acquisition_frame_gen(xml_file, channel)
    else:
        frame_gen = volume_acquisition_frame_gen(xml_file, channel)

    hdr = nib.nifti1.Nifti1Header()
    hdr.set_data_dtype(np.uint16)
    # NOTE: nifti (or at least nibabel) expects Fortran style column-major
    # order for the data block. We write our frames in a tranposed row-major
    # order, so this is fine
    hdr.set_data_shape(acq_shape)

    hdr.set_sform(np.eye(4))

    # released from the shackles of backwards compatibility, we can add some useful to the header
    # much of this turns out to be rather important for getting to registration
    if not legacy:
        # TODO: double pixdim[0], orientation info
        resolution = parse_acquisition_resolution(xml_file)
