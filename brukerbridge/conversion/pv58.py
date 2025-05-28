""" Conversion logic for PrairieView 5.8
"""
import logging
from pathlib import Path
from typing import Dict, Tuple, Union
from xml.etree import ElementTree

import nibabel as nib
import numpy as np
from PIL import Image

from brukerbridge.conversion.common import AcquisitionType, TiffPageFormat
from brukerbridge.utils import format_acq_path

logger = logging.getLogger(__name__)

# NOTE: berger 8/6/24
# I do not have a schema from Bruker so all of the assumptions about the
# structure of acquistion xmls here are really just educated guesses

# this consant is overloaded in other modules, be careful you don't import the wrong one
SUPPORTED_PRAIREVIEW_VERSION = "5.8.64.800"

AXIS_NAMES = ["XAxis", "YAxis", "ZAxis"]

# TODO: logging
def parse_acquisition_type(xml_path: Path) -> AcquisitionType:
    """Parses acquisition type (Sequence type, technically) into enum encoding its allowed values

    Makes the crucial assumption that acquisition/sequence type does NOT change
    during the course of an acquisition, which is something you could actually
    do with PraireView I think
    """
    acq_root = ElementTree.parse(xml_path).getroot()

    seq_type = acq_root.find("./Sequence").attrib["type"]  # type: ignore

    if seq_type == "TSeries ZSeries Element":
        return AcquisitionType.VOL_SERIES
    elif seq_type == "TSeries Timed Element":
        return AcquisitionType.PLANE_SERIES
    elif seq_type == "Single":
        return AcquisitionType.SINGLE_IMAGE
    else:
        raise AssertionError(f"Unknown acquisition type: {seq_type}")


def parse_acquisition_is_bidirectional(xml_path: Path) -> bool:
    """Determines whether acquisition volumes were acquired bidirectionally in z. Only relevant to volumes.

    Checks only a single Sequence element.
    """
    acq_root = ElementTree.parse(xml_path).getroot()

    if parse_acquisition_type(xml_path) != AcquisitionType.VOL_SERIES:
        raise RuntimeError("Only volume series have a notion of bidirectional z scans")

    sequence = acq_root.find("./Sequence")
    return sequence.attrib["bidirectionalZ"] == "True"  # type: ignore


def parse_acquisition_tiff_page_format(xml_path: Path) -> TiffPageFormat:
    """Reads off tiff page format from env xml."""
    # find corresponding env path
    env_path = xml_path.with_suffix(".env")

    if not env_path.exists():
        return parse_acquisition_tiff_page_format_fallback(xml_path)

    env_root = ElementTree.parse(env_path).getroot()

    tiff_page_format_rec = env_root.find(
        "./PVStateShard/PVStateValue[@key='saveAsMultipageTIFF']"
    )

    if tiff_page_format_rec is not None:
        if tiff_page_format_rec.attrib["value"] == "True":
            return TiffPageFormat.MULTI_PAGE
        else:
            return TiffPageFormat.SINGLE_PAGE
    else:
        return parse_acquisition_tiff_page_format_fallback(xml_path)

    # key is saveAsMultipageTIFF
    # PVStateShard/PVStateValue


# NOTE: this may be unnecessary but I had already written it when I discovered
# you could read off the setting from the env file, so it stays
def parse_acquisition_tiff_page_format_fallback(xml_path: Path) -> TiffPageFormat:
    """Fallback strategy that does some educated guess work to figure out whether this acquisition uses single or multi-page tiffs. Ideally one just reads the value off the .env file

    There is no global setting for this, so it reads some frames in an
    acquisition type dependent way and checks to see whether each frame has a
    unique filename attrib/whether the page attrib is ever incremented.

    Also, only looks at one channel.
    """
    acq_root = ElementTree.parse(xml_path).getroot()

    acq_type = parse_acquisition_type(xml_path)

    if acq_type == AcquisitionType.SINGLE_IMAGE:
        # definitional
        return TiffPageFormat.SINGLE_PAGE
    elif acq_type == AcquisitionType.VOL_SERIES:
        # pick channel value to inspect arbitrarily, by taking the value of first file element
        channel = acq_root.find("./Sequence/Frame/File").attrib["channel"]  # type: ignore

        inferred_multi_page = []

        # one sequence would probably be enough, but we'll look at four just to be safe
        # xml elements are one-indexed
        for seq_idx in range(1, 5):
            seq = acq_root.find(f"./Sequence[{seq_idx}]")

            page_vals = set()
            filename_vals = set()

            seq_files = seq.findall(f"./Frame/File[@channel='{channel}']")  # type: ignore
            for frame_file in seq_files:
                page_vals.add(frame_file.attrib["page"])
                filename_vals.add(frame_file.attrib["filename"])

            if len(seq_files) == 1:
                raise RuntimeError(
                    "Cannot infer tiff page format from a volume series with only one frame"
                )

            if len(page_vals) > 1:
                inferred_multi_page.append(True)

                # product of cardinality of page vals and filename vals should upper bound number of frames
                assert len(page_vals) * len(filename_vals) >= len(seq_files)
            else:
                inferred_multi_page.append(False)

        # this would only be false if one Sequence element in an acquisition can be
        # single paged and another multi. note that this sort of defeats the
        # point of checking multiple sequences in that it will be raised if
        # ever they give different results, however I must again emphasize that
        # I do not have a schema from bruker and am just guessing, so its good
        # to check assumptions
        assert any(inferred_multi_page) == all(inferred_multi_page)

        if all(inferred_multi_page):
            return TiffPageFormat.MULTI_PAGE
        else:
            return TiffPageFormat.SINGLE_PAGE

    else:
        assert acq_type == AcquisitionType.PLANE_SERIES

        # NOTE: single plane acquisitions have a single Sequence element (I
        # think) and can have many thousands of pages in a single multi-page
        # tiff. We will just check 128 frames

        # again pick channel value to inspect arbitrarily, by taking the value of first file element
        channel = acq_root.find("./Sequence/Frame/File").attrib["channel"]  # type: ignore

        page_vals = set()
        filename_vals = set()

        frame_idx = 1
        frame_itr = acq_root.iterfind(f"./Sequence/Frame/File[@channel='{channel}']")
        while frame_idx < 129:
            try:
                frame_file = next(frame_itr)
            except StopIteration:
                break

            page_vals.add(frame_file.attrib["page"])  # type: ignore
            filename_vals.add(frame_file.attrib["filename"])  # type: ignore

            frame_idx += 1

        if frame_idx == 1:
            raise RuntimeError(
                "Cannot infer tiff page format from a plane series with only one frame"
            )

        if len(page_vals) > 1:
            # again, product of cardinality of page vals and filename vals should upper bound number of frames
            assert len(page_vals) * len(filename_vals) >= frame_idx - 1
            return TiffPageFormat.MULTI_PAGE
        else:
            return TiffPageFormat.SINGLE_PAGE


# type hinting getting a little ridiculous.. get in boys, we're writing java
def parse_acquisition_shape(
    xml_path: Path,
) -> Tuple[
    Union[Tuple[int, int], Tuple[int, int, int], Tuple[int, int, int, int]], bool
]:
    """Parses shape of acquisition from acquisition xml. Also returns info on whether acquisition was force terminated since this method necessarily contains logic to detect that anyway.

    Returns:
      acq_shape: (n_x, n_y, n_t) or (n_x, n_y, n_z, n_t) for single plane and volume acquisitions respectively
      force_terminated: true if acquisition was force terminated. only defined for volume acquisitions
    """
    acq_root = ElementTree.parse(xml_path).getroot()

    acq_type = parse_acquisition_type(xml_path)

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

    sequences = acq_root.findall("Sequence")
    assert len(sequences) >= 1

    if acq_type == AcquisitionType.VOL_SERIES:
        # sequence ordering as read is arbitrary, cycle attribute is assumed to give ordering of sequences
        # its necessary to have oredered sequences for vol series to check last volume, but not plane series
        sequences = sorted(
            sequences, key=lambda sequence: int(sequence.attrib["cycle"])
        )

        n_t = len(sequences)

        if not n_t == 1:
            # if the scan was force terminated, last volume scan could be
            # incomplete the following logic checks for this. note that we are
            # unable to detect "frame perfect" incomplete scans that completed
            # the last volume and then stopp

            n_z_last = len(sequences[-1].findall("Frame"))

            # get base case n_z, checking a few vols just be sure
            vols_to_inspect = min(4, len(sequences) - 1)
            n_zs = [
                len(sequences[idx].findall("Frame")) for idx in range(vols_to_inspect)
            ]

            # check assumption that all vols besides the last have the same number of planes
            assert all([vol_z == n_zs[0] for vol_z in n_zs])

            n_z = n_zs[0]

            if n_z_last != n_z:
                logger.info("Volume acquisition %s was force terminated.", xml_path)

                return (n_x, n_y, n_z, n_t - 1), True
        else:
            # debated raising warning over this but could imagine a scenario
            # where you'd do this, and the only problem here is that we can't
            # determine if the acquisition completed succesfully. which we
            # can't do for all plane series anyways so I guess that's fine
            logger.debug("Volume acquisition %s has a single timepoint.", xml_path)
            n_z = len(sequences[0].findall("Frame"))

        return (n_x, n_y, n_z, n_t), False
    elif acq_type == AcquisitionType.PLANE_SERIES:
        # single plane acquisitions appear to have only a single Sequence which
        # contains one Frame for each plane
        assert len(sequences) == 1
        n_t = len(sequences[0].findall("Frame"))

        return (n_x, n_y, n_t), False
    # single image
    else:
        return (n_x, n_y), False


def parse_acquisition_channel_info(xml_path: Path) -> Dict[int, str]:
    """Parses index and name of channels from acquisition xml

    Assumes that this is a well-formed acquisition xml and (implicitly) that the number of
    channels is fixed throughout the acquisition

    Channels are one indexed. For most acquisitions channel_info will be

    {
        1: "Red",
        2: "Green"
    }

    Only inspects one frame of one sequence
    """
    acq_root = ElementTree.parse(xml_path).getroot()

    # raises attribute error on malformed xmls
    ch_files = acq_root.find("./Sequence/Frame").findall("./File")  # type: ignore

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
        ).attrib[  # type: ignore
            "value"
        ]

        resolution.append(float(res_rec))

    return tuple(resolution)


def single_plane_acquisition_frame_gen(xml_path: Path, channel: int):
    """Generator over frames (in time) for ripped Bruker data"""
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
    """Generator over frames (in z and time) for ripped Bruker data"""
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


# TODO: deprecate
def write_nifti(xml_path: Path, channel: int):
    acq_path = xml_path.parent
    output_path = acq_path / f"{acq_path.name}_channel_{channel}.nii"

    acq_shape = parse_acquisition_shape(xml_path)

    if len(acq_shape) == 3:
        frames = single_plane_acquisition_frame_gen(xml_path, channel)
    else:
        frames = volume_acquisition_frame_gen(xml_path, channel)

    hdr = nib.nifti1.Nifti1Header()
    hdr.set_data_dtype(np.uint16)
    # NOTE: nifti (or at least nibabel) expects Fortran style column-major
    # order for the data block, but evidently expects C style row-major order for the shape
    hdr.set_data_shape(acq_shape)

    hdr.set_sform(np.eye(4))

    # NOTE: NIfTI supports data scaling, which nibabel uses to maximize
    # precision. We have no use for this since we're saving 13-bit data as uint16s.
    assert hdr.get_slope_inter() == (1.0, 0.0)

    with open(output_path, "wb") as img_fh:
        hdr.write_to(img_fh)

        img_fh.seek(hdr.get_data_offset())
        assert img_fh.tell() == hdr.get_data_offset()

        for frame in frames:
            # NOTE: the net result of this transposition and the frame
            # generators is Fortran style column-major order
            for slc in frame.T:
                img_fh.write(slc.tobytes())
