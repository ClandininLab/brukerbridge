""" Conversion logic for PrairieView 5.8
"""
import logging
from pathlib import Path
from typing import Dict, Iterator, Tuple, Union
from xml.etree import ElementTree

import nibabel as nib
import numpy as np
from numpy.typing import NDArray
from PIL import Image

from brukerbridge.constants import AcquisitionType, TiffPageFormat
from brukerbridge.io import write_nifti_streaming
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


def convert_acquisition_to_nifti(xml_path: Path):
    # loop over channels
    # dispatch to frame gen for acquisition types
    # write some metadata?

    acq_type = parse_acquisition_type(xml_path)
    channel_info = parse_acquisition_channel_info(xml_path)
    acq_shape, force_terminated = parse_acquisition_shape(xml_path)

    if force_terminated:
        assert acq_type == AcquisitionType.VOL_SERIES
        logger.warning(
            "Volume series %s was force terminated and will be truncated to last complete volume.",
            xml_path,
        )

    # could parallelize over channels at expense of adding significant complexity to bridge
    for channel_idx, channel_name in channel_info.items():
        logger.info(
            "Converting channel %s: %s of %s",
            channel_idx,
            channel_name,
            format_acq_path(xml_path.parent),
        )

        # silence type hinting on acq_shape. despite writing an insane type
        # declaration for parse_acquisition_shape the static analyzer is not
        # capable of inferring that the AcquisitionType enum enforces the type
        # of acq_shape
        if acq_type == AcquisitionType.VOL_SERIES:
            frame_gen = vol_series_frame_gen(
                xml_path, channel_idx, acq_shape  # type: ignore
            )

        elif acq_type == AcquisitionType.PLANE_SERIES:
            frame_gen = plane_series_frame_gen(
                xml_path, channel_idx, acq_shape  # type: ignore
            )
        # single image
        else:
            raise NotImplementedError("Conversion of single images is not supported.")

        # create a new header object here out of an abundance caution that
        # nibabel might be doing something stateful. it's not very expensive
        header = create_acquisition_nifti_header(xml_path)

        acq_path = xml_path.parent
        output_path = acq_path / f"{acq_path.name}_channel_{channel_idx}.nii"

        write_nifti_streaming(header, frame_gen, output_path)


# TODO: record more metadata in the header. resolution in particular
def create_acquisition_nifti_header(xml_path: Path) -> nib.nifti1.Nifti1Header:
    acq_shape, _ = parse_acquisition_shape(xml_path)

    # NOTE: AB 2025/05/29
    # We'll stick to Nifti1 because the main changes to Nifti2 are switching
    # some data types from 32 bit to 64 bit which we do not need, but msotly
    # because I have only tested against Nifti1.
    header = nib.nifti1.Nifti1Header()
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


def vol_series_frame_gen(
    xml_path: Path, channel: int, acq_shape: Tuple[int, int, int, int]
) -> Iterator[NDArray]:
    if parse_acquisition_is_bidirectional(xml_path):
        # NOTE: AB 2024/08/06, PV5.5
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
        # the order of the frames every other volume/Sequence. Definitely cowboy shit,
        # but this is what you would want to do to naively support
        # bidirectional scans: take the *sorted* frames and reverse the order
        # the list is traversed by the generator for Sequences with an even
        # cycle attribute
        # NOTE AB 2025/05/29: this continues to be true for PV5.8
        raise NotImplementedError(
            (
                "Support for bidirectional scans not supported due to Bruker sketchiness. "
                "See the source where this error was thrown for an explanation."
            )
        )

    tiff_page_format = parse_acquisition_tiff_page_format(xml_path)

    acq_root = ElementTree.parse(xml_path).getroot()

    sequence_elements = acq_root.findall("./Sequence")
    assert len(sequence_elements) == acq_shape[3]

    # ordering of elements within an xml file is not guaranteed.
    # cycle attribute is assumed to give ordering of sequences within an acquisition
    sequence_elements = sorted(
        sequence_elements, key=lambda sequence: int(sequence.attrib["cycle"])
    )

    for sequence_element in sequence_elements:
        frame_elements = sequence_element.findall("./Frame")

        # index attribute is assumed to give ordering of frames within a sequence
        frame_elements = sorted(
            frame_elements, key=lambda frame: int(frame.attrib["index"])
        )

        # parse_acquisition_shape only checks the last couple of Sequences for this condition
        assert len(frame_elements) == acq_shape[2]

        if tiff_page_format == TiffPageFormat.SINGLE_PAGE:
            for frame_element in frame_elements:
                # relative filesystem path to frame image
                frame_path = frame_element.find(f"./File[@channel='{channel}']").attrib["filename"]  # type: ignore

                with Image.open(xml_path.parent / frame_path) as frame_img:
                    frame_img_arr = np.array(frame_img)
                    assert frame_img_arr.shape == acq_shape[:2]
                    assert frame_img_arr.dtype == np.uint16

                    yield frame_img_arr
        # multi-page
        else:
            # NOTE: AB 2025/05/29
            # while we could simply iterate through frame_elements, open a new
            # image file for each and seek directly to the corresponding page,
            # that will be fairly expensive for large tiffs with many pages. this
            # significantly more complicated logic implements the optimization
            # of opening new images and seeking only when necessary, and
            # handles the possibility that there might be more than one tiff
            # per volume/Sequence (I do not expect this to occur in practice,
            # but again I am only guessing at the spec so I think its wise to
            # handle this case)
            frame_idx = 0
            # File element for frame with frame_idx and for channel index channel
            frame_file_element = frame_elements[frame_idx].find(
                f"./File[@channel='{channel}']"
            )

            while frame_idx < len(frame_elements):
                img_path = frame_file_element.attrib["filename"]  # type: ignore
                with Image.open(xml_path.parent / img_path) as frame_img:
                    # iterate and emit frames for each page in this tiff
                    while frame_file_element.attrib["filename"] == img_path:  # type: ignore
                        frame_page = int(frame_file_element.attrib["page"])  # type: ignore

                        # xml pages are one-indexed, pillow uses zero indexing
                        frame_img.seek(frame_page - 1)

                        frame_img_arr = np.array(frame_img)
                        assert frame_img_arr.shape == acq_shape[:2]
                        assert frame_img_arr.dtype == np.uint16

                        yield frame_img_arr

                        frame_idx += 1

                        if frame_idx < len(frame_elements):
                            frame_file_element = frame_elements[frame_idx].find(
                                f"./File[@channel='{channel}']"
                            )
                        else:
                            break


def plane_series_frame_gen(
    xml_path: Path, channel: int, acq_shape: Tuple[int, int, int]
) -> Iterator[NDArray]:

    tiff_page_format = parse_acquisition_tiff_page_format(xml_path)

    acq_root = ElementTree.parse(xml_path).getroot()

    frame_elements = acq_root.findall("./Sequence/Frame")

    # parsed xml order is unreliable so the index attrib of Frames is assumed
    # to give ordering. again, we're just guessing at the spec here
    frame_elements = sorted(
        frame_elements, key=lambda frame: int(frame.attrib["index"])
    )
    assert len(frame_elements) == acq_shape[2]

    if tiff_page_format == TiffPageFormat.SINGLE_PAGE:
        # relative filesystem path to frame image
        frame_path = frame_rec.find(f"./File[@channel='{channel}']").attrib["filename"]  # type: ignore

        with Image.open(xml_path.parent / frame_path) as frame_img:
            frame_img_arr = np.array(frame_img)
            assert frame_img_arr.shape == acq_shape[:2]
            assert frame_img_arr.dtype == np.uint16

            yield frame_img_arr
    # multi-page
    else:
        frame_idx = 0
        # File element for frame with frame_idx and for channel index channel
        frame_file_element = frame_elements[frame_idx].find(
            f"./File[@channel='{channel}']"
        )

        while frame_idx < len(frame_elements):
            img_path = frame_file_element.attrib["filename"]  # type: ignore
            with Image.open(xml_path.parent / img_path) as frame_img:
                # iterate and emit frames for each page in this tiff
                while frame_file_element.attrib["filename"] == img_path:  # type: ignore
                    frame_page = int(frame_file_element.attrib["page"])  # type: ignore

                    # xml pages are one-indexed, pillow uses zero indexing
                    frame_img.seek(frame_page - 1)

                    frame_img_arr = np.array(frame_img)
                    assert frame_img_arr.shape == acq_shape[:2]
                    assert frame_img_arr.dtype == np.uint16

                    yield frame_img_arr

                    frame_idx += 1

                    if frame_idx < len(frame_elements):
                        frame_file_element = frame_elements[frame_idx].find(
                            f"./File[@channel='{channel}']"
                        )
                    else:
                        break
