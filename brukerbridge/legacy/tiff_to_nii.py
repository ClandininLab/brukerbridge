import gzip as gzip_module
import logging
import os
import shutil
from xml.etree import ElementTree as ET

import nibabel as nib
import numpy as np
from PIL import Image
from skimage import io

logger = logging.getLogger(__name__)

# NIfTI-1: 348 byte header + 4 byte extension flag
NII_VOX_OFFSET = 352


def tiff_to_nii(xml_file: str, gzip: bool = False):
    aborted = False
    data_dir, _ = os.path.split(xml_file)

    # Check if multipage tiff files
    companion_filepath = xml_file.split(".")[0] + ".companion.ome"
    if os.path.exists(companion_filepath):
        isMultiPageTiff = True
    else:
        isMultiPageTiff = False
    logger.debug("%s, isMultiPageTiff: %s", xml_file, isMultiPageTiff)

    tree = ET.parse(xml_file)
    root = tree.getroot()
    # Get all volumes
    sequences = root.findall("Sequence")

    # Check if bidirectional - will affect loading order
    isBidirectionalZ = sequences[0].get("bidirectionalZ")
    if isBidirectionalZ == "True":
        isBidirectionalZ = True
    else:
        isBidirectionalZ = False
    logger.debug("%s, BidirectionalZ: %s", xml_file, isBidirectionalZ)

    # Get axis dims
    if (
        root.find("Sequence").get("type") == "TSeries Timed Element"  # type: ignore
    ):  # Plane time series
        num_timepoints = len(sequences[0].findall("Frame"))
        num_z = 1
        isVolumeSeries = False
    elif (
        root.find("Sequence").get("type") == "TSeries ZSeries Element"  # type: ignore
    ):  # Volume time series
        num_timepoints = len(sequences)
        num_z = len(sequences[0].findall("Frame"))
        isVolumeSeries = True
    else:  # Default to: Volume time series
        num_timepoints = len(sequences)
        num_z = len(sequences[0].findall("Frame"))
        isVolumeSeries = True
    logger.debug("%s, isVolumeSeries: %s", xml_file, isVolumeSeries)

    num_channels = get_num_channels(sequences[0])
    test_file = sequences[0].findall("Frame")[0].findall("File")[0].get("filename")
    fullfile = os.path.join(data_dir, test_file)  # type: ignore

    ### Luke added try except 20221024 because sometimes but rarely a file doesn't exist
    # somthing to do with bruker xml file
    #
    # berger 2024/7/18: removed exception handling, this should raise a fatal
    # error since and would have at the next unbound reference to `img` anyways
    img = imread_stack(fullfile)

    num_y = np.shape(img)[-2]
    num_x = np.shape(img)[-1]
    logger.debug(
        "%s, num_channels: %s, num_timepoints: %s, num_z: %s, num_y: %s, num_x: %s",
        xml_file,
        num_channels,
        num_timepoints,
        num_z,
        num_y,
        num_x,
    )

    # Handle aborted scans for volumes: a volume whose number of z-slices
    # differs from the previous one marks an aborted acquisition. Detected up
    # front so the output can be allocated at its final size; as before, data
    # stops at the first inconsistent volume and the last volume is discarded.
    first_bad_volume = num_timepoints
    if isVolumeSeries:
        for i in range(1, num_timepoints):
            if len(sequences[i].findall("Frame")) != len(
                sequences[i - 1].findall("Frame")
            ):
                logger.warning(
                    "%s: this acquisition was aborted, discarding last volume",
                    xml_file,
                )
                aborted = True
                first_bad_volume = i
                break

    # loop over channels
    for channel in range(num_channels):
        save_name = xml_file[:-4] + "_channel_{}".format(channel + 1) + ".nii"

        if isVolumeSeries:
            # x, y, z, t (the per-volume transposes below map z, y, x -> x, y, z)
            num_volumes = num_timepoints - 1 if aborted else num_timepoints
            shape = (num_x, num_y, num_z, num_volumes)
            logger.debug("%s, final array shape: %s", xml_file, shape)
            logger.debug("%s, saving nii as %s", xml_file, save_name)

            # stream volumes straight to disk so peak memory is one volume
            with open(save_name, "wb") as f:
                f.truncate(NII_VOX_OFFSET + int(np.prod(shape)) * 2)
            mm = np.memmap(
                save_name,
                dtype=np.uint16,
                mode="r+",
                offset=NII_VOX_OFFSET,
                shape=shape,
                order="F",
            )
            for i in range(min(first_bad_volume, shape[-1])):
                frames = sequences[i].findall("Frame")

                # Flip frame order if a bidirectionalZ upstroke (odd i)
                if isBidirectionalZ and (i % 2 != 0):
                    frames = frames[::-1]

                if isMultiPageTiff:
                    files = frames[0].findall("File")
                    filename = files[channel].get("filename")
                    fullfile = os.path.join(data_dir, filename)  # type: ignore
                    volume = imread_stack(fullfile)  # shape = z, y, x
                    # all frames point to the same file, so reordering frames
                    # above is a no-op; pages are in acquisition order, so
                    # flip z on upstrokes
                    if isBidirectionalZ and (i % 2 != 0):
                        volume = volume[::-1]
                else:
                    volume = np.zeros((num_z, num_y, num_x), dtype=np.uint16)
                    # loop over depth (z-dim)
                    for j, frame in enumerate(frames):
                        # For a given frame, get filename
                        files = frame.findall("File")
                        filename = files[channel].get("filename")
                        fullfile = os.path.join(data_dir, filename)  # type: ignore

                        # Read in file
                        volume[j, :, :] = io.imread(fullfile, plugin="pil")

                # z, y, x -> x, y, z
                mm[..., i] = np.transpose(volume, (2, 1, 0))
                if (i + 1) % 100 == 0:
                    mm.flush()
            mm.flush()
            del mm

            write_nii1_header(save_name, shape)
            assert nib.load(save_name).shape == shape

            if gzip:
                with open(save_name, "rb") as f_in, gzip_module.open(
                    save_name + ".gz", "wb"
                ) as f_out:
                    shutil.copyfileobj(f_in, f_out)
                os.remove(save_name)
            continue

        image_array = np.zeros((num_timepoints, num_z, num_y, num_x), dtype=np.uint16)
        # saved as a single big tif for all time steps
        if isMultiPageTiff:
            frames = [sequences[0].findall("Frame")[0]]
            files = frames[0].findall("File")
            filename = files[channel].get("filename")
            fullfile = os.path.join(data_dir, filename)  # type: ignore
            img = imread_stack(fullfile)  # shape = t, y, x
            image_array[:, 0, :, :] = img
        else:
            # loop over time steps to load one tif at a time
            for i in range(num_timepoints):
                # Plane series: Get frame
                frame = sequences[0].findall("Frame")[i]
                files = frame.findall("File")
                filename = files[channel].get("filename")
                fullfile = os.path.join(data_dir, filename)  # type: ignore

                # Read in file
                img = io.imread(fullfile, plugin="pil")
                image_array[i, 0, :, :] = img

        image_array = np.squeeze(image_array)  # t, y, x
        image_array = np.moveaxis(image_array, 0, -1)
        image_array = np.swapaxes(image_array, 0, 1)  # x, y, t

        logger.debug("%s, final array shape: %s", xml_file, image_array.shape)

        aff = np.eye(4)
        if gzip:
            save_name += ".gz"
        img = nib.nifti2.Nifti2Image(image_array, aff)  # 64 bit
        image_array = None  # for memory
        logger.debug("%s, saving nii as %s", xml_file, save_name)
        img.to_filename(save_name)
        img = None  # for memory

        # NOTE berger 2024/07/18: previously there was a 2 second sleep here
        # for "memory management". I suspect this is unnecessary but if OOM
        # issues are experienced in this method in the future a gc.collect here
        # may help


def imread_stack(fullfile):
    """Read a (multipage) TIFF with skimage's PIL plugin as (pages, y, x).

    skimage.io.imread moves a leading axis of length 3 or 4 to the end, assuming
    it is colour channels, so a 3- or 4-page stack comes back as (y, x, pages).
    Undo that here.
    """
    img = io.imread(fullfile, plugin="pil")
    with Image.open(fullfile) as pil_img:
        n_pages = getattr(pil_img, "n_frames", 1)
    if n_pages > 1 and img.ndim == 3 and img.shape[0] != n_pages:
        img = np.moveaxis(img, -1, 0)
    return img


def write_nii1_header(save_name, shape):
    """Write the NIfTI-1 header (uint16, identity affine) for a .nii file whose
    data block starts at NII_VOX_OFFSET. Byte-identical to what
    nib.Nifti1Image(data, np.eye(4)).to_filename() produces."""
    header = nib.Nifti1Header()
    header.set_data_shape(shape)
    header.set_data_dtype(np.uint16)
    header.set_sform(np.eye(4), code="aligned")
    header.set_qform(np.eye(4), code="unknown")
    header["vox_offset"] = NII_VOX_OFFSET
    block = header.binaryblock
    # 348 byte header followed by 4 byte extension flag (no extensions)
    assert len(block) + 4 == NII_VOX_OFFSET
    with open(save_name, "r+b") as f:
        f.seek(0)
        f.write(block + b"\x00\x00\x00\x00")


def get_num_channels(sequence):
    frame = sequence.findall("Frame")[0]
    files = frame.findall("File")
    return len(files)


def convert_tiff_collections_to_nii(directory, gzip=False):
    for item in os.listdir(directory):
        new_path = directory + "/" + item

        # Check if item is a directory
        if os.path.isdir(new_path):
            convert_tiff_collections_to_nii(new_path, gzip=gzip)

        # If the item is a file
        else:
            if item.endswith(".xml"):
                tree = ET.parse(new_path)
                root = tree.getroot()
                # If the item is an xml file with scan info
                if root.tag == "PVScan":

                    # Also, verify that this folder doesn't already contain any .niis
                    # This is useful if rebooting the pipeline due to some error, and
                    # not wanting to take the time to re-create the already made niis
                    for item in os.listdir(directory):
                        if item.endswith(".nii") or item.endswith(".nii.gz"):
                            logger.warning(
                                "skipping nii containing folder (nilpotency): %s",
                                directory,
                            )
                            break
                    else:
                        tiff_to_nii(new_path, gzip=gzip)
