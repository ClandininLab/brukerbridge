import logging
import os
import time
from xml.etree import ElementTree as ET

import nibabel as nib
import numpy as np
from skimage import io

logger = logging.getLogger(__name__)


def tiff_to_nii(xml_file):
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
        root.find("Sequence").get("type") == "TSeries Timed Element"
    ):  # Plane time series
        num_timepoints = len(sequences[0].findall("Frame"))
        num_z = 1
        isVolumeSeries = False
    elif (
        root.find("Sequence").get("type") == "TSeries ZSeries Element"
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
    fullfile = os.path.join(data_dir, test_file)

    ### Luke added try except 20221024 because sometimes but rarely a file doesn't exist
    # somthing to do with bruker xml file
    #
    # berger 2024/7/18: removed exception handling, this should raise a fatal
    # error since and would have at the next unbound reference to `img` anyways
    img = io.imread(fullfile, plugin="pil")

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

    # loop over channels
    for channel in range(num_channels):
        last_num_z = None
        image_array = np.zeros((num_timepoints, num_z, num_y, num_x), dtype=np.uint16)
        # saved as a single big tif for all time steps
        if isMultiPageTiff and (isVolumeSeries is False):
            frames = [sequences[0].findall("Frame")[0]]
            files = frames[0].findall("File")
            filename = files[channel].get("filename")
            fullfile = os.path.join(data_dir, filename)
            img = io.imread(fullfile, plugin="pil")  # shape = t, y, x
            image_array[:, 0, :, :] = img
        else:
            # loop over time steps to load one tif at a time
            for i in range(num_timepoints):
                if isVolumeSeries:  # For a given volume, get all frames
                    frames = sequences[i].findall("Frame")
                    current_num_z = len(frames)
                    # Handle aborted scans for volumes
                    if last_num_z is not None:
                        if current_num_z != last_num_z:
                            # detected by "Inconsistent number of z-slices"
                            logger.info(
                                "%s: this acquisition was aborted, discarding last volume"
                            )
                            aborted = True
                            break
                    last_num_z = current_num_z

                    # Flip frame order if a bidirectionalZ upstroke (odd i)
                    if isBidirectionalZ and (i % 2 != 0):
                        frames = frames[::-1]

                else:  # Plane series: Get frame
                    frames = [sequences[0].findall("Frame")[i]]

                if isMultiPageTiff:
                    files = frames[0].findall("File")
                    filename = files[channel].get("filename")
                    fullfile = os.path.join(data_dir, filename)
                    img = io.imread(fullfile, plugin="pil")  # shape = z, y, x
                    image_array[i, :, :, :] = img
                else:
                    # loop over depth (z-dim)
                    for j, frame in enumerate(frames):
                        # For a given frame, get filename
                        files = frame.findall("File")
                        filename = files[channel].get("filename")
                        fullfile = os.path.join(data_dir, filename)

                        # Read in file
                        img = io.imread(fullfile, plugin="pil")
                        image_array[i, j, :, :] = img

        if isVolumeSeries:
            # Will start as t,z,x,y. Want y,x,z,t
            image_array = np.moveaxis(image_array, 1, -1)  # Now t,x,y,z
            image_array = np.moveaxis(image_array, 0, -1)  # Now x,y,z,t
            image_array = np.swapaxes(image_array, 0, 1)  # Now y,x,z,t

            # Toss last volume if aborted
            if aborted:
                image_array = image_array[:, :, :, :-1]
        else:
            image_array = np.squeeze(image_array)  # t, x, y
            image_array = np.moveaxis(image_array, 0, -1)  # x, y, t
            image_array = np.swapaxes(image_array, 0, 1)  # y, x, t

        logger.debug("%s, final array shape: %s", xml_file, image_array.shape)

        aff = np.eye(4)
        save_name = xml_file[:-4] + "_channel_{}".format(channel + 1) + ".nii"
        if isVolumeSeries:
            img = nib.Nifti1Image(
                image_array, aff
            )  # 32 bit: maxes out at 32767 in any one dimension
        else:
            img = nib.Nifti2Image(image_array, aff)  # 64 bit
        image_array = None  # for memory
        logger.debug("%s, saving nii as %s", xml_file, save_name)
        img.to_filename(save_name)
        img = None  # for memory

        # NOTE berger 2024/07/18: previously there was a 2 second sleep here
        # for "memory management". I suspect this is unnecessary but if OOM
        # issues are experienced in this method in the future a gc.collect here
        # may help


def get_num_channels(sequence):
    frame = sequence.findall("Frame")[0]
    files = frame.findall("File")
    return len(files)


def convert_tiff_collections_to_nii(directory):
    for item in os.listdir(directory):
        new_path = directory + "/" + item

        # Check if item is a directory
        if os.path.isdir(new_path):
            convert_tiff_collections_to_nii(new_path)

        # If the item is a file
        else:
            # If the item is an xml file
            if ".xml" in item:
                tree = ET.parse(new_path)
                root = tree.getroot()
                # If the item is an xml file with scan info
                if root.tag == "PVScan":

                    # Also, verify that this folder doesn't already contain any .niis
                    # This is useful if rebooting the pipeline due to some error, and
                    # not wanting to take the time to re-create the already made niis
                    for item in os.listdir(directory):
                        if item.endswith(".nii"):
                            logger.info(
                                "skipping nii containing folder (nilpotency): %s",
                                directory,
                            )
                            break
                    else:
                        tiff_to_nii(new_path)
