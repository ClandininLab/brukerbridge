import logging
import os
from xml.etree import ElementTree as ET

import nibabel as nib
import numpy as np
from matplotlib.pyplot import imread

logger = logging.getLogger(__name__)


def tiff_to_nii(xml_file: str):
    aborted = False
    data_dir, _ = os.path.split(xml_file)

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
    img = imread(fullfile)
    num_y = np.shape(img)[0]
    num_x = np.shape(img)[1]
    logger.debug(
        "%s, num_channels: %s, num_timepoints: %s, num_z: %s, num_y: %s, num_x: %s",
        xml_file,
        num_channels,
        num_timepoints,
        num_z,
        num_y,
        num_x,
    )

    ##determine if it is too big for memory
    ##create range of timepoints to use
    # max_timepoints = 3384 #this number comes from Luke's data where the memory is sufficient to process the nii file. The other dimensions matter too in terms of overall size (256 x 128 x 49), but for now I'll assume the other dims are similar
    max_timepoints = 500  # still memory error so going to 1000

    # this will give all the starting points for the different broken up nii files
    timepoint_starts = list(range(0, num_timepoints, max_timepoints))
    logger.debug("%s, timepoint_starts: %s", xml_file, timepoint_starts)
    timepoint_ends = []
    for t_index in range(len(timepoint_starts)):
        if (
            timepoint_starts[t_index] == timepoint_starts[-1]
        ):  # if it's the last element in the list (may also be the first)
            timepoint_ends.append(num_timepoints)  # so it goes until the end
        else:
            timepoint_ends.append(
                timepoint_starts[t_index + 1]
            )  # 20231031 NEW EDIT TO FIX INDEXING PROBLEM THAT DROPS A TIMEPOINT

    timepoint_ranges = list(zip(timepoint_starts, timepoint_ends))

    ##run function for creating nii file(s)
    # run through each set of timepoints to make the different nii files
    logger.debug("%s, timepoint ranges: %s", xml_file, timepoint_ranges)
    for i in range(len(timepoint_ranges)):
        create_nii_file(
            timepoint_ranges[i],
            num_channels,
            num_z,
            num_y,
            num_x,
            isVolumeSeries,
            isBidirectionalZ,
            sequences,
            xml_file,
            data_dir,
            aborted,
        )


def get_num_channels(sequence):
    frame = sequence.findall("Frame")[0]
    files = frame.findall("File")
    return len(files)


def create_nii_file(
    timepoint_range,
    num_channels,
    num_z,
    num_y,
    num_x,
    isVolumeSeries,
    isBidirectionalZ,
    sequences,
    xml_file,
    data_dir,
    aborted,
):
    """this creates a nii file in the same way as before, but it creates seperate nii files if the original data is bigger than the size memory allows.
    The save name appends on the starting frame number to keep the files seperate
    timepoint_range is a tuple that contains (timepoint_start, timepoint_end) for each set"""
    # loop over channels
    for channel in range(num_channels):
        timepoint_start = timepoint_range[0]
        timepoint_end = timepoint_range[1]
        logger.debug("%s, frames: %s-%s", xml_file, timepoint_start, timepoint_end)
        last_num_z = None
        image_array = np.zeros(
            ((timepoint_end - timepoint_start), num_z, num_y, num_x), dtype=np.uint16
        )
        # loop over time
        for i in range(timepoint_start, timepoint_end):
            if isVolumeSeries:  # For a given volume, get all frames
                frames = sequences[i].findall("Frame")
                current_num_z = len(frames)
                # Handle aborted scans for volumes
                if last_num_z is not None:
                    if current_num_z != last_num_z:
                        # detected by "Inconsistent number of z-slices"
                        logger.warning(
                            "%s: this acquisition was aborted, discarding last volume",
                            xml_file,
                        )
                        aborted = True
                        break
                last_num_z = current_num_z

                # Flip frame order if a bidirectionalZ upstroke (odd i)
                if isBidirectionalZ and (i % 2 != 0):
                    frames = frames[::-1]

            else:  # Plane series: Get frame
                frames = [sequences[0].findall("Frame")[i]]

            # loop over depth (z-dim)
            for j, frame in enumerate(frames):
                # For a given frame, get filename
                files = frame.findall("File")
                filename = files[channel].get("filename")
                # print(filename)
                fullfile = os.path.join(data_dir, filename)

                # Read in file
                img = imread(fullfile)
                image_array[i - timepoint_start, j, :, :] = img

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
        save_name = (
            xml_file[:-4]
            + "_channel_{}".format(channel + 1)
            + "_s"
            + str(timepoint_start)
            + ".nii"
        )
        if isVolumeSeries:
            img = nib.nifti1.Nifti1Image(
                image_array, aff
            )  # 32 bit: maxes out at 32767 in any one dimension
        else:
            img = nib.nifti2.Nifti2Image(image_array, aff)  # 64 bit
        image_array = None  # for memory
        logger.debug("%s, saving nii as %s", xml_file, save_name)
        img.to_filename(save_name)
        img = None  # for memory

        # NOTE berger 2024/07/18: previously there was a 10 second sleep here
        # for "memory management". I suspect this is unnecessary but if OOM
        # issues are experienced in this method in the future a gc.collect here
        # may help


def convert_tiff_collections_to_nii_split(directory):
    for item in os.listdir(directory):
        new_path = directory + "/" + item

        # Check if item is a directory
        if os.path.isdir(new_path):
            convert_tiff_collections_to_nii_split(new_path)

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

                    # NOTE: berger 2024/8/1 this was commented out for unknown
                    # reason by bella on 2021/07/02. I don't see any reason not
                    # to reenable it
                    for item in os.listdir(directory):
                        if item.endswith(".nii"):
                            logger.warning(
                                "skipping nii containing folder (nilpotency): %s",
                                directory,
                            )
                            break
                    else:
                        tiff_to_nii(new_path)
