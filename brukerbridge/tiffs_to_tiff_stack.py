import glob
import os
from xml.etree import ElementTree as ET

# from skimage.external import tifffile # this is deprecated in new skimage. directly import tifffile.
import tifffile


def convert_tiff_collections_to_stack(directory):
    for item in os.listdir(directory):
        new_path = os.path.join(directory, item)

        # Check if item is a directory
        if os.path.isdir(new_path):
            convert_tiff_collections_to_stack(new_path)

        # If the item is a file
        else:
            # If the item is an xml file
            if ".xml" in item:
                tree = ET.parse(new_path)
                root = tree.getroot()
                # If the item is an xml file with scan info
                if root.tag == "PVScan":
                    tiffs_to_stack(directory)


def tiffs_to_stack(directory):
    stack_fn = os.path.join(directory, "stack.tiff")
    print("Creating tiff stack from {}".format(directory))
    with tifffile.TiffWriter(stack_fn, imagej=True) as stack:
        # For some reason, the first tif file grabs the whole stack, so saving
        #   only the first tif file using stack.save is sufficient...
        # for filename in sorted(glob.glob(os.path.join(directory, '*.tif'))):
        #     stack.save(tifffile.imread(filename))
        stack.save(
            tifffile.imread(sorted(glob.glob(os.path.join(directory, "*.tif")))[0])
        )
