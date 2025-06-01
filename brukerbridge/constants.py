""" Global constants and enums
"""
from enum import Enum


class AcquisitionType(Enum):
    VOL_SERIES = 1
    PLANE_SERIES = 2
    SINGLE_IMAGE = 3


class TiffPageFormat(Enum):
    SINGLE_PAGE = 1
    MULTI_PAGE = 2


OAK_TRANSFER_SUFFIX_WHITELIST = (
    ".nii.gz",
    ".nii",
    ".csv",
    ".xml",
    "json",
    "tiff",
    "hdf5",
)

# base log dir used in app
LOG_DIR = "C:/Users/User/logs"


# max concurrent processes
MAX_RIPPERS = 2
MAX_TIFF_WORKERS = 2
MAX_OAK_WORKERS = 2
