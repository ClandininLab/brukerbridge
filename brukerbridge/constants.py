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
