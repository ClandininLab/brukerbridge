""" Conversion logic common to all PraireView version
"""

import logging
from enum import Enum
from pathlib import Path
from xml.etree import ElementTree

logger = logging.getLogger(__name__)


class AcquisitionType(Enum):
    VOL_SERIES = 1
    PLANE_SERIES = 2
    SINGLE_IMAGE = 3


class TiffPageFormat(Enum):
    SINGLE_PAGE = 1
    MULTI_PAGE = 2


def parse_pvscan_xml_version(xml_path: Path) -> str:
    """Parse version of PraireView that made this PVScan XML file. This is the only parsing logic which is assumed to work across files created by different versions of PraireView"""
    acq_root = ElementTree.parse(xml_path).getroot()
    assert acq_root.tag == "PVScan"

    return acq_root.attrib["version"]


# separate version parsing and checking whether it is a volume or plane series
