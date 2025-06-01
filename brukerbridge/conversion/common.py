""" Conversion logic common to all PraireView version
"""

import logging
from pathlib import Path
from xml.etree import ElementTree

import brukerbridge.conversion.pv58 as pv58

logger = logging.getLogger(__name__)


RIPPER_EXECUTABLES = dict()
RIPPER_EXECUTABLES[pv58.SUPPORTED_PRAIREVIEW_VERSION] = pv58.RIPPER_EXECUTABLE

CONVERSION_MODULES = dict()
CONVERSION_MODULES[pv58.SUPPORTED_PRAIREVIEW_VERSION] = pv58

SUPPORTED_PRAIREVIEW_VERSIONS = set(RIPPER_EXECUTABLES.keys())


def parse_acquisition_pvscan_version(xml_path: Path) -> str:
    """Parse version of PraireView that made this PVScan XML file. This is the only parsing logic which is assumed to work across files created by different versions of PraireView"""
    acq_root = ElementTree.parse(xml_path).getroot()
    assert acq_root.tag == "PVScan"

    return acq_root.attrib["version"]


def convert_acquisition_to_nifti(xml_path: Path, compress: bool, max_image_size: int):
    pv_version = parse_acquisition_pvscan_version(xml_path)
    CONVERSION_MODULES[pv_version].convert_acquisition_to_nifti(
        xml_path, compress, max_image_size
    )
