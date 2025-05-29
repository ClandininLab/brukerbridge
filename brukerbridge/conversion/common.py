""" Conversion logic common to all PraireView version
"""

import logging
from pathlib import Path
from xml.etree import ElementTree

import brukerbridge.conversion.pv58 as pv58

logger = logging.getLogger(__name__)


def parse_pvscan_xml_version(xml_path: Path) -> str:
    """Parse version of PraireView that made this PVScan XML file. This is the only parsing logic which is assumed to work across files created by different versions of PraireView"""
    acq_root = ElementTree.parse(xml_path).getroot()
    assert acq_root.tag == "PVScan"

    return acq_root.attrib["version"]


def convert_acquisition_to_nifti(xml_path: Path):
    pv_version = parse_pvscan_xml_version(xml_path)
    if pv_version != pv58.SUPPORTED_PRAIREVIEW_VERSION:
        # NOTE: AB 2025/05/2
        # I've put this method in common with the intent that it will
        # eventually dispatch to PV version appropriate logic. That being said,
        # it only supports PV5.8 right now and immediately goes back to using
        # logic in the pv58 module so
        raise NotImplementedError

    pv58.convert_acquisition_to_nifti(xml_path)
