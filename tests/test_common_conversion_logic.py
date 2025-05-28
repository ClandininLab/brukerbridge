""" Test conversion logic common to all versions of PrairieView
"""

from brukerbridge.conversion.common import parse_pvscan_xml_version
from brukerbridge.conversion.pv58 import \
    SUPPORTED_PRAIREVIEW_VERSION as PV58_VERSION


def test_parse_pvscan_detects_58(pv58_test_acq_xml_path):
    assert parse_pvscan_xml_version(pv58_test_acq_xml_path) == PV58_VERSION
