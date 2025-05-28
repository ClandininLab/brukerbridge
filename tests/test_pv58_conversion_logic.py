""" Test for XML parsing and associated conversion logic specific to PraireView 5.8
"""

from brukerbridge.conversion.common import AcquisitionType, TiffPageFormat
from brukerbridge.conversion.pv58 import parse_acquisition_type


def test_parse_acquisition_type_detects_volume_series(volume_test_acq_xml_path):
    assert (
        parse_acquisition_type(volume_test_acq_xml_path) == AcquisitionType.VOL_SERIES
    )


def test_parse_acquisition_type_detects_plane_series(single_plane_test_acq_xml_path):
    assert (
        parse_acquisition_type(single_plane_test_acq_xml_path)
        == AcquisitionType.PLANE_SERIES
    )


def test_parse_acquisition_type_detects_single_images(single_image_test_acq_xml_path):
    assert (
        parse_acquisition_type(single_image_test_acq_xml_path)
        == AcquisitionType.SINGLE_IMAGE
    )
