""" Test for XML parsing and associated conversion logic specific to PraireView 5.8
"""
import pytest

from brukerbridge.conversion.common import AcquisitionType, TiffPageFormat
from brukerbridge.conversion.pv58 import (
    parse_acquisition_channel_info, parse_acquisition_is_bidirectional,
    parse_acquisition_tiff_page_format,
    parse_acquisition_tiff_page_format_fallback, parse_acquisition_type)


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


def test_parse_acquisition_tiff_page_format_detects_single_page_acqs(
    single_page_test_acq_xml_path,
):
    assert (
        parse_acquisition_tiff_page_format(single_page_test_acq_xml_path)
        == TiffPageFormat.SINGLE_PAGE
    )


def test_parse_acquisition_tiff_page_format_detects_multi_page_acqs(
    multi_page_test_acq_xml_path,
):
    assert (
        parse_acquisition_tiff_page_format(multi_page_test_acq_xml_path)
        == TiffPageFormat.MULTI_PAGE
    )


@pytest.mark.slow
def test_parse_acquisition_tiff_page_format_fallback_detects_single_page_acqs(
    single_page_test_acq_xml_path,
):
    assert (
        parse_acquisition_tiff_page_format_fallback(single_page_test_acq_xml_path)
        == TiffPageFormat.SINGLE_PAGE
    )


@pytest.mark.slow
def test_parse_acquisition_tiff_page_format_fallback_detects_multi_page_acqs(
    multi_page_test_acq_xml_path,
):
    assert (
        parse_acquisition_tiff_page_format_fallback(multi_page_test_acq_xml_path)
        == TiffPageFormat.MULTI_PAGE
    )


def test_parse_acquisition_is_bidirectional_pos(bidir_test_acq_xml_path):
    assert parse_acquisition_is_bidirectional(bidir_test_acq_xml_path) == True


def test_parse_acquisition_is_bidirectional_neg(singledir_test_acq_xml_path):
    assert parse_acquisition_is_bidirectional(singledir_test_acq_xml_path) == False


def test_parse_acquisition_channel_info_2ch(two_channel_test_acq_xml_path):
    assert len(parse_acquisition_channel_info(two_channel_test_acq_xml_path)) == 2


def test_parse_acquisition_channel_info_3ch(three_channel_test_acq_xml_path):
    assert len(parse_acquisition_channel_info(three_channel_test_acq_xml_path)) == 3
