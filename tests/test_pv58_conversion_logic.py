""" Test for XML parsing and associated conversion logic specific to PraireView 5.8
"""
import nibabel as nib
import pytest

from brukerbridge.constants import AcquisitionType, TiffPageFormat
from brukerbridge.conversion.pv58 import (
    create_acquisition_nifti_header, parse_acquisition_channel_info,
    parse_acquisition_is_bidirectional, parse_acquisition_resolution,
    parse_acquisition_shape, parse_acquisition_tiff_page_format,
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
    single_page_complete_test_acq_xml_path,
):
    assert (
        parse_acquisition_tiff_page_format_fallback(
            single_page_complete_test_acq_xml_path
        )
        == TiffPageFormat.SINGLE_PAGE
    )


@pytest.mark.slow
def test_parse_acquisition_tiff_page_format_fallback_detects_multi_page_acqs(
    multi_page_complete_test_acq_xml_path,
):
    assert (
        parse_acquisition_tiff_page_format_fallback(
            multi_page_complete_test_acq_xml_path
        )
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


def test_parse_acquisition_channel_info_single_image(single_image_test_acq_xml_path):
    """channel info unspecified for single images so just check it runs"""
    assert isinstance(
        parse_acquisition_channel_info(single_image_test_acq_xml_path), dict
    )


def test_parse_acqusition_resolution(pv58_test_acq_xml_path):
    """In the absence of ground truth (which could be obtained by inspection of
    the XML, but at which point would just be circular) this just checks that
    this returns the right type
    """
    parsed_res = parse_acquisition_resolution(pv58_test_acq_xml_path)
    assert isinstance(parsed_res, tuple)
    assert len(parsed_res) == 3


def test_parse_acquisition_shape_detects_force_termination_completed(
    completed_volume_test_acq_xml_path,
):
    _, force_terminated = parse_acquisition_shape(completed_volume_test_acq_xml_path)
    assert force_terminated == False


def test_parse_acquisition_shape_detects_force_termination_aborted(
    aborted_volume_test_acq_xml_path,
):
    if (
        aborted_volume_test_acq_xml_path.parent.name
        == "vol_single-page_single-z-stroke_2ch_abort"
    ):
        # jacob either labeled this one wrong or executed a frame perfect termination
        # either way, it can't be distingiushed from a completed acquisition
        pytest.xfail(
            "This acquisition is structurally identical to a completed acquisition"
        )

    _, force_terminated = parse_acquisition_shape(aborted_volume_test_acq_xml_path)
    assert force_terminated == True


def test_parse_acquisition_shape_handles_volume_series(volume_test_acq_xml_path):
    """Once again in the absence of ground truth just check return type and
    shape. Which the static analyzer already does... so basically just checking
    that it runs"""
    acq_shape, _ = parse_acquisition_shape(volume_test_acq_xml_path)
    assert isinstance(acq_shape, tuple)
    assert len(acq_shape) == 4


def test_parse_acquisition_shape_handles_plane_series(single_plane_test_acq_xml_path):
    """Once again in the absence of ground truth just check return type and shape"""
    acq_shape, _ = parse_acquisition_shape(single_plane_test_acq_xml_path)
    assert isinstance(acq_shape, tuple)
    assert len(acq_shape) == 3


def test_parse_acquisition_shape_handles_single_images(single_image_test_acq_xml_path):
    """Once again in the absence of ground truth just check return type and shape"""
    acq_shape, _ = parse_acquisition_shape(single_image_test_acq_xml_path)
    assert isinstance(acq_shape, tuple)
    assert len(acq_shape) == 2


def test_create_acquisition_nifti_header_runs(pv58_test_acq_xml_path):
    assert isinstance(
        create_acquisition_nifti_header(pv58_test_acq_xml_path), nib.nifti1.Nifti1Header
    )
