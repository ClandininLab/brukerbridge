""" Test that streaming conversion I/O logic reproduces the original
"""

import nibabel as nib
import pytest

from brukerbridge.io import write_nifti


@pytest.mark.usefixtures("monolithic_nii_config")
def test_monolithic_sp_data_block(sp_acq_xml_path, channel, monolithic_sp_target):
    write_nifti(sp_acq_xml_path, channel)

    acq_path = sp_acq_xml_path.parent
    test_img_path = acq_path / f"{acq_path.name}_channel_{channel}.nii"
    test_img = nib.load(test_img_path)

    assert test_img.get_fdata() == monolithic_sp_target.get_fdata()


@pytest.mark.usefixtures("monolithic_nii_config")
def test_monolithic_volume_data_block(volume_acquisition):
    pass


@pytest.mark.usefixtures("monolithic_nii_config")
def test_monolithic_sp_header(sp_acq_xml_path, channel, monolithic_sp_target):
    write_nifti(sp_acq_xml_path, channel)

    acq_path = sp_acq_xml_path.parent
    test_img_path = acq_path / f"{acq_path.name}_channel_{channel}.nii"
    test_img = nib.load(test_img_path)

    # TODO: header assertion


@pytest.mark.usefixtures("monolithic_nii_config")
def test_monolithic_volume_data_header(volume_acquisition):
    pass


@pytest.mark.usefixtures("split_nii_config")
def test_split_single_plane_data_block(single_plane_acquisition):
    pass


@pytest.mark.usefixtures("split_nii_config")
def test_split_volume_data_block(volume_acquisition):
    pass


@pytest.mark.usefixtures("split_nii_config")
def test_split_single_plane_header(single_plane_acquisition):
    pass


@pytest.mark.usefixtures("split_nii_config")
def test_split_volume_data_header(volume_acquisition):
    pass
