""" Test that streaming conversion I/O logic reproduces the original
"""

import pytest


@pytest.mark.usefixtures("monolithic_nii_config")
def test_monolithic_single_plane_data_block(single_plane_acquisition):
    pass


@pytest.mark.usefixtures("monolithic_nii_config")
def test_monolithic_volume_data_block(volume_acquisition):
    pass


@pytest.mark.usefixtures("monolithic_nii_config")
def test_monolithic_single_plane_header(single_plane_acquisition):
    pass


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
