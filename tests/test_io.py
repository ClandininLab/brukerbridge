""" Test brukerbridge.io module
"""

import pytest
from brukerbridge.io import write_nifti_streaming


@pytest.mark.skip()
def test_mismatched_dims():
    """Test what happens if you do a streaming write where header dimensions don't match frame gen dimensions and then try to read that"""
    pass


def test_veracity(header, frame_gen, streaming_nii_path, buffered_nii_path):
    """Test that streaming write matches buffered write byte for byte"""
    write_nifti_streaming(header, frame_gen, streaming_nii_path)

    with open(streaming_nii_path, "rb") as s_fh, open(buffered_nii_path, "rb") as b_fh:
        assert s_fh.read() == b_fh.read()
