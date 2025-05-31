""" Test brukerbridge.io module
"""

import pytest

from brukerbridge.io import write_nifti_streaming


# i'm not really sure what to with this one, exept as an expected fail
@pytest.mark.skip()
def test_mismatched_dims():
    """Test what happens if you do a streaming write where header dimensions don't match frame gen dimensions and then try to read that"""
    pass


def test_write_nifti_streaming_veracity(
    header, frame_gen, streaming_nii_path, buffered_nii_path
):
    """Test that streaming write matches buffered write byte for byte"""
    write_nifti_streaming(header, frame_gen, streaming_nii_path)

    with open(streaming_nii_path, "rb") as s_fh, open(buffered_nii_path, "rb") as b_fh:
        assert s_fh.read() == b_fh.read()


def test_write_nifti_streaming_handles_exception(
    tmp_path, header, raising_frame_gen, streaming_nii_path
):
    """Check that when an exception is thrown within write_nifti_streaming, it
    deletes it temp output before reraising the exception
    """
    assert len(list(tmp_path.iterdir())) == 0

    with pytest.raises(Exception):
        write_nifti_streaming(header, raising_frame_gen, streaming_nii_path)

    assert len(list(tmp_path.iterdir())) == 0


def test_write_nifti_streaming_handles_too_few_frames(
    header, too_few_frame_gen, streaming_nii_path
):
    """Check write_nifti_streaming detects when not enough frames are generated"""
    with pytest.raises(RuntimeError):
        write_nifti_streaming(header, too_few_frame_gen, streaming_nii_path)


def test_write_nifti_streaming_handles_too_many_frames(
    header, too_many_frame_gen, streaming_nii_path
):
    """Check write_nifti_streaming detects when not enough frames are generated"""
    with pytest.raises(RuntimeError):
        write_nifti_streaming(header, too_many_frame_gen, streaming_nii_path)
