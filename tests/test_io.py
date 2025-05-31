""" Test brukerbridge.io module
"""

import gzip

import pytest

from brukerbridge.io import write_nifti_streaming


# i'm not really sure what to with this one, exept as an expected fail
@pytest.mark.skip()
def test_mismatched_dims():
    """Test what happens if you do a streaming write where header dimensions don't match frame gen dimensions and then try to read that"""
    pass


def test_write_nifti_streaming_uncompressed(
    header, frame_gen, streaming_nii_path, buffered_nii_path
):
    """Test that streaming write matches buffered write byte for byte"""
    write_nifti_streaming(header, frame_gen, streaming_nii_path)

    with open(streaming_nii_path, "rb") as s_fh, open(buffered_nii_path, "rb") as b_fh:
        assert s_fh.read() == b_fh.read()


def test_write_nifti_streaming_compressed(
    header, frame_gen, streaming_nii_gz_path, buffered_nii_gz_path
):
    """Test that streaming write matches buffered write byte for byte"""
    write_nifti_streaming(header, frame_gen, streaming_nii_gz_path)

    # filename is included in the gzip header so you can't check for byte-for-byte identity of uncompressed files
    with gzip.GzipFile(streaming_nii_gz_path, "rb") as s_fh, gzip.GzipFile(
        buffered_nii_gz_path, "rb"
    ) as b_fh:
        assert s_fh.read() == b_fh.read()


def test_write_nifti_streaming_compressed_determinism(
    tmp_path, header, frame_gen, frame_gen2
):
    """gzip isn't deterministic by default for the inclusion of a timestamp"""
    write_nifti_streaming(header, frame_gen, tmp_path / "test1.nii.gz")
    write_nifti_streaming(header, frame_gen2, tmp_path / "test2.nii.gz")

    # filename is included in the gzip header so you can't check for byte-for-byte identity of uncompressed files
    with gzip.GzipFile(tmp_path / "test1.nii.gz", "rb") as t1_fh, gzip.GzipFile(
        tmp_path / "test2.nii.gz", "rb"
    ) as t2_fh:
        assert t1_fh.read() == t2_fh.read()


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
