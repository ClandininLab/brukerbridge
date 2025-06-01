""" Test brukerbridge.io module
"""

import gzip
import math

import nibabel as nib
import numpy as np
import pytest

from brukerbridge.io import (calculate_max_chunk_size, write_nifti_streaming,
                             write_nifti_streaming_chunked)


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
    n_frames = math.prod(header.get_data_shape()[2:])
    if n_frames == 1:
        # write nifti streaming only reads one frame, no time to raise
        pytest.skip("Skipping inapplicable values of parametrized fixture")

    assert len(list(tmp_path.iterdir())) == 0

    with pytest.raises(Exception):
        write_nifti_streaming(header, raising_frame_gen, streaming_nii_path)

    assert len(list(tmp_path.iterdir())) == 0


def test_write_nifti_streaming_handles_too_few_frames(
    header, too_few_frame_gen, streaming_nii_path
):
    """Check write_nifti_streaming detects when not enough frames are generated"""
    n_frames = math.prod(header.get_data_shape()[2:])
    if n_frames == 1:
        # write nifti streaming stops after reading expected number of frames
        pytest.skip("Skipping inapplicable values of parametrized fixture")

    with pytest.raises(RuntimeError):
        write_nifti_streaming(header, too_few_frame_gen, streaming_nii_path)


def test_write_nifti_streaming_handles_too_many_frames(
    header, too_many_frame_gen, streaming_nii_path
):
    """Check write_nifti_streaming detects when not enough frames are generated"""

    # write_nifti_streaming only reads the number of frames it requires so it
    # suffices to check that it runs without raising
    write_nifti_streaming(header, too_many_frame_gen, streaming_nii_path)


def test_calculate_max_chunk_size_normal():
    """Test chunk size calculation for normal case"""
    # 16x16 uint16 = 16*16*2 = 512 bytes/frame
    # max_chunk_size = 1024 bytes allows 2 frames
    chunk_size = calculate_max_chunk_size((16, 16, 16), 1024)
    assert chunk_size == 2


def test_calculate_max_chunk_size_floor():
    """Test chunk size calculation for floor div case"""
    # 16x16 uint16 = 16*16*2 = 512 bytes/frame
    # max_chunk_size = 1100 bytes allows 2 frames after floor div
    chunk_size = calculate_max_chunk_size((16, 16, 16), 1100)
    assert chunk_size == 2


def test_calculate_max_chunk_size_larger_than_data():
    """Test when max chunk size is larger than all data"""
    chunk_size = calculate_max_chunk_size((16, 16, 10), 50000)
    assert chunk_size == 10  # should be clamped to data size


def test_calculate_max_chunk_size_too_small():
    """Test when max chunk size is too small"""
    with pytest.raises(RuntimeError, match="max_chunk_size .* too small"):
        calculate_max_chunk_size((16, 16, 16), 100)  # way too small


def test_write_nifti_streaming_chunked_creates_multiple_files_one_sample_chunks(
    tmp_path, header, frame_gen, min_chunk_size_bytes
):
    # size of last axis is the max possible number of chunks
    if header.get_data_shape()[-1] == 1:
        pytest.skip("Skipping inapplicable values of parametrized fixture")

    write_nifti_streaming_chunked(
        header, frame_gen, tmp_path / "test.nii", min_chunk_size_bytes
    )

    chunk_files = list(tmp_path.glob("test_chunk_*.nii"))
    assert len(chunk_files) > 1  # should create multiple chunks


def test_write_nifti_streaming_chunked_creates_multiple_files_two_sample_chunks(
    tmp_path, header, frame_gen, min_chunk_size_bytes
):
    # size of last axis is the max possible number of chunks
    if header.get_data_shape()[-1] <= 2:
        # need more than two frame to make chunks with chunk size of 2 * frame_size_bytes
        pytest.skip("Skipping inapplicable values of parametrized fixture")

    write_nifti_streaming_chunked(
        header, frame_gen, tmp_path / "test.nii", 2 * min_chunk_size_bytes
    )

    chunk_files = list(tmp_path.glob("test_chunk_*.nii"))
    assert len(chunk_files) > 1  # should create multiple chunks


@pytest.mark.parametrize("chunk_size_samples", [1, 2, 3, 4, 5])
def test_write_nifti_streaming_chunked_reconstruction_matches_original_integral_chunk_size(
    tmp_path, header, frame_gen, buffered_nii, min_chunk_size_bytes, chunk_size_samples
):
    """Test that concatenating chunks gives same result as original, when chunk
    size is integral multiple of atomic chunk size

    chunk_size_samples: size of chunk in units of samples

    """

    # size of last axis is the max possible number of chunks/samples
    if header.get_data_shape()[-1] <= chunk_size_samples:
        pytest.skip("Skipping inapplicable values of parametrized fixture")

    # Write chunked
    write_nifti_streaming_chunked(
        header,
        frame_gen,
        tmp_path / "test.nii",
        min_chunk_size_bytes * chunk_size_samples,
    )

    # Read all chunks and concatenate
    chunk_files = sorted(
        tmp_path.glob("test_chunk_*.nii"),
        key=lambda x: int(x.name.split(".")[0].split("_")[-1]),
    )
    chunk_arrays = []
    for chunk_file in chunk_files:
        chunk_img = nib.load(chunk_file)
        chunk_arrays.append(chunk_img.get_fdata())

    # Concatenate along last axis
    reconstructed = np.concatenate(chunk_arrays, axis=-1)

    # Compare with original
    original_data = buffered_nii.get_fdata()
    np.testing.assert_array_equal(reconstructed, original_data)


@pytest.mark.parametrize("chunk_size_samples", [1, 2, 3, 4, 5])
def test_write_nifti_streaming_chunked_reconstruction_matches_original_nonintegral_chunk_size(
    tmp_path, header, frame_gen, buffered_nii, min_chunk_size_bytes, chunk_size_samples
):
    """Test that concatenating chunks gives same result as original, when chunk
    size is not integral multiple of atomic chunk size

    chunk_size_samples: size of chunk in units of samples

    """

    # size of last axis is the max possible number of chunks/samples
    if header.get_data_shape()[-1] <= chunk_size_samples:
        pytest.skip("Skipping inapplicable values of parametrized fixture")

    # Write chunked
    write_nifti_streaming_chunked(
        header,
        frame_gen,
        tmp_path / "test.nii",
        int(1.1 * min_chunk_size_bytes * chunk_size_samples),
    )

    # Read all chunks and concatenate
    chunk_files = sorted(
        tmp_path.glob("test_chunk_*.nii"),
        key=lambda x: int(x.name.split(".")[0].split("_")[-1]),
    )
    chunk_arrays = []
    for chunk_file in chunk_files:
        chunk_img = nib.load(chunk_file)
        chunk_arrays.append(chunk_img.get_fdata())

    # Concatenate along last axis
    reconstructed = np.concatenate(chunk_arrays, axis=-1)

    # Compare with original
    original_data = buffered_nii.get_fdata()
    np.testing.assert_array_equal(reconstructed, original_data)


def test_write_nifti_streaming_chunked_single_chunk_when_small(
    tmp_path, header, frame_gen
):
    """Test that single chunk is created when data fits in memory limit"""
    large_chunk_size = 1000000  # much larger than test data

    write_nifti_streaming_chunked(
        header, frame_gen, tmp_path / "test.nii", large_chunk_size
    )

    chunk_files = list(tmp_path.glob("test_chunk_*.nii"))
    assert len(chunk_files) == 1
    assert chunk_files[0].name == "test_chunk_0.nii"


def test_write_nifti_streaming_chunked_preserves_gz_suffix(
    tmp_path, header, frame_gen, min_chunk_size_bytes
):
    """Test that .gz extension is preserved in chunk filenames"""
    write_nifti_streaming_chunked(
        header, frame_gen, tmp_path / "test.nii.gz", min_chunk_size_bytes
    )

    chunk_files = list(tmp_path.glob("test_chunk_*.nii.gz"))
    assert len(chunk_files) >= 1
    assert all(f.suffix == ".gz" for f in chunk_files)


def test_write_nifti_streaming_chunked_handles_exception_cleanup(
    tmp_path, header, raising_frame_gen, min_chunk_size_bytes
):
    """Test that chunked writing cleans up on exception"""

    max_chunks = header.get_data_shape()[-1]
    n_frames = math.prod(header.get_data_shape()[2:])
    if n_frames == 1:
        # write nifti streaming only reads one frame, no time to raise
        pytest.skip("Skipping inapplicable values of parametrized fixture")

    assert len(list(tmp_path.iterdir())) == 0

    with pytest.raises(Exception):
        write_nifti_streaming_chunked(
            header, raising_frame_gen, tmp_path / "test.nii", min_chunk_size_bytes
        )

    chunk_files = list(tmp_path.glob("test_chunk_*.nii"))
    assert len(chunk_files) < max_chunks
