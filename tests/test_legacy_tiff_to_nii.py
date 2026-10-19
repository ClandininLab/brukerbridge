""" Tests for the legacy tiff -> nii converters on synthetic PVScan 5.8-style data
"""

import gzip
import os
import sys
import tracemalloc
from glob import glob
from xml.etree import ElementTree as ET

import nibabel as nib
import numpy as np
import pytest
from PIL import Image

from brukerbridge.legacy import tiff_to_nii_split
from brukerbridge.legacy.tiff_to_nii import tiff_to_nii

NUM_T, NUM_Z, NUM_Y, NUM_X, NUM_CHANNELS = 3, 4, 8, 6, 2
ACQ_NAME = "TSeries-20240101-001"


def _write_tiff(path, pages):
    """Write a (pages, y, x) uint16 stack as a single- or multipage tiff"""
    images = [Image.fromarray(np.ascontiguousarray(page)) for page in pages]
    images[0].save(path, save_all=len(images) > 1, append_images=images[1:])


def make_acquisition(directory, multipage, num_t=NUM_T, frames_per_volume=None):
    """Write a synthetic acquisition into directory

    Returns (xml_path, data) where data[channel] is the ground truth
    (t, z, y, x) uint16 array in acquisition order
    """
    # tiff_to_nii locates the companion file via xml_file.split(".")[0]
    assert "." not in str(directory)
    if frames_per_volume is None:
        frames_per_volume = [NUM_Z] * num_t

    rng = np.random.default_rng(0)
    data = rng.integers(
        0, 2**16, size=(NUM_CHANNELS, num_t, NUM_Z, NUM_Y, NUM_X), dtype=np.uint16
    )

    root = ET.Element("PVScan", version="5.8.64.814")
    for t in range(num_t):
        sequence = ET.SubElement(
            root,
            "Sequence",
            type="TSeries ZSeries Element",
            cycle=str(t + 1),
            bidirectionalZ="False",
        )
        num_z = frames_per_volume[t]
        for z in range(num_z):
            frame = ET.SubElement(sequence, "Frame", index=str(z + 1))
            for c in range(NUM_CHANNELS):
                if multipage:
                    filename = f"{ACQ_NAME}_Cycle{t + 1:05d}_Ch{c + 1}_000001.ome.tif"
                    ET.SubElement(
                        frame, "File", channel=str(c + 1), page=str(z + 1), filename=filename
                    )
                else:
                    filename = f"{ACQ_NAME}_Cycle{t + 1:05d}_Ch{c + 1}_{z + 1:06d}.ome.tif"
                    ET.SubElement(frame, "File", channel=str(c + 1), filename=filename)
                    _write_tiff(directory / filename, data[c, t, z : z + 1])
        if multipage:
            for c in range(NUM_CHANNELS):
                filename = f"{ACQ_NAME}_Cycle{t + 1:05d}_Ch{c + 1}_000001.ome.tif"
                _write_tiff(directory / filename, data[c, t, :num_z])

    xml_path = directory / f"{ACQ_NAME}.xml"
    ET.ElementTree(root).write(xml_path)
    if multipage:
        (directory / f"{ACQ_NAME}.companion.ome").write_text("<OME/>")
    return str(xml_path), data


def old_in_ram_reference(path, volumes):
    """Build the nii exactly as tiff_to_nii did before streaming to a memmap"""
    image_array = np.array(volumes, dtype=np.uint16)  # t, z, y, x
    image_array = np.moveaxis(image_array, 1, -1)
    image_array = np.moveaxis(image_array, 0, -1)
    image_array = np.swapaxes(image_array, 0, 1)  # x, y, z, t
    nib.nifti1.Nifti1Image(image_array, np.eye(4)).to_filename(path)
    return path


def nii_path(xml_path, channel, suffix=""):
    return xml_path[:-4] + f"_channel_{channel + 1}{suffix}.nii"


def split_chunks(xml_path, channel):
    paths = glob(xml_path[:-4] + f"_channel_{channel + 1}_s*.nii")
    return sorted(paths, key=lambda p: int(p.rsplit("_s", 1)[1][:-4]))


@pytest.fixture(params=[True, False], ids=["multipage", "per-plane"])
def acquisition(request, tmp_path):
    return make_acquisition(tmp_path, multipage=request.param)


def test_tiff_to_nii_matches_in_ram_reference(acquisition, tmp_path):
    xml_path, data = acquisition
    tiff_to_nii(xml_path)

    for c in range(NUM_CHANNELS):
        ref = old_in_ram_reference(str(tmp_path / f"ref_{c}.nii"), data[c])
        with open(nii_path(xml_path, c), "rb") as f, open(ref, "rb") as f_ref:
            assert f.read() == f_ref.read()

        img = nib.load(nii_path(xml_path, c))
        assert img.shape == (NUM_X, NUM_Y, NUM_Z, NUM_T)
        assert img.get_data_dtype() == np.uint16
        np.testing.assert_array_equal(img.affine, np.eye(4))
        np.testing.assert_array_equal(
            np.asarray(img.dataobj), np.transpose(data[c], (3, 2, 1, 0))
        )


def test_tiff_to_nii_gzip(tmp_path):
    xml_path, data = make_acquisition(tmp_path, multipage=True)
    tiff_to_nii(xml_path, gzip=True)

    for c in range(NUM_CHANNELS):
        assert not os.path.exists(nii_path(xml_path, c))
        ref = old_in_ram_reference(str(tmp_path / f"ref_{c}.nii"), data[c])
        with gzip.open(nii_path(xml_path, c) + ".gz", "rb") as f:
            with open(ref, "rb") as f_ref:
                assert f.read() == f_ref.read()


@pytest.mark.parametrize("multipage", [True, False], ids=["multipage", "per-plane"])
def test_tiff_to_nii_aborted(tmp_path, multipage):
    # last volume only got 2 of 4 z-slices before the scan was aborted
    xml_path, data = make_acquisition(
        tmp_path, multipage=multipage, num_t=4, frames_per_volume=[4, 4, 4, 2]
    )
    tiff_to_nii(xml_path)

    for c in range(NUM_CHANNELS):
        img = nib.load(nii_path(xml_path, c))
        assert img.shape == (NUM_X, NUM_Y, NUM_Z, 3)
        np.testing.assert_array_equal(
            np.asarray(img.dataobj), np.transpose(data[c, :3], (3, 2, 1, 0))
        )


def test_tiff_to_nii_split_matches_tiff_to_nii(acquisition, monkeypatch):
    xml_path, data = acquisition
    # force more than one chunk: volumes [0, 1] and [2]
    monkeypatch.setattr(tiff_to_nii_split, "MAX_TIMEPOINTS", 2)
    tiff_to_nii_split.tiff_to_nii(xml_path)
    tiff_to_nii(xml_path)

    for c in range(NUM_CHANNELS):
        chunks = split_chunks(xml_path, c)
        assert [os.path.basename(p)[-7:] for p in chunks] == ["_s0.nii", "_s2.nii"]
        chunk_data = [np.asarray(nib.load(p).dataobj) for p in chunks]
        assert [d.shape[-1] for d in chunk_data] == [2, 1]

        # every volume has distinct z-slices (multipage used to repeat page 1)
        for d in chunk_data:
            for t in range(d.shape[-1]):
                for z1 in range(NUM_Z):
                    for z2 in range(z1 + 1, NUM_Z):
                        assert not np.array_equal(d[:, :, z1, t], d[:, :, z2, t])

        full = np.asarray(nib.load(nii_path(xml_path, c)).dataobj)
        np.testing.assert_array_equal(np.concatenate(chunk_data, axis=-1), full)


@pytest.mark.parametrize("multipage", [True, False], ids=["multipage", "per-plane"])
@pytest.mark.parametrize("converter", ["tiff_to_nii", "tiff_to_nii_split"])
def test_bidirectional_z_is_consistent_across_volumes(tmp_path, multipage, converter):
    # synthetic data is in acquisition order, as the scope writes it: odd
    # volumes are z upstrokes, so their planes / pages run top to bottom
    xml_path, data = make_acquisition(tmp_path, multipage=multipage)
    tree = ET.parse(xml_path)
    for sequence in tree.getroot().findall("Sequence"):
        sequence.set("bidirectionalZ", "True")
    tree.write(xml_path)

    if converter == "tiff_to_nii":
        tiff_to_nii(xml_path)
    else:
        tiff_to_nii_split.tiff_to_nii(xml_path)

    for c in range(NUM_CHANNELS):
        # every output volume should have z in the same (downstroke) order
        volumes = data[c].copy()
        volumes[1::2] = volumes[1::2, ::-1]
        ref = old_in_ram_reference(str(tmp_path / f"ref_{c}.nii"), volumes)
        if converter == "tiff_to_nii":
            out = nii_path(xml_path, c)
        else:
            (out,) = split_chunks(xml_path, c)
        np.testing.assert_array_equal(
            np.asarray(nib.load(out).dataobj), np.asarray(nib.load(ref).dataobj)
        )


def _traced_peak(fn):
    tracemalloc.start()
    try:
        fn()
        return tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()


@pytest.mark.parametrize("multipage", [True, False], ids=["multipage", "per-plane"])
def test_tiff_to_nii_peak_memory_is_bounded(tmp_path, monkeypatch, multipage):
    module = sys.modules[__name__]
    monkeypatch.setattr(module, "NUM_Y", 128)
    monkeypatch.setattr(module, "NUM_X", 64)
    num_t = 120
    xml_path, _ = make_acquisition(tmp_path, multipage=multipage, num_t=num_t)
    volume_bytes = NUM_Z * module.NUM_Y * module.NUM_X * 2

    # the parsed XML tree scales with num_t but isn't image data
    def parse_xml():
        sequences = ET.parse(xml_path).getroot().findall("Sequence")
        return [sequence.findall("Frame") for sequence in sequences]

    xml_peak = _traced_peak(parse_xml)
    peak = _traced_peak(lambda: tiff_to_nii(xml_path))

    # the in-RAM version held all num_t volumes (twice, while saving); now
    # only the volume being read is held, plus skimage's per-read copies
    assert peak - xml_peak < 6 * volume_bytes
