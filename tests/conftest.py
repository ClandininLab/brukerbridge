""" Shared fixtures and test control flow logic
"""

import json
import shutil
import sys
from glob import glob

import nibabel as nib
import numpy as np
import pytest
from brukerbridge.legacy.tiff_to_nii import convert_tiff_collections_to_nii
from brukerbridge.legacy.tiff_to_nii_split import \
    convert_tiff_collections_to_nii_split
# in 3.9 and above files is provided by importlib.resources
from importlib_resources import files

DATA_DRIVE = "D:"
PLATFORMS = set("darwin linux win32".split())

#  ===========================================================
#  ================ TEST CONTROL FLOW LOGIC ==================
#  ===========================================================


def pytest_addoption(parser):
    parser.addoption(
        "--run-slow", action="store_true", default=False, help="run slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-slow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


def pytest_runtest_setup(item):
    supported_platforms = PLATFORMS.intersection(
        mark.name for mark in item.iter_markers()
    )
    plat = sys.platform
    if supported_platforms and plat not in supported_platforms:
        pytest.skip(f"cannot run on platform {plat}")


#  ============================================
#  ================ FIXTURES ==================
#  ============================================


@pytest.fixture
def tmp_user(tmp_path):
    """Create a unique user under the data drive.

    Just wraps the built in tmp_path fixture with some assertions to check that
    the basetemp flag is set to DATA_DRIVE and tmp_path is DATA_DRIVE"s direct
    descendent
    """
    assert tmp_path.drive == "D:"
    assert tmp_path.parent == tmp_path.anchor

    return tmp_path


@pytest.fixture
def test_data_path():
    return files("brukerbridge") / "../../tests/data"


@pytest.fixture
def monolithic_nii_config(tmp_user):
    config = {"oak_target": "X:/data/berger/pytest", "convert_to": "nii"}
    config_dir = files("brukerbridge") / "../users"
    config_path = config_dir / tmp_user.name

    with open(config_path, "w") as fh:
        json.dump(config, fh)

    yield

    config_path.unlink()


# parametrize over json bool booboo values
@pytest.fixture(params=["True", True])
def split_nii_config(tmp_user, request):
    config = {
        "oak_target": "X:/data/berger/pytest",
        "convert_to": "nii",
        "split": request.param,
    }
    config_dir = files("brukerbridge") / "../users"
    config_path = config_dir / tmp_user.name

    with open(config_path, "w") as fh:
        json.dump(config, fh)

    yield

    config_path.unlink()


@pytest.fixture(params=[1, 2])
def channel(request):
    return request.param


@pytest.fixture(params=["small", "med"])
def size(request):
    return request.param


@pytest.fixture
def sp_acq_xml_path(size, tmp_user, test_data_path):
    """Copies over some test data and returns the path to the xml"""
    reserve_acq_path = test_data_path / f"test_acquisitions/sp_{size}"
    test_acq_path = tmp_user / f"sp_{size}"
    shutil.copy(reserve_acq_path, test_acq_path)

    xml_glob = test_acq_path.glob("*.xml")
    assert len(xml_glob) == 1

    return xml_glob[0]


@pytest.fixture
def vol_acq_xml_path(size, tmp_user, test_data_path):
    """Copies over some test data and returns the path to the xml"""
    reserve_acq_path = test_data_path / f"test_acquisitions/vol_{size}"
    test_acq_path = tmp_user / f"vol_{size}"
    shutil.copy(reserve_acq_path, test_acq_path)

    xml_glob = test_acq_path.glob("*.xml")
    assert len(xml_glob) == 1

    return test_acq_path


@pytest.fixture
def monolithic_sp_target(size, channel, test_data_path):
    matching_paths = glob(
        test_data_path / f"conversion_targets/sp_{size}/*_channel_{channel}.nii"
    )
    assert len(matching_paths) == 1
    return nib.load(matching_paths[0])


@pytest.fixture
def monolithic_vol_target(size, channel, test_data_path):
    matching_paths = glob(
        test_data_path / f"conversion_targets/vol_{size}/*_channel_{channel}.nii"
    )
    assert len(matching_paths) == 1
    return nib.load(matching_paths[0])


@pytest.fixture
def split_sp_targets(single_plane_acquisition):
    pass


@pytest.fixture
def split_vol_targets(volume_acquisition):
    pass


#  =============================================
#  ====== fixtures for XML parsing tests  ======
#  =============================================


def get_matching_test_acqs(
    pv_version, volume_series, multi_page_tiff, bidir_z_stroke, n_channels, complete
):
    """List test acquisitions matching params

    Args:
      pv_version: eg 'PV5.8' - str
      volume_series: True for vol series, False for single plane - bool
      multi_page_tiff: True for multi page tiff, False for single page - bool
      bidir_z_stroke: True for bidir, False for single dir strong - bool
      n_channels - int
      complete: scan complete - bool
    """
    pass


#  ======================================================
#  ====== fixtures for streaming io veracity tests ======
#  ======================================================


# NOTE: I have decided it will just be simplest to manually denote all shape test cases
@pytest.fixture(
    params=[
        (16, 16, 16),
        (16, 16, 16, 16),
        (16, 17, 18),
        (16, 17, 18, 19),
        (8, 16, 16),
        (16, 8, 16),
        (16, 16, 8),
        (8, 16, 16, 16),
        (16, 8, 16, 16),
        (16, 16, 8, 16),
        (16, 16, 16, 8),
        (1, 16, 16),
        (16, 1, 16),
        (16, 16, 1),
        (1, 16, 16, 16),
        (16, 1, 16, 16),
        (16, 16, 1, 16),
        (16, 16, 16, 1),
    ]
)
def img_shape(request):
    return request.param


@pytest.fixture
def img_arr(img_shape):
    return np.arange(np.prod(img_shape), dtype=np.uint16).reshape(img_shape)


@pytest.fixture
def header(img_shape):
    hdr = nib.nifti1.Nifti1Header()
    hdr.set_data_dtype(np.uint16)
    hdr.set_data_shape(img_shape)
    hdr.set_sform(np.eye(4))

    return hdr


@pytest.fixture
def frame_gen(img_arr):
    def _frame_gen():
        # 3d
        if len(img_arr.shape) == 3:
            for t_idx in range(img_arr.shape[2]):
                yield img_arr[:, :, t_idx]
        # 4d
        else:
            for t_idx in range(img_arr.shape[3]):
                for z_idx in range(img_arr.shape[2]):
                    yield img_arr[:, :, z_idx, t_idx]

    return _frame_gen()


@pytest.fixture
def buffered_nii(img_arr):
    aff = np.eye(4)
    return nib.nifti1.Nifti1Image(img_arr, aff)


@pytest.fixture
def streaming_nii_path(tmp_path):
    return tmp_path / "streaming.nii"


@pytest.fixture
def buffered_nii_path(tmp_path, buffered_nii):
    _buffered_path = tmp_path / "buffered.nii"
    buffered_nii.to_filename(_buffered_path)
    return _buffered_path
