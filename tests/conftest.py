""" Shared fixtures and test control flow logic
"""

import json
import math
import shutil
import sys
from glob import glob
from pathlib import Path

import nibabel as nib
import numpy as np
import pytest
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
    """some config that allows for platform dependent tests.

    mark windows only tests with @pytest.mark.win32 and they will be skipped on other platforms
    """
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
    return (files("brukerbridge") / "../tests/data").resolve()


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


def get_matching_raw_test_acqs(
    pv_version: str,
    is_vol=None,
    is_multi_page_tiff=None,
    is_bidir_z_stroke=None,
    n_channels=None,
    is_complete=None,
):
    """List test acquisitions matching params

    Single images gets its own method since there are no options beyond version

    Args:
      pv_version: eg 'PV5-8' - str
      is_vol: True for vol series, False for single plane - bool
      is_multi_page_tiff: True for multi page tiff, False for single page - bool
      is_bidir_z_stroke: True for bidir, False for single dir strong - bool
      n_channels - int
      is_complete: scan complete - bool
    """
    # NOTE: there is a fixture which returns this value, but since this method
    # is expected during runtime set up of fixtures, I have decided not to use
    # it, out of concern for weird fixture behavior
    base_path = (files("brukerbridge") / "../tests/data").resolve()

    if is_vol is None:
        vol_str = "*"
    else:
        if is_vol:
            vol_str = "vol"
        else:
            vol_str = "slc"

    if is_multi_page_tiff is None:
        tiff_str = "*"
    else:
        if is_multi_page_tiff:
            tiff_str = "multi-page"
        else:
            tiff_str = "single-page"

    if is_bidir_z_stroke is None:
        z_stroke_str = "*"
    else:
        if is_bidir_z_stroke:
            z_stroke_str = "bidir-z-stroke"
        else:
            z_stroke_str = "single-z-stroke"
        if not is_vol:
            z_stroke_str = "na"

    if n_channels is None:
        ch_str = "*"
    else:
        ch_str = f"{n_channels}ch"

    if is_complete is None:
        compl_str = "*"
    else:
        if is_complete:
            compl_str = "complete"
        else:
            compl_str = "abort"

    search_path = (
        base_path
        / pv_version
        / "raw_test_acqs"
        / f"{vol_str}_{tiff_str}_{z_stroke_str}_{ch_str}_{compl_str}"
    )

    return glob(str(search_path))


def get_single_image_raw_test_acqs(
    pv_version: str,
):
    """List test single image acquisitions

    Args:
      pv_version: eg 'PV5-8' - str
    """
    # NOTE: there is a fixture which returns this value, but since this method
    # is expected during runtime set up of fixtures, I have decided not to use
    # it, out of concern for weird fixture behavior
    base_path = (files("brukerbridge") / "../tests/data").resolve()

    search_path = base_path / pv_version / "raw_test_acqs" / "single_image_*"

    return glob(str(search_path))


def get_matching_ripped_test_acqs(
    pv_version: str,
    is_vol=None,
    is_multi_page_tiff=None,
    is_bidir_z_stroke=None,
    n_channels=None,
    is_complete=None,
):
    """List test acquisitions matching params

    Single images gets its own method since there are no options beyond version

    Args:
      pv_version: eg 'PV5-8' - str
      is_vol: True for vol series, False for single plane - bool
      is_multi_page_tiff: True for multi page tiff, False for single page - bool
      is_bidir_z_stroke: True for bidir, False for single dir strong - bool
      n_channels - int
      is_complete: scan complete - bool
    """
    # NOTE: there is a fixture which returns this value, but since this method
    # is expected during runtime set up of fixtures, I have decided not to use
    # it, out of concern for weird fixture behavior
    base_path = (files("brukerbridge") / "../tests/data").resolve()

    if is_vol is None:
        vol_str = "*"
    else:
        if is_vol:
            vol_str = "vol"
        else:
            vol_str = "slc"

    if is_multi_page_tiff is None:
        tiff_str = "*"
    else:
        if is_multi_page_tiff:
            tiff_str = "multi-page"
        else:
            tiff_str = "single-page"

    if is_bidir_z_stroke is None:
        z_stroke_str = "*"
    else:
        if is_bidir_z_stroke:
            z_stroke_str = "bidir-z-stroke"
        else:
            z_stroke_str = "single-z-stroke"
        if not is_vol:
            z_stroke_str = "na"

    if n_channels is None:
        ch_str = "*"
    else:
        ch_str = f"{n_channels}ch"

    if is_complete is None:
        compl_str = "*"
    else:
        if is_complete:
            compl_str = "complete"
        else:
            compl_str = "abort"

    search_path = (
        base_path
        / pv_version
        / "ripped_test_acqs"
        / f"{vol_str}_{tiff_str}_{z_stroke_str}_{ch_str}_{compl_str}"
    )

    return glob(str(search_path))


def get_single_image_ripped_test_acqs(
    pv_version: str,
):
    """List test single image acquisitions

    Args:
      pv_version: eg 'PV5-8' - str
    """
    # NOTE: there is a fixture which returns this value, but since this method
    # is expected during runtime set up of fixtures, I have decided not to use
    # it, out of concern for weird fixture behavior
    base_path = (files("brukerbridge") / "../tests/data").resolve()

    search_path = base_path / pv_version / "ripped_test_acqs" / "single_image_*"

    return glob(str(search_path))


def get_xml_path(acq_path):
    xml_search = list(Path(acq_path).glob("*.xml"))

    assert len(xml_search) == 1

    return xml_search[0]


@pytest.fixture(params=get_matching_raw_test_acqs("PV5-8"))
def pv58_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_matching_raw_test_acqs("PV5-8", is_multi_page_tiff=True))
def multi_page_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_matching_raw_test_acqs("PV5-8", is_multi_page_tiff=False))
def single_page_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(
    params=get_matching_raw_test_acqs(
        "PV5-8", is_multi_page_tiff=True, is_complete=True
    )
)
def multi_page_complete_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(
    params=get_matching_raw_test_acqs(
        "PV5-8", is_multi_page_tiff=False, is_complete=True
    )
)
def single_page_complete_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(
    params=get_matching_raw_test_acqs("PV5-8", is_vol=True, is_bidir_z_stroke=True)
)
def bidir_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(
    params=get_matching_raw_test_acqs("PV5-8", is_vol=True, is_bidir_z_stroke=False)
)
def singledir_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_matching_raw_test_acqs("PV5-8", n_channels=2))
def two_channel_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_matching_raw_test_acqs("PV5-8", n_channels=3))
def three_channel_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(
    params=get_matching_raw_test_acqs("PV5-8", is_complete=True, is_vol=True)
)
def completed_volume_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(
    params=get_matching_raw_test_acqs("PV5-8", is_complete=False, is_vol=True)
)
def aborted_volume_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_matching_raw_test_acqs("PV5-8", is_vol=True))
def volume_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_matching_raw_test_acqs("PV5-8", is_vol=False))
def single_plane_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_single_image_raw_test_acqs("PV5-8"))
def single_image_test_acq_xml_path(request):
    return get_xml_path(request.param)


# =====================


@pytest.fixture(params=get_matching_ripped_test_acqs("PV5-8"))
def pv58_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_matching_ripped_test_acqs("PV5-8", is_multi_page_tiff=True))
def multi_page_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_matching_ripped_test_acqs("PV5-8", is_multi_page_tiff=False))
def single_page_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(
    params=get_matching_ripped_test_acqs(
        "PV5-8", is_multi_page_tiff=True, is_complete=True
    )
)
def multi_page_complete_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(
    params=get_matching_ripped_test_acqs(
        "PV5-8", is_multi_page_tiff=False, is_complete=True
    )
)
def single_page_complete_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(
    params=get_matching_ripped_test_acqs("PV5-8", is_vol=True, is_bidir_z_stroke=True)
)
def bidir_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(
    params=get_matching_ripped_test_acqs("PV5-8", is_vol=True, is_bidir_z_stroke=False)
)
def singledir_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_matching_ripped_test_acqs("PV5-8", n_channels=2))
def two_channel_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_matching_ripped_test_acqs("PV5-8", n_channels=3))
def three_channel_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(
    params=get_matching_ripped_test_acqs("PV5-8", is_complete=True, is_vol=True)
)
def completed_volume_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(
    params=get_matching_ripped_test_acqs("PV5-8", is_complete=False, is_vol=True)
)
def aborted_volume_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_matching_ripped_test_acqs("PV5-8", is_vol=True))
def volume_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_matching_ripped_test_acqs("PV5-8", is_vol=False))
def single_plane_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


@pytest.fixture(params=get_single_image_ripped_test_acqs("PV5-8"))
def single_image_ripped_test_acq_xml_path(request):
    return get_xml_path(request.param)


#  =============================================
#  ====== fixtures for streaming io tests ======
#  =============================================


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


@pytest.fixture(params=[1, 2])
def img_arr(request, img_shape):
    # parametrize power we take each element, just to sidestep potential reshape problems
    return (
        np.arange(np.prod(img_shape), dtype=np.uint16).reshape(img_shape)
        ** request.param
    )


@pytest.fixture
def min_chunk_size_bytes(img_shape):
    # sample is one slice along last axis, i.e. atomic chunk size
    return math.prod(img_shape[:-1]) * 2


@pytest.fixture
def header(img_shape):
    hdr = nib.nifti2.Nifti2Header()
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


# sometimes you just need another one
@pytest.fixture
def frame_gen2(img_arr):
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
def raising_frame_gen(frame_gen, img_shape):
    """Yields a few frames before raising"""

    def _frame_gen():
        n_frames = math.prod(img_shape[2:])
        for _ in range(min(4, n_frames)):
            yield next(frame_gen)

        raise Exception

    return _frame_gen()


@pytest.fixture
def too_few_frame_gen(img_arr):
    def _frame_gen():
        # 3d
        if len(img_arr.shape) == 3:
            for t_idx in range(img_arr.shape[2] - 1):
                yield img_arr[:, :, t_idx]
        # 4d
        else:
            for t_idx in range(img_arr.shape[3] - 1):
                for z_idx in range(img_arr.shape[2]):
                    yield img_arr[:, :, z_idx, t_idx]

    return _frame_gen()


@pytest.fixture
def too_many_frame_gen(frame_gen):
    """Yields a few frames before raising"""

    def _frame_gen():
        frame = next(frame_gen)
        yield frame

        yield from frame_gen

        yield frame

    return _frame_gen()


@pytest.fixture
def buffered_nii(img_arr):
    aff = np.eye(4)
    return nib.nifti2.Nifti2Image(img_arr, aff)


@pytest.fixture
def streaming_nii_path(tmp_path):
    return tmp_path / "streaming.nii"


@pytest.fixture
def streaming_nii_gz_path(tmp_path):
    return tmp_path / "streaming.nii.gz"


@pytest.fixture
def buffered_nii_path(tmp_path, buffered_nii):
    _buffered_path = tmp_path / "buffered.nii"
    buffered_nii.to_filename(_buffered_path)
    return _buffered_path


@pytest.fixture
def buffered_nii_gz_path(tmp_path, buffered_nii):
    _buffered_path = tmp_path / "buffered.nii.gz"
    buffered_nii.to_filename(_buffered_path)
    return _buffered_path
