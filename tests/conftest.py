""" Shared fixtures and test control flow logic
"""

import json
import shutil

import pytest
# in 3.9 and above files is provided by importlib.resources
from importlib_resources import files

DATA_DRIVE = "D:"

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


@pytest.fixture
def single_plane_acquisition(tmp_user, test_data_path):
    """Copies over some test data and returns the path"""
    reserve_acq_path = test_data_path / "test_acquisitions/single_plane"
    test_acq_path = tmp_user / "single_plane"
    shutil.copy(reserve_acq_path, test_acq_path)

    return test_acq_path


@pytest.fixture
def volume_acquisition(tmp_user, test_data_path):
    """Copies over some test data and returns the path"""
    reserve_acq_path = test_data_path / "test_acquisitions/volume"
    test_acq_path = tmp_user / "volume"
    shutil.copy(reserve_acq_path, test_acq_path)

    return test_acq_path
