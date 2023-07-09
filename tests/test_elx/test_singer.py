import json
from pathlib import Path
import subprocess
import pytest
from elx.singer import Singer


def pipx_uninstall(executable: str) -> None:
    """
    Uninstall a package using pipx.

    Args:
        executable (str): The name of the package to uninstall.
    """
    subprocess.run(
        [
            "pipx",
            "uninstall",
            executable,
        ],
        check=False,
    )


def test_singer_can_install(singer: Singer):
    """
    Test that the singer executable is installed.
    """
    # Uninstall the singer executable if it is installed.
    pipx_uninstall(singer.executable)
    # Assert that the singer executable is not installed.
    assert not singer.is_installed
    # Install the singer executable.
    singer.install()
    # Assert that the singer executable is installed.
    assert singer.is_installed


def test_singer_can_run(singer: Singer):
    """
    Test that the singer executable can run.
    """
    # Install the executable if it is not installed.
    if not singer.is_installed:
        singer.install()

    # Run the executable.
    with pytest.raises(json.decoder.JSONDecodeError):
        singer.run(["--version"])


def test_singer_config(singer: Singer):
    """
    Test that the content of the config file is
    the same as the config attribute.
    """
    # Open the config file and load the contents.
    with open(singer.config_path, "r") as f:
        config = json.load(f)
        assert config == singer.config
