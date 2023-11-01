import asyncio
import pytest
from subprocess import Popen
from elx import Tap
from elx.catalog import Stream


def test_tap_discovery(tap: Tap):
    """
    Test that the tap schema can be discovered.
    """
    # Make sure the catalog is of the right type.
    assert type(tap.catalog) == dict
    # Make sure the catalog has the right number of streams.
    assert len(tap.catalog["streams"]) == 1


@pytest.mark.asyncio
async def test_tap_process(tap: Tap):
    """
    Test that the tap process can be run.
    """
    async with tap.process() as process:
        # Make sure the tap process is of the right type.
        assert type(process) == asyncio.subprocess.Process


def test_tap_streams(tap: Tap):
    """
    Test that the tap streams can be retrieved.
    """
    # Make sure the streams are of the right type.
    assert type(tap.streams) == list
    # Make sure the streams have the right number of streams.
    assert len(tap.streams) == 1
    # Make sure the streams are of the right type.
    assert type(tap.streams[0]) == Stream
