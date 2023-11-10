import asyncio
import pytest
from elx import Tap
from elx.catalog import Stream, Catalog


def test_tap_discovery(tap: Tap):
    """
    Test that the tap schema can be discovered.
    """
    # Make sure the catalog is of the right type.
    assert type(tap.catalog) == Catalog
    # Make sure the catalog has the right number of streams.
    assert len(tap.catalog.streams) == 1
    # Make sure the streams are of the right type.
    assert type(tap.catalog.streams[0]) == Stream


@pytest.mark.asyncio
async def test_tap_process(tap: Tap):
    """
    Test that the tap process can be run.
    """
    async with tap.process() as process:
        # Make sure the tap process is of the right type.
        assert type(process) == asyncio.subprocess.Process
