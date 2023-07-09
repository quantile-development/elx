from subprocess import Popen
from elx import Tap
from elx.catalog import Catalog


def test_tap_discovery(tap: Tap):
    """
    Test that the tap schema can be discovered.
    """
    # Make sure the catalog is of the right type.
    assert type(tap.catalog) == dict
    # Make sure the catalog has the right number of streams.
    assert len(tap.catalog["streams"]) == 1


def test_tap_process(tap: Tap):
    """
    Test that the tap process can be run.
    """
    with tap.process() as process:
        # Make sure the tap process is of the right type.
        assert type(process) == Popen
        # Make sure the tap process is running.
        assert process.poll() is None
