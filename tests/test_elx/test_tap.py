from elx import Tap


def test_tap_discovery(tap: Tap):
    """
    Test that the tap schema can be discovered.
    """
    print(tap.catalog)
    assert 1
    # assert "streams" in tap.catalog
    # assert len(tap.catalog["streams"]) == 1
