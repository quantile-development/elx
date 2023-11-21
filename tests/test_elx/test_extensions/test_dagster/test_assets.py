from elx import Runner
from elx.extensions.dagster import load_assets
from dagster import AssetsDefinition


def test_asset_loading(runner: Runner):
    """
    Test that assets are loaded correctly.
    """
    # Verifies that the tap associated with the runner has 2 streams
    assert len(runner.tap.catalog.streams) == 2

    # Load assets
    assets = load_assets(runner)

    # Length of assets should be 1 as one stream is deselected per default
    assert len(assets) == 1
    assert isinstance(assets[0], AssetsDefinition)
