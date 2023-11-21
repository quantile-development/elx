from elx import Runner
from elx.extensions.dagster import load_assets
from dagster import AssetsDefinition


def test_asset_loading(runner: Runner):
    """
    Test that assets are loaded correctly.
    """
    assets = load_assets(runner)
    assert len(assets) == 1
    assert isinstance(assets[0], AssetsDefinition)


def test_asset_loading_with_deselected_stream(runner_with_deselected_stream: Runner):
    """
    Test that assets are loaded correctly.
    """
    # Verifies that the associated tap has multiple streams
    assert len(runner_with_deselected_stream.tap.catalog.streams) == 2

    assets = load_assets(runner_with_deselected_stream)
    assert len(assets) == 1
    assert isinstance(assets[0], AssetsDefinition)
