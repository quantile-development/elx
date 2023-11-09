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
