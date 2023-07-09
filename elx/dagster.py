from typing import List
from elx import Tap
from dagster import multi_asset, AssetsDefinition, AssetOut, Output


def load_assets(tap: Tap) -> List[AssetsDefinition]:
    def compute_fn(_):
        yield Output("Hello, world!", "animals")
        yield Output("Hello, world!", "animals_two")

    return [
        multi_asset(
            outs={
                stream.safe_name: AssetOut(
                    is_required=False,
                    code_version=tap.hash_key,
                )
                for stream in tap.catalog.streams
            },
            group_name="tap_smoke_test",
            can_subset=True,
            compute_kind="python",
        )(compute_fn),
    ]
