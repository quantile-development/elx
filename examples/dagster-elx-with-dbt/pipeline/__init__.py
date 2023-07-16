from elx.extensions.dagster import load_assets
from elx import Tap, Target, Runner, StateManager
from dagster import Definitions

tap = Tap(
    executable="tap-smoke-test",
    spec="git+https://github.com/meltano/tap-smoke-test.git",
    config={
        "streams": [
            {
                "stream_name": "stream-one",
                "input_filename": "https://gitlab.com/meltano/tap-smoke-test/-/raw/main/demo-data/animals-data.jsonl",
            },
            {
                "stream_name": "stream-two",
                "input_filename": "https://gitlab.com/meltano/tap-smoke-test/-/raw/main/demo-data/animals-data.jsonl",
            },
        ],
    },
)
target = Target(
    spec="git+https://github.com/estrategiahq/target-parquet.git",
    executable="target-parquet",
    config={
        "destination_path": "/tmp",
    },
)
runner = Runner(tap, target, StateManager("/tmp"))

defs = Definitions(
    assets=load_assets(runner),
)