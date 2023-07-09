from elx import load_assets, Tap
from dagster import Definitions

tap = Tap(
    executable="tap-smoke-test",
    spec="git+https://github.com/meltano/tap-smoke-test.git",
    config={
        "streams": [
            {
                "stream_name": "animals",
                "input_filename": "https://gitlab.com/meltano/tap-smoke-test/-/raw/main/demo-data/animals-data.jsonl",
            },
            {
                "stream_name": "animals-two",
                "input_filename": "https://gitlab.com/meltano/tap-smoke-test/-/raw/main/demo-data/animals-data.jsonl",
            },
        ],
    },
)

defs = Definitions(
    assets=load_assets(tap),
)
