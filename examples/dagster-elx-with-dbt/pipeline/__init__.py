from elx.extensions.dagster import load_assets
from elx import Tap, Target, Runner, StateManager
from dagster import Definitions, file_relative_path
from dagster_dbt import load_assets_from_dbt_project, DbtCliClientResource

DBT_PROJECT_PATH = file_relative_path(__file__, "../dbt")
DBT_PROFILES = file_relative_path(__file__, "../dbt")

# Tap smoke test to ingest some sample data.
tap = Tap(
    spec="git+https://github.com/meltano/tap-smoke-test.git",
    executable="tap-smoke-test",
    config={
        "streams": [
            {
                "stream_name": "stream_one",
                "input_filename": "https://gitlab.com/meltano/tap-smoke-test/-/raw/main/demo-data/animals-data.jsonl",
            },
            {
                "stream_name": "stream_two",
                "input_filename": "https://gitlab.com/meltano/tap-smoke-test/-/raw/main/demo-data/animals-data.jsonl",
            },
        ],
    },
)

# We send the data to parquet files in the temp directory.
target = Target(
    spec="git+https://github.com/estrategiahq/target-parquet.git",
    executable="target-parquet",
    config={
        "destination_path": "/tmp",
    },
)

# Connect the tap to the target.
runner = Runner(tap, target, StateManager("/tmp"))

# Load the assets from the tap and target.
elx_assets = load_assets(runner)

# Load the assets from the dbt project.
dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES,
    key_prefix="dbt",
    use_build_command=True,
)

# Define the DBT cli resource.
resources = {
    "dbt": DbtCliClientResource(
        project_dir=DBT_PROJECT_PATH,
        profiles_dir=DBT_PROFILES,
    ),
}

# Define the Dagster definitions, this gets loaded into Dagit.
defs = Definitions(
    assets=[*elx_assets, *dbt_assets],
    resources=resources,
)
