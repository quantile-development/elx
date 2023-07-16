from typing import List
from elx import Runner, Tap, Target
from elx.catalog import Stream
from dagster import AssetsDefinition, asset
from elx.extensions.dagster.utils import dagster_safe_name, generate_description

def load_assets(runner: Runner) -> List[AssetsDefinition]:
    def run_factory(runner: Runner, stream: Stream):
        def run(context):
            runner.run(stream=stream.name)
            return dagster_safe_name(stream.name)

        return run

    return [
        asset(
            name=dagster_safe_name(stream.name),
            description=generate_description(runner=runner, stream=stream),
            group_name=dagster_safe_name(runner.tap.executable),
        )(run_factory(runner, stream))
        for stream 
        in runner.tap.streams
    ]