from typing import List
from elx import Runner, Tap, Target
from elx.catalog import Stream
from dagster import AssetsDefinition, asset
from elx.extensions.dagster.utils import dagster_safe_name

def load_assets(runner: Runner) -> List[AssetsDefinition]:
    def run_factory(runner: Runner, stream: Stream):
        def run(context):
            runner.run()
            return dagster_safe_name(stream.name)

        return run

    return [
        asset(
            name=dagster_safe_name(stream.name),
            description=f"Load {stream.name} from {runner.tap.executable} into {runner.target.executable}.",
        )(run_factory(runner, stream))
        for stream 
        in runner.tap.streams
    ]