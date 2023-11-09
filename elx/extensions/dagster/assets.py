from typing import Generator, List
from elx import Runner
from dagster import (
    AssetsDefinition,
    Nothing,
    OpExecutionContext,
    Output,
    multi_asset,
    AssetOut,
    get_dagster_logger,
)
from elx.extensions.dagster.utils import dagster_safe_name, generate_description

logger = get_dagster_logger()


def load_assets(runner: Runner) -> List[AssetsDefinition]:
    """
    Load the assets for a runner, each asset represents one tap target combination.

    Args:
        runner (Runner): The runner to extract from.

    Returns:
        List[AssetsDefinition]: The assets.
    """

    def run_factory(runner: Runner) -> callable:
        """
        Create a run function for a runner.

        Args:
            runner (Runner): The runner to create a run function for.

        Returns:
            callable: The run function that gets executed by Dagster.
        """

        def run(context: OpExecutionContext) -> Generator[Output, None, None]:
            """
            Run a tap target combination.

            Args:
                context (OpExecutionContext): The context to run in.

            Yields:
                Generator[Output, None, None]: The names of the selected outputs.
            """
            # Execute the runner and yield the selected outputs.
            runner.run(
                streams=list(context.selected_output_names),
                logger=logger,
            )

            for context_output_name in context.selected_output_names:
                yield Output(
                    value=Nothing,
                    output_name=context_output_name,
                    metadata={
                        "state_path": f"{runner.state_manager.base_path}/{runner.state_file_name}",
                        "state": runner.load_state(),
                    },
                )

        return run

    return [
        multi_asset(
            name=f"run_{dagster_safe_name(runner.tap.executable)}_{dagster_safe_name(runner.target.executable)}",
            outs={
                dagster_safe_name(stream.name): AssetOut(
                    is_required=False,
                    description=generate_description(runner=runner, stream=stream),
                    key_prefix=dagster_safe_name(runner.tap.executable),
                    code_version=runner.tap.hash_key,
                )
                for stream in runner.tap.catalog.streams
            },
            can_subset=True,
            group_name=dagster_safe_name(runner.tap.executable),
            compute_kind="python",
        )(run_factory(runner))
    ]
