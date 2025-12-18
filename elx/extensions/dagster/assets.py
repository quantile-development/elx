from typing import Generator, Iterable, List, Mapping, Sequence
from elx import Runner
from dagster import (
    AssetsDefinition,
    Nothing,
    OpExecutionContext,
    Output,
    multi_asset,
    AssetOut,
    SourceAsset,
    AssetKey,
    AssetDep,
    get_dagster_logger,
)
from elx.extensions.dagster.utils import dagster_safe_name, generate_description

logger = get_dagster_logger()


def load_assets(
    runner: Runner,
    deps: Iterable[AssetKey | str | Sequence[str] | AssetsDefinition | SourceAsset | AssetDep] | None = None,
    key_prefix: str | Sequence[str] | None = None,
    group_name: str | None = None,
) -> List[AssetsDefinition]:
    """
    Load the assets for a runner, each asset represents one tap target combination.

    Args:
        runner (Runner): The runner to extract from.
        deps (Iterable[AssetKey | str | Sequence[str] | AssetsDefinition | SourceAsset | AssetDep] | None): Upstream assets upon which the assets depend.
        key_prefix (str | Sequence[str] | None): Key prefix for the assets. If not provided, defaults to the tap executable name.
        group_name (str | None): Group name for the assets. If not provided, defaults to the tap executable name.

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
            # Build a mapping from dagster-safe names back to original stream names
            stream_name_mapping = {
                dagster_safe_name(stream.name): stream.name
                for stream in runner.tap.catalog.streams
                if stream.is_selected
            }

            # Execute the runner and yield the selected outputs.
            runner.run(
                streams=list(context.selected_output_names),
                logger=logger,
            )

            for context_output_name in context.selected_output_names:
                # Get the original stream name to look up the row count
                original_stream_name = stream_name_mapping.get(
                    context_output_name, context_output_name
                )
                row_count = runner.record_counts.get(original_stream_name, 0)

                yield Output(
                    value=Nothing,
                    output_name=context_output_name,
                    metadata={
                        "state_path": f"{runner.state_manager.base_path}/{runner.state_file_name}",
                        "state": runner.load_state(),
                        "row_count": row_count,
                    },
                )

        return run

    return [
        multi_asset(
            name=f"run_{dagster_safe_name(runner.tap.executable)}_{dagster_safe_name(runner.target.executable)}",
            deps=deps,
            outs={
                dagster_safe_name(stream.name): AssetOut(
                    is_required=False,
                    description=generate_description(runner=runner, stream=stream),
                    key_prefix=key_prefix or dagster_safe_name(runner.tap.executable),
                    code_version=runner.tap.hash_key,
                )
                for stream in runner.tap.catalog.streams
                if stream.is_selected
            },
            can_subset=True,
            group_name=group_name or dagster_safe_name(runner.tap.executable),
            compute_kind="python",
        )(run_factory(runner))
    ]
