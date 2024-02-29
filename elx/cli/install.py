import logging
from elx import Tap, Target
from elx.cli.utils import find_instances_of_type
from rich.table import Column
from rich.progress import (
    Progress,
    TextColumn,
    SpinnerColumn,
    TimeElapsedColumn,
)

logger = logging.getLogger()
logger.disabled = True


def install(
    locator: str,
):
    """
    Install all taps and targets for a certain location.

    Args:
        locator (str): The locator to the module or path to file.
    """
    taps = list(find_instances_of_type(locator, Tap))
    targets = list(find_instances_of_type(locator, Target))

    instances = taps + targets

    if len(instances) == 0:
        print("No taps or targets found.")
        return

    spinner_column = SpinnerColumn()
    text_column = TextColumn(
        "Installing {task.description}", table_column=Column(ratio=1)
    )
    time_column = TimeElapsedColumn()
    progress = Progress(
        spinner_column,
        text_column,
        time_column,
        transient=False,
        redirect_stdout=False,
        redirect_stderr=False,
    )

    with progress:
        for instance in instances:
            task_id = progress.add_task(instance.name, total=None)
            instance.install()
            progress.stop_task(task_id)
