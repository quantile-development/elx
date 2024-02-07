from typing import Dict
import inquirer
from elx.tap import Tap
from elx.cli.utils import find_instances_of_type, request_instance
from rich import print_json


def catalog(
    locator: str,
    tap: str = None,
):
    """
    Get the catalog of a tap.

    Args:
        locator (str): The locator to the module or path to file.
        tap (str): A default tap to select.
    """
    instances = list(find_instances_of_type(locator, Tap))

    if len(instances) == 0:
        print("No taps found.")
        return

    tap = request_instance(
        instances=instances,
        default_name=tap,
        message="Which tap do you want to catalog?",
    )

    print_json(data=tap.catalog.dict(by_alias=True))
