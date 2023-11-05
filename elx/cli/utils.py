import imp
from pathlib import Path


def load_variables_from_path(path: Path) -> dict:
    """
    Load all variables from a path.

    Args:
        path (Path): The path to load variables from.

    Returns:
        dict: A dictionary of variables.
    """
    file = imp.load_source(name="file", pathname=str(path))

    # Get all variables from foo
    variables = vars(file)

    return variables
