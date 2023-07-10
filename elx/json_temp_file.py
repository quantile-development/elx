import contextlib
import json
from pathlib import Path
import tempfile
from typing import Generator


@contextlib.contextmanager
def json_temp_file(content: dict) -> Generator[Path, None, None]:
    """
    Write a json object to a temporary file. The file is deleted after the
    context manager exits.

    Args:
        content (dict): The object to write to the file.

    Yields:
        Path: Path to the catalog.
    """
    try:
        # Use tempfile to create a temporary config file.
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            delete=False,
        ) as catalog_file:
            # Write the config attribute to the config file.
            catalog_file.write(json.dumps(content))
        # Yield the path to the config file.
        yield Path(catalog_file.name)
    finally:
        # Always delete the catalog file.
        Path(catalog_file.name).unlink()
