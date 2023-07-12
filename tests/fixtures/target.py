from typing import Generator
import pytest
from elx import Target


@pytest.fixture
def target(tmp_path) -> Generator[Target, None, None]:
    """
    Return a Target instance for the target-smoke-test executable.
    """
    yield Target(
        "target-jsonl",
        config={
            "destination_path": str(tmp_path),
        },
    )
