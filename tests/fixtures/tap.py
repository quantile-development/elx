from typing import Generator
import pytest
from elx import Tap


@pytest.fixture
def tap() -> Generator[Tap, None, None]:
    """
    Return a Tap instance for the tap-smoke-test executable.
    """
    yield Tap(
        executable="tap-smoke-test",
        spec="git+https://github.com/meltano/tap-smoke-test.git",
        config={
            "streams": [
                {
                    "stream_name": "animals",
                    "input_filename": "https://gitlab.com/meltano/tap-smoke-test/-/raw/main/demo-data/animals-data.jsonl",
                },
            ],
        },
    )
