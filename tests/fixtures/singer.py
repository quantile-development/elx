from typing import Generator
import pytest
from elx.singer import Singer


@pytest.fixture(params=["tap-smoke-test", None])
def singer(request) -> Generator[Singer, None, None]:
    """
    Return a Singer instance for the tap-smoke-test executable.
    """
    yield Singer(
        # Test with and without an executable.
        executable=request.param,
        spec="git+https://github.com/meltano/tap-smoke-test.git",
        config={
            "streams": [
                {
                    "stream_name": "users",
                    "input_filename": "https://gitlab.com/meltano/tap-smoke-test/-/raw/main/demo-data/animals-data.jsonl",
                },
            ],
        },
    )
