from typing import Generator
import pytest
from elx import Tap


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
def tap_incremental() -> Generator[Tap, None, None]:
    """
    Return a Tap instance for the  executable with an incremental stream.
    """
    yield Tap(
        executable="tap-mock-incremental",
        spec="git+https://github.com/quantile-taps/tap-mock-incremental.git",
        config={},
    )
