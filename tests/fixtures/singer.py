from typing import Generator
import pytest
from elx.singer import Singer


@pytest.fixture(params=["tap-mock-fixture", None])
def singer(request) -> Generator[Singer, None, None]:
    """
    Return a Singer instance for the tap-mock-fixture executable.
    """
    yield Singer(
        # Test with and without an executable.
        executable=request.param,
        spec="git+https://github.com/quantile-taps/tap-mock-fixture.git",
        config={},
    )
