from typing import Generator
import pytest
from elx import Tap


@pytest.fixture()
def tap() -> Generator[Tap, None, None]:
    """
    Return a Tap instance for the  executable with an incremental stream.
    """
    yield Tap(
        executable="tap-mock-fixture",
        spec="git+https://github.com/quantile-taps/tap-mock-fixture.git",
        config={},
        replication_keys={"users": "updated_at"},
    )
