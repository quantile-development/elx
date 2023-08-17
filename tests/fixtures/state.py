from typing import Generator
import pytest
from elx import StateManager


@pytest.fixture
def state_manager(tmp_path) -> Generator[StateManager, None, None]:
    """
    Return a StateManager instance.
    """
    yield StateManager(
        base_path=str(tmp_path),
    )
