from typing import Generator
import pytest
from elx import Tap, Target, Runner, StateManager
from .tap import tap
from .target import target


@pytest.fixture
def runner(tmp_path, tap: Tap, target: Target) -> Generator[Runner, None, None]:
    """
    Return a Runner instance for the tap-smoke-test executable.
    """
    yield Runner(
        tap=tap,
        target=target,
        state_manager=StateManager(base_path=str(tmp_path)),
    )


@pytest.fixture
def runner_with_deselected_stream(
    tmp_path, tap_multiple_streams: Tap, target: Target
) -> Generator[Runner, None, None]:
    """
    Return a Runner instance for the tap-smoke-test executable with two streams,
    of which one is deselected.
    """
    yield Runner(
        tap=tap_multiple_streams,
        target=target,
        state_manager=StateManager(base_path=str(tmp_path)),
    )
