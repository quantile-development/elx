from functools import cached_property
from subprocess import PIPE, Popen
from typing import Generator
from elx.singer import Singer, require_install
import contextlib


class Target(Singer):
    @contextlib.contextmanager
    @require_install
    def process(self, tap_process: Popen) -> Generator[Popen, None, None]:
        """
        Run the tap process.

        Returns:
            Popen: The tap process.
        """
        with self.configured() as config_path:
            yield Popen(
                [
                    self.executable,
                    "--config",
                    str(config_path),
                ],
                stdin=tap_process.stdout,
                stdout=PIPE,
                stderr=PIPE,
            )
