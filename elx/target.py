from subprocess import PIPE, Popen
from typing import Generator, Optional
from elx.singer import Singer, require_install
from elx.json_temp_file import json_temp_file
import contextlib


class Target(Singer):
    @contextlib.contextmanager
    @require_install
    def process(
        self,
        tap_process: Popen,
        config_interpolation: Optional[dict] = {},
    ) -> Generator[Popen, None, None]:
        """
        Run the tap process.

        Args:
            tap_process (Popen): The process where the tap is running.
                Is used to pipe the output to the target.
            config_interpolation (Optional[dict], optional): Values that can be
                used in the config of the tap or target. Defaults to {}.

        Returns:
            Popen: The tap process.
        """
        with json_temp_file(self.config) as config_path:
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
