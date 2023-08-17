import json
import logging
import subprocess
import hashlib
from distutils.spawn import find_executable
from functools import cached_property
from typing import Generator, Optional
from pipx.commands.install import install
from pipx.commands.uninstall import uninstall
from pipx.commands.common import package_name_from_spec
from pipx.constants import LOCAL_BIN_DIR
from elx.exceptions import DecodeException, PipxInstallException
from elx.utils import require_install, interpolate_in_config

PYTHON = "python3"


class Singer:
    def __init__(
        self,
        spec: str,
        executable: Optional[str] = None,
        config: dict = {},
    ):
        self.spec = spec
        self._executable = executable
        self._config = config

    @property
    def config(self) -> dict:
        """
        Get the config for this plugin.
        """
        config = self._config

        # If the config is a callable, call it and return the result.
        if callable(self._config):
            config = self._config()

        # If there is a runner attribute, interpolate the config.
        if hasattr(self, "runner"):
            config = interpolate_in_config(
                config=config,
                interpolation=self.runner.interpolation_values,
            )

        return config

    @cached_property
    def executable(self) -> str:
        """
        Get the package name for this plugin.
        """
        if self._executable:
            return self._executable

        return package_name_from_spec(
            package_spec=self.spec,
            python=PYTHON,
            pip_args=[],
            verbose=False,
        )

    @cached_property
    def hash_key(self) -> str:
        """
        Generate a md5 hash key for this plugin, based on the executable, spec and config.
        """
        return hashlib.md5(
            json.dumps(
                {
                    "executable": self.executable,
                    "spec": self.spec,
                    "config": self.config,
                }
            ).encode()
        ).hexdigest()

    @property
    def is_installed(self) -> bool:
        """
        Check if the executable is installed.

        Returns:
            bool: True if the executable is installed, False otherwise.
        """
        return find_executable(self.executable) is not None

    def install(self) -> None:
        """
        Install the executable using pipx.
        """
        logging.info(f"Installing {self.executable}...")

        try:
            subprocess.run(
                [
                    "pipx",
                    "install",
                    self.spec or self.executable,
                    "--force",
                ],
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise PipxInstallException(e.stderr.decode())

    @require_install
    def run(self, args: list) -> dict:
        """
        Run the executable with the given arguments.

        Args:
            args (list): The arguments to pass to the executable.

        Returns:
            dict: The JSON output of the executable.

        Raises:
            DecodeException: If the JSON output of the executable is not valid.
        """
        result = subprocess.Popen(
            [
                self.executable,
                *args,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stdout, stderr = result.communicate()

        # If any of the processes exited with a non-zero exit code,
        # raise an exception.
        if result.returncode != 0:
            raise DecodeException(f"Error running {self.executable}: {stderr.decode()}")

        # Else try to parse the JSON output.
        try:
            return json.loads(stdout)
        except json.decoder.JSONDecodeError as e:
            raise DecodeException(
                f"Error parsing json: {e.msg} at position {e.pos} in {e.doc} \n\n {stderr.decode()}"
            )
