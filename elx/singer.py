import json
import shutil
import subprocess
import tempfile
import hashlib
import contextlib
from functools import cached_property
from pathlib import Path
from typing import Generator, Optional
from pipx.commands.install import install
from pipx.commands.uninstall import uninstall
from pipx.constants import LOCAL_BIN_DIR
from elx.exceptions import DecodeException


def require_install(func):
    """
    Decorator to check if the executable is installed.
    """

    def wrapper(self: "Singer", *args, **kwargs):
        if not self.is_installed:
            self.install()

        return func(self, *args, **kwargs)

    return wrapper


class Singer:
    def __init__(
        self,
        executable: str,
        spec: Optional[str] = None,
        config: dict = {},
    ):
        self.executable = executable
        self.spec = spec
        self.config = config

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
        return shutil.which(self.executable) is not None

    def install(self) -> None:
        """
        Install the executable using pipx.
        """
        install(
            venv_dir=None,
            package_name=self.executable,
            package_spec=self.spec or self.executable,
            local_bin_dir=LOCAL_BIN_DIR,
            python="python3",
            pip_args=[],
            venv_args=[],
            verbose=False,
            force=False,
            include_dependencies=False,
        )

    @contextlib.contextmanager
    def configured(self) -> Generator[Path, None, None]:
        """
        Create a temporary config file with the config attribute as content.

        Yields:
            Path: The path to the temporary config file.
        """
        try:
            # Use tempfile to create a temporary config file.
            with tempfile.NamedTemporaryFile(
                mode="w",
                prefix=self.executable,
                suffix="config.json",
                delete=False,
            ) as config_file:
                # Write the config attribute to the config file.
                config_file.write(json.dumps(self.config))
                # Yield the path to the config file.
            yield Path(config_file.name)
        finally:
            Path(config_file.name).unlink()

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
        try:
            return json.loads(stdout)
        except json.decoder.JSONDecodeError as e:
            raise DecodeException(
                f"Error parsing json: {e.msg} at position {e.pos} in {e.doc}"
            )
