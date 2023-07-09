import json
import shutil
import subprocess
import tempfile
import hashlib
from functools import cached_property
from pathlib import Path
from typing import Optional
from pipx.commands.install import install
from pipx.commands.uninstall import uninstall
from pipx.constants import LOCAL_BIN_DIR


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

    @cached_property
    def config_path(self) -> Path:
        with tempfile.NamedTemporaryFile(
            delete=False,
            prefix=self.executable,
            suffix="config.json",
        ) as config_file:
            config_file.write(json.dumps(self.config).encode())
            return Path(config_file.name)

    def run(self, args: list) -> dict:
        """
        Run the executable with the given arguments.

        Args:
            args (list): The arguments to pass to the executable.

        Returns:
            dict: The JSON output of the executable.
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
        return json.loads(stdout)
