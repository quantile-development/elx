import json
import shutil
import subprocess
from typing import Optional
from pipx.commands.install import install
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

    @property
    def is_installed(self) -> bool:
        return shutil.which(self.executable) is not None

    def install(self) -> None:
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

    def run(self, args: list) -> dict:
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
