import logging
import contextlib
from functools import cached_property
from pathlib import Path
from typing import Generator, Optional
from elx.singer import Singer, require_install
from elx.catalog import Catalog
from elx.json_temp_file import json_temp_file
from subprocess import Popen, PIPE


class Tap(Singer):
    def discover(self, config_path: Path) -> dict:
        """
        Run the tap in discovery mode.

        Args:
            config_path (Path): Path to the config file.

        Returns:
            dict: The catalog.
        """
        logging.debug(f"Discovering {self.executable} with {config_path}")
        return self.run(["--config", str(config_path), "--discover"])

    @cached_property
    def catalog(self) -> dict:
        """
        Discover the catalog.

        Returns:
            Catalog: The catalog as a Pydantic model.
        """
        with json_temp_file(self.config) as config_path:
            return self.discover(config_path)
            # return Catalog.parse_obj(catalog)

    @contextlib.contextmanager
    @require_install
    def process(
        self,
        state: dict = {},
    ) -> Generator[Popen, None, None]:
        """
        Run the tap process.

        Returns:
            Popen: The tap process.
        """
        logging.debug(f"Using state: {state}")

        with json_temp_file(self.config) as config_path:
            with json_temp_file(self.catalog) as catalog_path:
                with json_temp_file(state) as state_path:
                    yield Popen(
                        [
                            self.executable,
                            "--config",
                            str(config_path),
                            "--catalog",
                            str(catalog_path),
                            "--state",
                            str(state_path),
                        ],
                        stdout=PIPE,
                        stderr=PIPE,
                    )
