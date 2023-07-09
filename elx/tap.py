import json
import logging
import contextlib
from functools import cached_property
from pathlib import Path
import re
import tempfile
from typing import Generator
from elx.singer import Singer, require_install
from elx.catalog import Catalog
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
        logging.info(f"Discovering {self.executable} with {config_path}")
        print(f"Discovering {self.executable} with {config_path}")
        return self.run(["--config", str(config_path), "--discover"])

    @cached_property
    def catalog(self) -> dict:
        """
        Discover the catalog.

        Returns:
            Catalog: The catalog as a Pydantic model.
        """
        with self.configured() as config_path:
            return self.discover(config_path)
            # return Catalog.parse_obj(catalog)

    @contextlib.contextmanager
    def discovered(self) -> Generator[Path, None, None]:
        """
        Write the catalog to a temporary file.

        Yields:
            Path: Path to the catalog.
        """
        try:
            # Use tempfile to create a temporary config file.
            with tempfile.NamedTemporaryFile(
                mode="w",
                prefix=self.executable,
                suffix="catalog.json",
                delete=False,
            ) as catalog_file:
                # Write the config attribute to the config file.
                catalog_file.write(json.dumps(self.catalog))
            # Yield the path to the config file.
            yield Path(catalog_file.name)
        finally:
            # Always delete the catalog file.
            Path(catalog_file.name).unlink()

    @contextlib.contextmanager
    @require_install
    def process(self) -> Generator[Popen, None, None]:
        """
        Run the tap process.

        Returns:
            Popen: The tap process.
        """
        with self.configured() as config_path:
            with self.discovered() as catalog_path:
                yield Popen(
                    [
                        self.executable,
                        "--config",
                        str(config_path),
                        "--catalog",
                        str(catalog_path),
                    ],
                    stdout=PIPE,
                    stderr=PIPE,
                )
