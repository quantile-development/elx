import asyncio
import logging
import contextlib
from functools import cached_property
from pathlib import Path
from typing import Generator, List, Optional
from elx.singer import Singer, require_install
from elx.catalog import Stream
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
            catalog = self.discover(config_path)
            return catalog

    def filtered_catalog(
        self,
        catalog: dict,
        streams: Optional[List[str]] = None,
    ) -> dict:
        """
        Filter the catalog.

        Args:
            catalog (dict): The catalog.
            streams (Optional[List[str]], optional): The streams to filter on. Defaults to None.

        Returns:
            dict: The filtered catalog.
        """
        if not streams:
            return catalog

        return {
            "streams": [
                {
                    **stream,
                    "selected": stream["tap_stream_id"] in streams,
                    "metadata": stream["metadata"]
                    + [
                        {
                            "metadata": {
                                "selected": stream["tap_stream_id"] in streams,
                            },
                            "breadcrumb": [],
                        }
                    ],
                }
                for stream in catalog["streams"]
            ]
        }

    @property
    def streams(self) -> list[Stream]:
        """
        Get the streams from the catalog.

        Returns:
            list[Stream]: The streams.
        """
        return [Stream(**stream) for stream in self.catalog["streams"]]

    @contextlib.asynccontextmanager
    @require_install
    async def process(
        self,
        state: dict = {},
        streams: Optional[List[str]] = None,
    ) -> Generator[Popen, None, None]:
        """
        Run the tap process.

        Returns:
            Popen: The tap process.
        """
        catalog = self.filtered_catalog(catalog=self.catalog, streams=streams)

        with json_temp_file(self.config) as config_path:
            with json_temp_file(catalog) as catalog_path:
                with json_temp_file(state) as state_path:
                    yield await asyncio.create_subprocess_exec(
                        *[
                            self.executable,
                            "--config",
                            str(config_path),
                            "--catalog",
                            str(catalog_path),
                            "--state",
                            str(state_path),
                        ],
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )

    def invoke(
        self,
        streams: Optional[List[str]] = None,
        limit: int = None,
    ) -> None:
        """
        Invoke the tap.

        Args:
            streams (Optional[List[str]], optional): The streams to invoke. Defaults to None.
        """
        # TODO: Make use of the process context manager.

        catalog = self.filtered_catalog(catalog=self.catalog, streams=streams)

        with json_temp_file(self.config) as config_path:
            with json_temp_file(catalog) as catalog_path:
                with json_temp_file({}) as state_path:
                    process = Popen(
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

                    n_lines = 0

                    for line in process.stdout:
                        if limit and n_lines >= limit:
                            break
                        print(line.decode("utf-8"))
                        n_lines += 1
