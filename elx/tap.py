from functools import cached_property
import logging
from pathlib import Path
from elx.singer import Singer
from elx.catalog import Catalog


class Tap(Singer):
    @cached_property
    def catalog(self) -> Catalog:
        logging.info(f"Discovering {self.executable} with {self.config_path}")
        catalog_dictionary = self.run(["--config", str(self.config_path), "--discover"])
        return Catalog.parse_obj(catalog_dictionary)
