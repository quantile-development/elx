from functools import lru_cache
import logging
from pathlib import Path
from elx.singer import Singer


class Tap(Singer):
    @lru_cache
    def discover(self, config_path: Path) -> dict:
        logging.info(f"Discovering {self.executable} with {config_path}")
        return self.run(["--config", str(config_path), "--discover"])
