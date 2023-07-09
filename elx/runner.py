import json
import logging
from pathlib import Path
import subprocess
from elx.tap import Tap
from elx.target import Target
from elx import StateManager
import tempfile
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)


class Runner:
    def __init__(
        self,
        tap: Tap,
        target: Target,
        state_manager: StateManager = StateManager(),
    ):
        load_dotenv()
        self.tap = tap
        self.target = target
        self.state_manager = state_manager

    @property
    def state_file_name(self) -> str:
        return f"{self.tap.executable}-{self.target.executable}.json"

    def save_state(self, state: dict) -> None:
        self.state_manager.save(self.state_file_name, state)

    def run(self) -> None:
        with self.tap.process() as tap_process:
            with self.target.process(tap_process=tap_process) as target_process:
                for line in iter(tap_process.stderr.readline, b""):
                    print(line.decode("utf-8"))

                tap_process.stdout.close()
                stdout, stderr = target_process.communicate()

                logging.info(f"STDOUT: {stdout}")
                logging.info(f"STDERR: {stderr}")

                state = json.loads(stdout.decode("utf-8"))
                self.save_state(state)

        # with tempfile.TemporaryDirectory() as tmpdir:
        #     tap_config_path = Path(tmpdir) / "tap_config.json"
        #     with open(tap_config_path, "w") as f:
        #         json.dump(self.tap.config, f)

        #     catalog = self.tap.discover(tap_config_path)
        #     catalog_path = Path(tmpdir) / "catalog.json"
        #     with open(catalog_path, "w") as f:
        #         json.dump(catalog, f)

        #     target_config_path = Path(tmpdir) / "target_config.json"
        #     with open(target_config_path, "w") as f:
        #         json.dump(self.target.config, f)

        #     tap_process = subprocess.Popen(
        #         [
        #             self.tap.executable,
        #             "--config",
        #             str(tap_config_path),
        #             "--catalog",
        #             str(catalog_path),
        #         ],
        #         stdout=subprocess.PIPE,
        #         stderr=subprocess.PIPE,
        #     )

        #     for line in iter(tap_process.stderr.readline, b""):
        #         print(line.decode("utf-8"))

        #     target_process = subprocess.Popen(
        #         [
        #             self.target.executable,
        #             "--config",
        #             str(target_config_path),
        #         ],
        #         stdin=tap_process.stdout,
        #         stdout=subprocess.PIPE,
        #         stderr=subprocess.PIPE,
        #     )

        #     tap_process.stdout.close()
        #     stdout, stderr = target_process.communicate()

        #     logging.info(f"STDOUT: {stdout}")
        #     logging.info(f"STDERR: {stderr}")

        # state = json.loads(stdout.decode("utf-8"))
        # self.save_state(state)


if __name__ == "__main__":
    tap = Tap(
        "tap-smoke-test",
        "git+https://github.com/meltano/tap-smoke-test.git",
        config={
            "streams": [
                {
                    "stream_name": "users",
                    "input_filename": "https://gitlab.com/meltano/tap-smoke-test/-/raw/main/demo-data/animals-data.jsonl",
                },
                {
                    "stream_name": "computers",
                    "input_filename": "https://gitlab.com/meltano/tap-smoke-test/-/raw/main/demo-data/animals-data.jsonl",
                },
            ]
        },
    )
    target = Target(
        "target-jsonl",
        config={
            "destination_path": "/tmp",
        },
    )
    runner = Runner(
        tap,
        target,
        # StateManager("azure://elx"),
    )
    runner.run()
