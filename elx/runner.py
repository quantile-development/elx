import json
import logging
import subprocess
from typing import Optional
from elx.tap import Tap
from elx.target import Target
from elx import StateManager
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

    def load_state(self) -> dict:
        return self.state_manager.load(self.state_file_name)

    def save_state(self, state: dict) -> None:
        self.state_manager.save(self.state_file_name, state)

    def run(self, stream: Optional[str] = None) -> None:
        state = self.load_state()

        with self.tap.process(state=state, stream=stream) as tap_process:
            with self.target.process(tap_process=tap_process) as target_process:

                def log_lines():
                    yield from iter(tap_process.stderr.readline, b"")
                    yield from iter(target_process.stderr.readline, b"")

                for line in log_lines():
                    print(line.decode("utf-8"))

                tap_process.stdout.close()
                stdout, stderr = target_process.communicate()

                # If any of the processes exited with a non-zero exit code,
                # raise an exception.
                if tap_process.returncode and tap_process.returncode != 0:
                    raise subprocess.CalledProcessError(
                        tap_process.returncode, tap_process.args
                    )
                if target_process.returncode and target_process.returncode != 0:
                    raise subprocess.CalledProcessError(
                        target_process.returncode, target_process.args
                    )

                state = json.loads(stdout.decode("utf-8"))
                self.save_state(state)


if __name__ == "__main__":
    tap = Tap(
        executable="tap-smoke-test",
        spec="git+https://github.com/meltano/tap-smoke-test.git",
        config={
            "streams": [
                {
                    "stream_name": "stream-one",
                    "input_filename": "https://gitlab.com/meltano/tap-smoke-test/-/raw/main/demo-data/animals-data.jsonl",
                },
                {
                    "stream_name": "stream-two",
                    "input_filename": "https://gitlab.com/meltano/tap-smoke-test/-/raw/main/demo-data/animals-data.jsonl",
                },
            ],
        },
    )
    target = Target(
        spec="git+https://github.com/estrategiahq/target-parquet.git",
        executable="target-parquet",
        config={
            "destination_path": "/tmp",
        },
    )
    runner = Runner(tap, target, StateManager("/tmp"))
    runner.run("stream-one")
