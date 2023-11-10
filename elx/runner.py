import asyncio
import datetime
import json
import logging
import select
import subprocess
import sys
import threading
from typing import List, Optional

from functools import cached_property
from elx.tap import Tap
from elx.target import Target
from elx import StateManager
from dotenv import load_dotenv

from elx.utils import capture_subprocess_output

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

        # Give the tap and target access to the runner
        self.tap.runner = self
        self.target.runner = self

    @property
    def state_file_name(self) -> str:
        return f"{self.tap.executable}-{self.target.executable}.json"

    def load_state(self) -> dict:
        return self.state_manager.load(self.state_file_name)

    def save_state(self, state: dict) -> None:
        self.state_manager.save(self.state_file_name, state)

    @cached_property
    def interpolation_values(self) -> dict:
        """
        Values that can be used in the config of the tap or target.
        """
        NOW = datetime.datetime.now()

        return {
            "NOW": NOW.isoformat(),
            "YESTERDAY": (NOW - datetime.timedelta(days=1)).isoformat(),
            "LAST_WEEK": (NOW - datetime.timedelta(days=7)).isoformat(),
            "TAP_EXECUTABLE": self.tap.executable,
            "TAP_NAME": self.tap.executable.replace("-", "_"),
            "TARGET_EXECUTABLE": self.target.executable,
            "TARGET_NAME": self.target.executable.replace("-", "_"),
        }

    def run(
        self,
        streams: Optional[List[str]] = None,
        logger: logging.Logger = None,
    ) -> None:
        asyncio.run(self.async_run(streams=streams, logger=logger))

    async def async_run(
        self,
        streams: Optional[List[str]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        state = self.load_state()

        class StateWriter:
            @staticmethod
            def writelines(state_line: str):
                state = json.loads(state_line)
                self.save_state(state)

        class LogWriter:
            def __init__(self, logger: Optional[logging.Logger]):
                self.logger = logger

            def writelines(self, line: str):
                if self.logger:
                    self.logger.info(line)

        async with self.tap.process(state=state, streams=streams) as tap_process:
            async with self.target.process(tap_process=tap_process) as target_process:
                tap_outputs = [target_process.stdin]
                tap_stdout_future = asyncio.ensure_future(
                    # forward subproc stdout to tap_outputs (i.e. targets stdin)
                    capture_subprocess_output(tap_process.stdout, *tap_outputs),
                )
                tap_stderr_future = asyncio.ensure_future(
                    capture_subprocess_output(
                        tap_process.stderr, *[sys.stderr, LogWriter(logger)]
                    ),
                )

                target_outputs = [StateWriter()]
                target_stdout_future = asyncio.ensure_future(
                    capture_subprocess_output(target_process.stdout, *target_outputs),
                )
                target_stderr_future = asyncio.ensure_future(
                    capture_subprocess_output(
                        target_process.stderr, *[sys.stderr, LogWriter(logger)]
                    ),
                )

                tap_process_future = asyncio.ensure_future(tap_process.wait())
                target_process_future = asyncio.ensure_future(target_process.wait())
                output_exception_future = asyncio.ensure_future(
                    asyncio.wait(
                        [
                            tap_stdout_future,
                            tap_stderr_future,
                            target_stdout_future,
                            target_stderr_future,
                        ],
                        return_when=asyncio.FIRST_EXCEPTION,
                    ),
                )

                done, _ = await asyncio.wait(
                    [
                        tap_process_future,
                        target_process_future,
                        output_exception_future,
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if output_exception_future in done:
                    output_futures_done, _ = output_exception_future.result()
                    if output_futures_failed := [
                        future
                        for future in output_futures_done
                        if future.exception() is not None
                    ]:
                        # If any output handler raised an exception, re-raise it.

                        # # Special behavior for the tap stdout handler raising a line
                        # # length limit error.
                        # if tap_stdout_future in output_futures_failed:
                        #     self._handle_tap_line_length_limit_error(
                        #         tap_stdout_future.exception(),
                        #         line_length_limit=line_length_limit,
                        #         stream_buffer_size=stream_buffer_size,
                        #     )

                        failed_future = output_futures_failed.pop()
                        raise failed_future.exception()  # noqa: RSE102
                    else:
                        # If all of the output handlers completed without raising an
                        # exception, we still need to wait for the tap or target to
                        # complete.
                        done, _ = await asyncio.wait(
                            [tap_process_future, target_process_future],
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                if target_process_future in done:
                    target_code = target_process_future.result()

                    if tap_process_future in done:
                        tap_code = tap_process_future.result()
                    else:
                        # If the target completes before the tap, it failed before
                        # processing all tap output

                        # Kill tap and cancel output processing since there's no more
                        # target to forward messages to
                        tap_process.kill()
                        await tap_process_future
                        tap_stdout_future.cancel()
                        tap_stderr_future.cancel()

                        # Pretend the tap finished successfully since it didn't itself fail
                        tap_code = 0

                    # Wait for all buffered target output to be processed
                    await asyncio.wait([target_stdout_future, target_stderr_future])
                else:  # if tap_process_future in done:
                    # If the tap completes before the target, the target should have a
                    # chance to process all tap output
                    tap_code = tap_process_future.result()

                    # Wait for all buffered tap output to be processed
                    await asyncio.wait([tap_stdout_future, tap_stderr_future])

                    # Close target stdin so process can complete naturally
                    target_process.stdin.close()
                    await target_process.stdin.wait_closed()

                    # Wait for all buffered target output to be processed
                    await asyncio.wait([target_stdout_future, target_stderr_future])

                    # Wait for target to complete
                    target_code = await target_process_future

                if tap_code and target_code:
                    raise Exception("Tap and target failed")
                elif tap_code:
                    raise Exception("Tap failed")
                elif target_code:
                    raise Exception("Target failed")

                # Save the state.
                # print("state", await tap_process.stdout.readline())
                # self.save_state(state)


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
