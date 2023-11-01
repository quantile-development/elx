import asyncio


def require_install(func):
    """
    Decorator to check if the executable is installed.
    """

    def wrapper(self, *args, **kwargs):
        if not self.is_installed:
            self.install()

        return func(self, *args, **kwargs)

    return wrapper


def interpolate_in_config(config: dict, interpolation: dict) -> dict:
    """
    Interpolate a value in the config. Recurse through the config and use format strings to interpolate the value.

    Args:
        config (dict): The config.
        interpolation (dict): The config to interpolate with, E.g. {"key": "value"}.

    Returns:
        dict: The interpolated config.
    """

    def _interpolate(value: str) -> str:
        if isinstance(value, str):
            return value.format(**interpolation)
        elif isinstance(value, list):
            return [_interpolate(item) for item in value]
        elif isinstance(value, dict):
            return {key: _interpolate(value) for key, value in value.items()}
        else:
            return value

    return {key: _interpolate(value) for key, value in config.items()}


async def _write_line_writer(writer, line):
    # StreamWriters like a subprocess's stdin need special consideration
    if isinstance(writer, asyncio.StreamWriter):
        try:
            writer.write(line)
            await writer.drain()
        except (BrokenPipeError, ConnectionResetError):
            await writer.wait_closed()
            return False
    else:
        writer.writelines(line.decode())

    return True


async def capture_subprocess_output(
    reader: asyncio.StreamReader | None,
    *line_writers,
) -> None:
    """Capture in real time the output stream of a suprocess that is run async.

    The stream has been set to asyncio.subprocess.PIPE and is provided using
    reader to this function.

    As new lines are captured for reader, they are written to output_stream.
    This async function should be run with await asyncio.wait() while waiting
    for the subprocess to end.

    Args:
        reader: `asyncio.StreamReader` object that is the output stream of the
            subprocess.
        line_writers: A `StreamWriter`, or object has a compatible writelines method.
    """
    while not reader.at_eof():
        line = await reader.readline()
        if not line:
            continue

        for writer in line_writers:
            if not await _write_line_writer(writer, line):
                # If the destination stream is closed, we can stop capturing output.
                return
