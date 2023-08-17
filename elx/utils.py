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
