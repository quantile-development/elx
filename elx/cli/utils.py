import imp
import os
from pathlib import Path


def load_variables_from_path(path: Path) -> dict:
    """
    Load all variables from a path.

    Args:
        path (Path): The path to load variables from.

    Returns:
        dict: A dictionary of variables.
    """
    file = imp.load_source(name="file", pathname=str(path))

    # Get all variables from foo
    variables = vars(file)

    return variables


def obfuscate_secrets(config: dict, secrets: dict = os.environ) -> str:
    """
    Obfuscate secrets in a config, only show the first character.

    Args:
        config (dict): The config to obfuscate.
        secrets (dict): The secrets to obfuscate.

    Returns:
        str: The obfuscated config.
    """
    for key, value in config.items():
        for secret_key, secret_value in secrets.items():
            if secret_value == value:
                config[key] = secret_value[:3] + "*" * (len(secret_value) - 3)

    return config
