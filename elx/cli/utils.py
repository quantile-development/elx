import importlib
from importlib.machinery import SourceFileLoader
import os
import pkgutil
from typing import Any, Generator, List

import inquirer


def find_sub_modules(module: Any) -> Generator:
    """
    Find all submodules of a module.

    Args:
        module (Any): The module to find submodules of.

    Returns:
        Generator: A generator of submodules.
    """
    for loader, module_name, is_pkg in pkgutil.walk_packages(module.__path__):
        yield loader.find_module(module_name).load_module(module_name)


def find_instances_of_type(locator: str, type: Any) -> Generator[Any, None, None]:
    """
    Find all instances of a type in a module or path.

    Args:
        locator (str): The locator to the module or path to file.
        type (Any): The type to find.

    Returns:
        Generator: A generator of instances of the type.
    """
    # If the locator is a file, load the module
    if locator.endswith(".py"):
        module = SourceFileLoader("module", locator).load_module()
        yield from [
            instance for instance in vars(module).values() if isinstance(instance, type)
        ]
    else:
        # Otherwise import the module
        start_module = importlib.import_module(locator)

        # Find all submodules of the module
        for module in find_sub_modules(start_module):
            yield from [
                instance
                for instance in vars(module).values()
                if isinstance(instance, type)
            ]


def request_instance(
    instances: List[Any],
    default_name: str = None,
    message: str = "",
) -> Any:
    """
    Request an instance of a type from the user.

    Args:
        instances (List[Any]): The instances to request from.
        default_name (str): The default name for the instance.
        message (str): The message to show the user.

    Returns:
        Any: The instance of the type.
    """
    # If there is only one instance, select it
    if len(instances) == 1:
        return instances[0]

    # Get all instances from foo that are of type Tap
    instances = {instance.name: instance for instance in instances}

    # If there is a default name, use it
    if default_name in instances:
        return instances[default_name]

    questions = [
        inquirer.List(
            "instance",
            message=message,
            choices=instances.keys(),
            default=default_name,
            carousel=True,
        ),
    ]

    instance_name = inquirer.prompt(questions)["instance"]

    return instances[instance_name]


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
