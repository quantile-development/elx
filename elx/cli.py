import asyncio
import os
import imp
import inquirer
import typer
from pathlib import Path
from typing_extensions import Annotated
from elx import Tap, Runner, Target, StateManager
from rich.prompt import Prompt

app = typer.Typer()


@app.command()
def invoke(
    path: Path,
    limit: int = typer.Option(3, help="Limit the number of records to ingest"),
):
    """
    Invoke a target or tap.

    Args:
        path (str): The path to the target or tap.
    """
    # Pathlib get current path and join with path
    absolute_path = Path.cwd() / path

    foo = imp.load_source(name="foo", pathname=str(absolute_path))

    # Get all variables from foo
    variables = vars(foo)

    # Get all instances from foo that are of type Tap
    taps = {tap.executable: tap for tap in variables.values() if isinstance(tap, Tap)}

    questions = [
        inquirer.List(
            "tap",
            message="Which tap do you want to invoke?",
            choices=taps.keys(),
            carousel=True,
        ),
    ]

    tap_name = inquirer.prompt(questions)["tap"]

    tap = taps[tap_name]

    streams = [
        inquirer.List(
            "stream",
            message="Which stream do you want to invoke?",
            choices=["all", *[stream.name for stream in tap.catalog.streams]],
            default="all",
            carousel=True,
        ),
    ]

    stream_name = inquirer.prompt(streams)["stream"]

    tap.invoke([stream_name], limit=limit)

    # print(taps[tap_name].invoke())

    # filename = "example.py"

    # with open(absolute_path, "r") as file:
    # content = file.read()

    # # Create a dictionary to store the variables
    # variables = {}

    # # Execute the code in a safe environment to populate the dictionary
    # exec(content, globals(), variables)

    # # Now, you can access the variables
    # print(variables)
    # # print(variables['var2'])


@app.command()
def run(path: Path):
    """
    Invoke a target or tap.

    Args:
        path (str): The path to the target or tap.
    """
    # Pathlib get current path and join with path
    absolute_path = Path.cwd() / path

    foo = imp.load_source(name="foo", pathname=str(absolute_path))

    # print(foo)

    # Get all variables from foo
    variables = vars(foo)

    # print(variables)

    # Get all instances from foo that are of type Tap
    runners = {
        f"{runner.tap.executable} > {runner.target.executable}": runner
        for runner in variables.values()
        if isinstance(runner, Runner)
    }

    # print(taps)

    # name = Prompt.ask("Enter your name", choices=[tap.executable for tap in taps])

    questions = [
        inquirer.List(
            "runner",
            message="Which runner do you want to run?",
            choices=runners.keys(),
        ),
    ]

    runner_name = inquirer.prompt(questions)["runner"]

    runner = runners[runner_name]

    async def run():
        await runner.run()

    # tap.invoke()
    asyncio.run(run())


@app.command()
def el(path: Path):
    """
    Invoke a target or tap.

    Args:
        path (str): The path to the target or tap.
    """
    # Pathlib get current path and join with path
    absolute_path = Path.cwd() / path

    foo = imp.load_source(name="foo", pathname=str(absolute_path))

    # Get all variables from foo
    variables = vars(foo)

    # Get all instances from foo that are of type Tap
    taps = {tap.executable: tap for tap in variables.values() if isinstance(tap, Tap)}
    targets = {
        tap.executable: tap for tap in variables.values() if isinstance(tap, Target)
    }

    tap_name = inquirer.prompt(
        [
            inquirer.List(
                "tap",
                message="Which tap do you want to run?",
                choices=taps.keys(),
                carousel=True,
            ),
        ]
    )["tap"]

    target_name = inquirer.prompt(
        [
            inquirer.List(
                "target",
                message="Which target do you want to run?",
                choices=targets.keys(),
                carousel=True,
            ),
        ]
    )["target"]

    tap = taps[tap_name]
    target = targets[target_name]

    runner = Runner(tap=tap, target=target, state_manager=StateManager("."))

    runner.run()


def main():
    return app()
