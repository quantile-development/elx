from pathlib import Path
import typer
from rich import print
from elx.cli import debug
from elx.cli import invoke
from dotenv import load_dotenv, dotenv_values

app = typer.Typer()

app.command()(debug.debug)
app.command()(invoke.invoke)


def cli():
    env_path = Path.cwd() / ".env"
    loaded_env = load_dotenv(env_path)

    # Use rich to print the loaded env
    if loaded_env:
        env_variables = dotenv_values(env_path)
        print(
            f"[bold]Loaded environment variables:[/bold] {', '.join(env_variables.keys())}"
        )

    return app()
