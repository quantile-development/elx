import typer
from elx.cli import debug
from elx.cli import invoke

app = typer.Typer()

app.command()(debug.debug)
app.command()(invoke.invoke)


def cli():
    return app()
