from elx import Runner
from elx.catalog import Stream

def dagster_safe_name(name: str) -> str:
    return name.replace("-", "_")

def schema(stream: Stream) -> str:
    # return stream.stream_schema.properties
    return '\n'.join(stream.stream_schema.properties.keys())

def generate_description(runner: Runner, stream: Stream) -> str:
    return f"""
        Load stream '{stream.name}' from '{runner.tap.executable}' into '{runner.target.executable}'.

        Refresh method: {stream.replication_method}
    """