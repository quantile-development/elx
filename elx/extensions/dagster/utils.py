def dagster_safe_name(name: str) -> str:
    return name.replace("-", "_")