# Dagster ELX with DBT

An example end-to-end project that uses Dagster with ELX for extract and load, and DBT for transformations.

### Setup

First install the dependencies:

```shell
poetry install
```

Then, start Dagit:

```shell
poetry run dagster dev -m dagster_elx_with_dbt
```
