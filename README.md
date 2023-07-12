# ELX

🚧 Under construction

A lightweight Python interface for extracting and loading using the Singer.io spec.

⚡ Lazy install of Singer.io taps and targets \
⚡ Stream parallelism for high performance \
⚡ Remote state management \
⚡ Tap catalog is available in Python for metadata purposes

## Installation

```bash
pip install elx
```

## Usage

The most basic usage is as follows. Simply define the Tap and the Target and elx will take care of the rest.

```python
from elx import Runner, Tap, Target

runner = Runner(
  Tap("git+https://gitlab.com/meltano/tap-carbon-intensity.git"),
  Target("target-jsonl")
)

runner.run()
```

### Configuration

You can configure the tap and target by passing a `config` dictionary to the `Tap` and `Target` constructors. The config will be injected into the tap and target at runtime.

```python
from elx import Tap, Target

tap = Tap(
  "tap-foo",
  config={
    "api_key": "1234567890",
    "start_date": "2020-01-01"
  }
)

target = Target(
  "target-bar",
  config={
    "file_path": "/tmp"
  }
)
```

### State

By default, elx will store the state in the same directory as the script that is running. You can override this by passing a `StateManager` to the `Runner` constructor. Behind the scenes, elx uses [smart-open](https://github.com/RaRe-Technologies/smart_open) to be able to store the state in a variety of locations.

```python
from elx import Runner, StateManager

runner = Runner(
  tap,
  target,
  state_manager=StateManager("s3://my-bucket/my-folder")
)
```

Supported paths include:

| Path                                                   | Required Environment Variables                         | Elx Extra    |
| ------------------------------------------------------ | ------------------------------------------------------ | ------------ |
| `s3://my-bucket/my-folder`                             | `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`        | `elx[s3]`    |
| `gs://my-bucket/my-folder`                             | `GOOGLE_APPLICATION_CREDENTIALS` or `GOOGLE_API_TOKEN` | `elx[gs]`    |
| `azure://my-bucket/my-folder`                          | `AZURE_STORAGE_CONNECTION_STRING`                      | `elx[azure]` |
| `~/my-folder`                                          | `None`                                                 | `None`       |
| `/tmp/my-folder`                                       | `None`                                                 | `None`       |
| `(ssh\|scp\|sftp)://username@host//my-folder`          | `None`                                                 | `None`       |
| `(ssh\|scp\|sftp)://username:password@host//my-folder` | `None`                                                 | `None`       |
