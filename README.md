# ELX

A lightweight Python interface for extracting and loading using the Singer.io spec.

⚡ Lazy install of Singer.io taps and targets \
⚡ Stream parallelism for high performance

## Installation

```bash
pip install elx
```

## Usage
The most basic usage is as follows. Simply define the Tap and the Target and elx will take care of the rest.
```python
from elx import Runner, Tap, Target

runner = Runner(
  Tap("tap-carbon-intensity", "git+https://gitlab.com/meltano/tap-carbon-intensity.git"),
  Target("target-jsonl")
)

runner.run()
```
