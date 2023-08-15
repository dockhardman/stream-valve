# stream-valve

A throughput valve util.


# Installation

```shell
pip install stream-valve
```
# Quick Started

```python
from stream_valve.valve import FileValve

filepath = <THE_FILEPATH>

file_size = os.path.getsize(dummy_file)
size_accumulator = 0

valve = FileValve(filepath=filepath)
with valve:
    for chunk in valve:
        size_accumulator += len(chunk)

assert size_accumulator == file_size
```
