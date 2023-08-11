import os
from typing import Text

from stream_valve.valve import FileValve


def test_file_valve(dummy_file: Text):
    filesize = os.path.getsize(dummy_file)

    size_accumulator = 0
    valve = FileValve(filepath=dummy_file)
    with valve:
        for chunk in valve:
            size_accumulator += len(chunk)

    assert size_accumulator == filesize
