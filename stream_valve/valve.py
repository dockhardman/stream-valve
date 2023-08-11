from abc import ABC
from pathlib import Path
from typing import Text, Union


class Valve(ABC):
    def __init__(self, *args, chunk_size: int = 1024, **kwargs):
        self.is_open: bool = False

        chunk_size = int(chunk_size)
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        else:
            self.chunk_size = chunk_size

    def open(self):
        self.is_open = True

    def close(self):
        self.is_open = False

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args):
        self.close()

    def __iter__(self):
        raise NotImplementedError


class FileValve(Valve):
    def __init__(
        self, filepath: Union[Text, Path], *args, chunk_size: int = 1024, **kwargs
    ):
        super().__init__(*args, chunk_size=chunk_size, **kwargs)

        self.filepath = filepath
        self.file_io = None

    def open(self):
        self.file_io = open(self.filepath, "rb")
        super().open()

    def close(self):
        self.file_io.close()
        super().close()

    def __iter__(self):
        if not self.is_open:
            raise ValueError("Valve is closed")

        while True:
            chunk = self.file_io.read(self.chunk_size)
            if not chunk:
                break
            yield chunk
