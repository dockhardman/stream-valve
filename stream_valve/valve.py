from abc import ABC
from pathlib import Path
from typing import Optional, Text, Union

from stream_valve.config import logger


class Valve(ABC):
    def __init__(
        self,
        *args,
        chunk_size: int = 1024,
        throughput: Optional[float] = None,
        rate_limit_backoff_delay: float = 0.01,
        **kwargs
    ):
        chunk_size = int(chunk_size)
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        else:
            self.chunk_size = chunk_size

        if throughput is not None and throughput <= 0:
            raise ValueError("throughput must be positive")
        else:
            self.throughput = throughput

        if rate_limit_backoff_delay is not None and rate_limit_backoff_delay <= 0:
            raise ValueError("rate_limit_backoff_delay must be positive")
        else:
            self.rate_limit_backoff_delay = rate_limit_backoff_delay

        self.is_open: bool = False
        self.throughput_accumulator: float = 0.0
        self.throughput_time_accumulator: float = 0.0

    def open(self):
        self.is_open = True
        self.meter_zero()

    def close(self):
        self.is_open = False

    def meter_zero(self):
        self.throughput_accumulator = 0.0
        self.throughput_time_accumulator = 0.0

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args):
        self.close()

    def __iter__(self):
        raise NotImplementedError


class FileValve(Valve):
    def __init__(
        self,
        filepath: Union[Text, Path],
        *args,
        chunk_size: int = 1024,
        throughput: Optional[float] = None,
        rate_limit_backoff_delay: float = 0.01,
        **kwargs
    ):
        super().__init__(
            *args,
            chunk_size=chunk_size,
            throughput=throughput,
            rate_limit_backoff_delay=rate_limit_backoff_delay,
            **kwargs
        )

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
