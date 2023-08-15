import logging
import time
from abc import ABC
from pathlib import Path
from typing import Generator, Optional, Text, Union

from rich import print

from stream_valve.config import logger


class Valve(ABC):
    def __init__(
        self,
        *args,
        chunk_size: int = 1024,
        throughput: Optional[float] = None,
        rate_limit_backoff_delay: float = 0.01,
        debug: bool = False,
        logger: Optional["logging.Logger"] = None,
        **kwargs,
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

        self.debug = debug
        self.logger = logger

        self.is_open: bool = False
        self.throughput_iter_count_accumulator: int = 0
        self.throughput_len_accumulator: int = 0
        self.throughput_time_accumulator: float = 0.0
        self._mono_timer: float = time.monotonic()

    def open(self):
        self.is_open = True
        self.meter_zero()

    def close(self):
        self.is_open = False

    def meter_zero(self):
        self.throughput_iter_count_accumulator = 0
        self.throughput_len_accumulator = 0
        self.throughput_time_accumulator = 0.0
        self._mono_timer: float = time.monotonic()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args):
        self.close()

    def __iter__(self) -> "Generator[bytes, None, None]":
        if not self.is_open:
            raise ValueError("Valve is closed")

        for chunk in self.throttle():
            self.throughput_iter_count_accumulator += 1
            self.throughput_len_accumulator += len(chunk)
            self.throughput_time_accumulator += time.monotonic() - self._mono_timer
            rate = self.throughput_len_accumulator / self.throughput_time_accumulator

            if self.debug is True:
                self.log(f"Rate: {rate:.2f} bytes/sec", level=logging.INFO)

            yield chunk

    def throttle(self) -> "Generator[bytes, None, None]":
        raise NotImplementedError

    def log(self, msg: Text, level: int = logging.DEBUG):
        if self.logger is not None:
            self.logger.log(level, msg)
        else:
            print(msg)


class FileValve(Valve):
    def __init__(
        self,
        filepath: Union[Text, Path],
        *args,
        chunk_size: int = 1024,
        throughput: Optional[float] = None,
        rate_limit_backoff_delay: float = 0.01,
        debug: bool = False,
        logger: Optional["logging.Logger"] = None,
        **kwargs,
    ):
        super().__init__(
            *args,
            chunk_size=chunk_size,
            throughput=throughput,
            rate_limit_backoff_delay=rate_limit_backoff_delay,
            debug=debug,
            logger=logger,
            **kwargs,
        )

        self.filepath = filepath
        self.file_io = None

    def open(self):
        self.file_io = open(self.filepath, "rb")
        super().open()

    def close(self):
        self.file_io.close()
        super().close()

    def throttle(self) -> "Generator[bytes, None, None]":
        while True:
            chunk = self.file_io.read(self.chunk_size)
            if not chunk:
                break
            yield chunk
