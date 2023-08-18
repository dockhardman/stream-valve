from abc import ABC
from typing import Text


class Limiter(ABC):
    def __init__(self, *args, **kwargs):
        self._is_rate_limited: bool = False

    def name(self) -> Text:
        raise NotImplementedError

    def is_exceeded(self) -> bool:
        raise NotImplementedError

    def is_rate_limited(self) -> bool:
        return self._is_rate_limited

    @property
    def is_limited(self) -> bool:
        return self.is_rate_limited()


class RateLimiter(Limiter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def name(self) -> Text:
        return "RateLimiter"

    def is_exceeded(self) -> bool:
        raise NotImplementedError
