from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class Element:
    value: Any
    created_at: datetime
    expiry: float  # in second


class Container:
    def __init__(self) -> None:
        self.kv: dict[Any, Element] = {}

    def get(self, key):
        if (key not in self.kv.keys()) or datetime.now() - self.kv[
            key
        ].created_at > self.kv[key].expiry:
            return "-1"
        return self.kv[key].value

    def set(self, key, value, expiry=-1):  # expiry input is in ms
        self.kv[key] = Element(
            value=value, created_at=datetime.now(), expiry=expiry * 100
        )
