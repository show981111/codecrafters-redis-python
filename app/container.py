from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class Element:
    value: Any
    created_at: datetime = datetime.now()
    expiry: float  # in second


class Container:
    def __init__(self) -> None:
        self.kv: dict[Any, Element] = {}

    def get(self, key):
        if key not in self.kv.keys():
            return None
        elif (datetime.now() - self.kv[key].created_at).total_seconds() > self.kv[
            key
        ].expiry:
            self.kv.pop(key)
            return None
        else:
            return self.kv[key].value

    def set(self, key, value, expiry=float("inf")):  # expiry input is in ms
        print(f"Set {key} = {value}, with expiry = {expiry* 0.001}")
        self.kv[key] = Element(
            value=value, created_at=datetime.now(), expiry=expiry * 0.001
        )

    def keys(self) -> list:
        res = []
        for k in self.kv.keys():
            if (datetime.now() - self.kv[k].created_at).total_seconds() > self.kv[
                k
            ].expiry:
                self.kv.pop(k)
            else:
                res.append(k)
        return res
