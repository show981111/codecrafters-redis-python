from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class Element:
    value: Any
    expire_at: datetime = datetime.max
    created_at: datetime = datetime.now()
    type: str = "string"


class Container:
    def __init__(self) -> None:
        self.kv: dict[Any, Element] = {}

    def get(self, key):
        if key not in self.kv.keys():
            return None
        elif datetime.now() > self.kv[key].expire_at:
            self.kv.pop(key)
            return None
        else:
            return self.kv[key].value

    def set(
        self, key, value, expire_at: datetime = datetime.max
    ):  # expiry input is in ms
        print(f"Set {key} = {value}, with expiry = {expire_at}")
        self.kv[key] = Element(
            value=value, created_at=datetime.now(), expire_at=expire_at
        )

    def keys(self) -> list:
        res = []
        for k in self.kv.keys():
            if datetime.now() > self.kv[k].expire_at:
                self.kv.pop(k)
            else:
                res.append(k)
        return res
