from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


@dataclass
class Element:
    value: Any
    expire_at: datetime = datetime.max
    created_at: datetime = datetime.now()
    type: Literal["string", "stream"] = "string"


@dataclass
class StreamEntry:
    id: str
    data: dict


@dataclass
class StreamEntries:
    entries: list[StreamEntry]


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
        if isinstance(value, StreamEntry):
            if key in self.kv.keys():
                id_of_last_entry = (
                    self.kv[key].value.entries[len(self.kv[key].value.entries) - 1].id
                )
                if not Container._less_than(id_of_last_entry, value.id):
                    raise ValueError(
                        f"ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    )
                self.kv[key].value.entries.append(value)
            else:
                if not Container._less_than("0-0", value.id):
                    raise ValueError(
                        f"ERR The ID specified in XADD must be greater than 0-0"
                    )
                self.kv[key] = Element(
                    value=StreamEntries(entries=[value]),
                    created_at=datetime.now(),
                    expire_at=expire_at,
                    type="stream",
                )
        else:
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

    @staticmethod
    def _less_than(a: str, b: str) -> bool:  # True if a < b
        ac = a.split("-")
        bc = b.split("-")
        for i in range(2):
            ac[i] = int(ac[i])
            bc[i] = int(bc[i])
        if ac[0] < bc[0]:
            return True
        elif ac[0] == bc[0] and ac[1] < bc[1]:
            return True
        return False
