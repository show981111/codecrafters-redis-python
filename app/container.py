import bisect
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Tuple


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

    @staticmethod
    def validate_input_id_format(id: str) -> bool:
        comp = id.split("-")
        if (
            len(comp) != 2
            or len(comp[0]) > 13
            or not comp[0].isdigit()
            or not comp[1].isdigit()
            or comp[0] == "$"
        ):
            return False
        return True


@dataclass
class StreamEntries:
    entries: list[StreamEntry]

    @staticmethod
    def key_func(item: StreamEntry) -> float:
        if item.id == "-":
            return 0
        elif item.id == "+":
            return float("inf")
        comp = item.id.split("-")
        x = 0.0
        x += float(comp[0])
        if len(comp) == 2:
            x += 0.1 * float(comp[1])
        return x


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
            if not Container._auto_populate(value.id) and not Container._less_than(
                "0-0", value.id
            ):
                raise ValueError(
                    f"ERR The ID specified in XADD must be greater than 0-0"
                )
            if key in self.kv.keys():
                id_of_last_entry = (
                    self.kv[key].value.entries[len(self.kv[key].value.entries) - 1].id
                )
                value.id = Container._get_next_if_auto(value.id, id_of_last_entry)
                print(f"New value.id", value.id)
                if not Container._less_than(id_of_last_entry, value.id):
                    raise ValueError(
                        f"ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    )
                self.kv[key].value.entries.append(value)
            else:
                value.id = Container._get_next_if_auto(value.id)
                print(f"New value.id", value.id)
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

    def get_after_excl(
        self, stream_keys: list, starts: list[StreamEntry]
    ) -> Tuple[list, int]:
        entry_length = 0
        res = []
        for idx, stream_key in enumerate(stream_keys):
            if stream_key in self.keys():
                entries = self.get(stream_key).entries
                if starts[idx] == "$":
                    # then set it to max so far
                    if len(entries) == 0:
                        starts[idx] = "0-0"
                    else:
                        starts[idx] = entries[len(entries) - 1]
                start_excl = bisect.bisect_right(
                    entries,
                    StreamEntries.key_func(starts[idx]),
                    key=StreamEntries.key_func,
                )
                entry_length += len(entries) - start_excl
                res.append([stream_key, entries[start_excl:]])

        return res, entry_length

    @staticmethod
    def _auto_populate(id: str) -> bool:
        components = id.split("-")
        if components[0] == "*" or components[1] == "*":
            return True
        return False

    @staticmethod
    def _get_next_if_auto(id: str, id_of_last_entry: str | None = None) -> str:
        components = id.split("-")
        if components[0] == "*":
            import time

            components[0] = str(int(time.time() * 1000.0))
            components.append("*")

        if components[1] == "*":
            if (
                id_of_last_entry is not None
                and id_of_last_entry.split("-")[0] == components[0]
            ):
                last_seq = int(id_of_last_entry.split("-")[1])
            elif components[0] == "0":
                last_seq = 0
            else:
                last_seq = -1
            components[1] = str(last_seq + 1)
            return "-".join(components)
        else:
            return id

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
