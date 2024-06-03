from pathlib import Path
from typing import Any, Tuple


class RdbParser:
    """
    Class to parse the redis db
    """

    def __init__(self, file_loc: Path):
        self.index = 0
        if not file_loc.is_file():
            self.data = b""
        else:
            file = open(file_loc, "rb")
            self.data = file.read()
            file.close()
        self.length = len(self.data)
        self.kv = {}

    def inc(self) -> None:
        self.index += 1

    def curr(self) -> bytes | None:
        return self.data[self.index : self.index + 1]

    def get(self, i) -> bytes:
        return self.data[i : i + 1]

    def substring(self, i, j) -> bytes:
        return self.data[i:j]

    def next(self, l: int) -> bytes:
        return self.substring(self.index, self.index + l)

    def read(self, l: int) -> bytes:
        value = self.substring(self.index, self.index + l)
        self.index += l
        return value

    def parse(self) -> dict:
        if not self.data:
            return self.kv
        self.parse_redis()
        version = self.parse_version()
        print(f"RDB Version: {version}")
        while self.has_next():
            # print(self.data[self.index:])
            print(self.data[self.index :])
            if self.curr()[0] == 0xFA:
                self.inc()
                length, key = self.parse_string()
                length, value = self.parse_string()
                print(f"{key}: {value}")
            elif self.curr()[0] == 0xFE:
                self.inc()
                database_number = self.parse_length()
                print(f"Database Number: {database_number}")
            elif self.curr()[0] == 0xFB:
                self.inc()
                db_ht_size = self.parse_length()
                print(f"Database hash table size: {db_ht_size}")
                exp_ht_size = self.parse_length()
                print(f"Expiry hash table size: {exp_ht_size}")
            elif self.curr()[0] == 0xFC:
                self.inc()
                expiry = int.from_bytes(self.read(8), byteorder="little")
                value_type = self.read(1)[0]
                length, key = self.parse_string()
                length, value = self.parse_value(value_type)
                print(f"Key:{key}, Value:{value}")
                self.kv[key] = {"value": value, "expiry": expiry}
            elif self.curr()[0] == 0xFF:
                self.inc()
                checksum = self.read(8)
                break
            else:
                value_type = self.read(1)[0]
                length, key = self.parse_string()
                length, value = self.parse_value(value_type)
                print(f"Key:{key}, Value:{value}")
                self.kv[key] = {"value": value, "expiry": None}
        return self.kv

    def parse_redis(self):
        if self.next(5) == b"REDIS":
            self.index += 5

    def parse_version(self):
        value = int(self.read(4))
        return value

    def parse_string(self) -> Tuple[int, Any]:
        length = self.parse_length()
        if length != -1:
            string = self.read(length)
            return 0, string.decode()
        else:
            self.index -= 1
            b = ord(self.curr()) & 0b00111111
            self.inc()
            if b == 0b00:
                val = int.from_bytes(self.read(1), byteorder="little")
            elif b == 0b01:
                val = int.from_bytes(self.read(2), byteorder="little")
            elif b == 0b10:
                val = int.from_bytes(self.read(4), byteorder="little")
            else:
                raise NotImplementedError
            return 0, val

    def parse_value(self, value_type):
        if value_type == 0x00:
            return self.parse_string()
        else:
            raise NotImplementedError

    def parse_length(self) -> int:
        b = ord(self.curr())
        self.inc()
        val = b >> 6
        length = 0b00
        if val == 0b00:
            length = (b & 0b00111111).to_bytes(1, byteorder="little")
        elif val == 0b01:
            length = (b & 0b00111111).to_bytes(1, byteorder="little") + self.curr()
            self.inc()
        elif val == 0b10:
            length = self.read(4)
        else:
            return -1
        return int.from_bytes(length, byteorder="little")

    def has_next(self) -> bool:
        return self.index < self.length
