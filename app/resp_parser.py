from typing import Final, Literal


class RespParser:

    empty_rdb_base64: Final[str] = (
        "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
    )

    @staticmethod
    def encode(
        data: str | int | bytes | list | None, type: Literal["", "bulk", "rdb"] = ""
    ) -> str:
        if data is None:
            return f"$-1\r\n"
        elif isinstance(data, int):
            return f":{data}\r\n"
        elif isinstance(data, list):
            encoded_elements = "".join(
                [RespParser.encode(element, type) for element in data]
            )
            return f"*{len(data)}\r\n{encoded_elements}"
        elif isinstance(data, bytes):
            return f"${len(data)}\r\n{data.decode('utf-8')}\r\n"
        elif isinstance(data, str) and type == "bulk":
            return f"${len(data)}\r\n{data}\r\n"
        elif isinstance(data, str) and type == "rdb":
            return f"${len(data)}\r\n{data}"
        elif isinstance(data, str):
            return f"+{data}\r\n"
        else:
            raise ValueError("Unsupported data type for encoding")

    @staticmethod
    def decode(data: bytes, type: Literal["", "bulk", "rdb"] = ""):
        end = data.find(b"\r\n")
        if end == -1:
            raise RespParserError("Invalid Input")

        if data.startswith(b"+"):
            # print("recvd:", data)
            return data[1:-2].decode("utf-8"), data[len(data) :]
        elif data.startswith(b"-"):
            return data[1:-2].decode("utf-8"), data[len(data) :]
        elif data.startswith(b":"):
            return int(data[1:-2]), data[len(data) :]
        elif data.startswith(b"$") and type == "rdb":
            length = int(data[1:end])
            if length != len(data) - (end + 2):
                raise RespParserError("Length is not matching")
            return data[end + 2 :], data[len(data) :]
        elif data.startswith(b"$"):  # bulk string
            length = int(data[1:end])
            start = end + 2
            end = start + length
            string_end = data.find(b"\r\n", start)
            if string_end == -1 or length != string_end - start:
                raise RespParserError("Invalid Input")
            return data[start:end].decode("utf-8"), data[end + 2 :]
        elif data.startswith(b"*"):
            num_elements = int(data[1:end])
            elements = []
            data = data[end + 2 :]
            for _ in range(num_elements):
                element, data = RespParser.decode(data)
                elements.append(element)
            return elements, data
        else:
            raise ValueError("Unsupported RESP data type")


class RespParserError(Exception):

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        """Return message."""
        return self.message
