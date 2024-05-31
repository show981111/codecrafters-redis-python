from typing import Final, Literal


class RespParser:

    empty_rdb_hex: Final[str] = (
        "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
    )

    @staticmethod
    def encode(
        data: str | int | bytes | list | None, type: Literal["", "bulk", "rdb"] = ""
    ) -> bytes:
        if data is None:
            return f"$-1\r\n".encode()
        elif isinstance(data, int):
            return f":{data}\r\n".encode()
        elif isinstance(data, list):
            encoded_elements = b"".join(
                [RespParser.encode(element, type) for element in data]
            )
            return f"*{len(data)}\r\n".encode() + encoded_elements
        elif isinstance(data, bytes):
            return f"${len(data)}\r\n{data.decode('utf-8')}\r\n".encode()
        elif isinstance(data, str) and type == "bulk":
            return f"${len(data)}\r\n{data}\r\n".encode()
        elif isinstance(data, str) and type == "rdb":
            content = bytes.fromhex(data)
            return f"${len(content)}\r\n".encode() + content
        elif isinstance(data, str):
            return f"+{data}\r\n".encode()
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
