class RespParser:
    @staticmethod
    def encode(data: str | int | bytes | list) -> str:
        if isinstance(data, str):
            return f"+{data}\r\n"
        elif isinstance(data, int):
            return f":{data}\r\n"
        elif isinstance(data, bytes):
            return f"${len(data)}\r\n{data.decode('utf-8')}\r\n"
        elif isinstance(data, list):
            encoded_elements = "".join([RespParser.encode(element) for element in data])
            return f"*{len(data)}\r\n{encoded_elements}"
        else:
            raise ValueError("Unsupported data type for encoding")

    @staticmethod
    def decode(data: bytes):
        if len(data) == 0:
            return b""
        end = data.find(b"\r\n")
        if end == -1:
            raise RespParserError("Invalid Input")

        if data.startswith(b"+"):
            return data[1:-2].decode("utf-8"), data[len(data) :]
        elif data.startswith(b"-"):
            return data[1:-2].decode("utf-8"), data[len(data) :]
        elif data.startswith(b":"):
            return int(data[1:-2]), data[len(data) :]
        elif data.startswith(b"$"):  # bulk string
            length = int(data[1:end])
            start = end + 2
            end = start + length
            string_end = data.find(b"\r\n")
            if string_end == -1:
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
