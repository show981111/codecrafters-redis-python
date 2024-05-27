from app.resp_parser import RespParser


class RequestHandler:
    def __init__(self) -> None:
        self.kv = {}

    def handle(self, input: list | int | str) -> str:
        if isinstance(input, list) and isinstance(input[0], str):
            match input[0].upper():
                case "ECHO":
                    return RespParser.encode(input[1])
                case "PING":
                    return RespParser.encode("PONG")
                case "SET":
                    if len(input) != 3:
                        raise ValueError("Invalid usage of SET")
                    self.kv[input[1]] = input[2]
                    return RespParser.encode("OK")
                case "GET":
                    if len(input) != 2:
                        raise ValueError("Invalid usage of GET")
                    if input[1] in self.kv.keys():
                        return RespParser.encode(self.kv[input[1]])
                    else:
                        return RespParser.encode("-1")
        raise ValueError("Unknown")
