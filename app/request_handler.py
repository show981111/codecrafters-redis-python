from app.resp_parser import RespParser


class RequestHandler:
    def handle(self, input: list | int | str) -> str:
        if isinstance(input, list) and isinstance(input[0], str):
            if input[0].upper() == "ECHO":
                return RespParser.encode(input[1])
            if input[0].upper() == "PING":
                return RespParser.encode("PONG")
        raise ValueError("Unknown")
