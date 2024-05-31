from app.resp_parser import RespParser
from app.container import Container


class RequestHandler:
    def __init__(
        self,
        role: str = "master",
        master_host: str | None = None,
        master_port: int | None = None,
        master_replid: int | None = None,
        master_repl_offset: str | None = None,
    ) -> None:
        self.container = Container()
        self.role = role
        if role == "slave":
            if master_host is None or master_port is None:
                raise ValueError("If it is a slave, should specify the master")
            self.master_host = master_host
            self.master_port = master_port
        else:
            self.master_replid = master_replid
            self.master_repl_offset = master_repl_offset

    def handle(self, input: list | int | str) -> bytes | list[bytes]:
        if isinstance(input, list) and isinstance(input[0], str):
            match input[0].upper():
                case "ECHO":
                    return RespParser.encode(input[1])
                case "PING":
                    return RespParser.encode("PONG")
                case "SET":
                    if len(input) < 3:
                        raise ValueError("Invalid usage of SET")
                    if len(input) == 5 and input[3] == "px":
                        self.container.set(input[1], input[2], expiry=float(input[4]))
                    else:
                        self.container.set(input[1], input[2])
                    return RespParser.encode("OK")
                case "GET":
                    if len(input) != 2:
                        raise ValueError("Invalid usage of GET")
                    return RespParser.encode(self.container.get(input[1]))
                case "INFO":
                    if len(input) != 2:
                        raise ValueError("Invalid usage of GET")
                    if input[1] == "replication":
                        return RespParser.encode(
                            self.get_info(),
                            type="bulk",
                        )
                case "REPLCONF":
                    return RespParser.encode("OK")
                case "PSYNC":
                    return [
                        RespParser.encode(
                            f"FULLRESYNC {self.master_replid} {self.master_repl_offset}"
                        ),
                        RespParser.encode(RespParser.empty_rdb_hex, type="rdb"),
                        # "$\r\n",
                        # bytes.fromhex(RespParser.empty_rdb_hex)
                    ]
        print("Unknown command")
        return ""

    def get_info(self) -> str:
        if self.role == "slave":
            return f"role:{self.role}"
        else:
            return f"role:{self.role}\nmaster_replid:{self.master_replid}\nmaster_repl_offset:{self.master_repl_offset}"
