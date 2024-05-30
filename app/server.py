import asyncio
from datetime import datetime
import socket
from typing import Any, Callable

from app.resp_parser import RespParser, RespParserError
from app.request_handler import RequestHandler


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    request_handler: RequestHandler,
):
    address = writer.get_extra_info("peername")
    print(f"Connected to {address}")
    data = b""
    while True:
        data += await reader.read(1024)  # Read up to 1024 bytes
        if not data:
            break
        try:
            parsed, _ = RespParser.decode(data)
            print(f"Received {parsed} from {address}")

            ret = request_handler.handle(parsed)
            data = b""
            if len(ret) == 0:
                continue
            writer.write(ret.encode())
            await writer.drain()
            print(f"Sent")
        except RespParserError as err:
            print(f"[WARNING] {err}")
            pass

    print(f"Closed connection to {address}")
    writer.close()
    await writer.wait_closed()


class Server:
    def __init__(
        self,
        port: int = 6379,
        role: str = "master",
        master_host: str | None = None,
        master_port: int | None = None,
    ) -> None:
        self.port = port
        self.master_replid = None
        self.master_repl_offset = None
        self.role = role
        self.master_host = master_host
        self.master_port = master_port

        if role == "slave":
            if master_host is None or master_port is None:
                raise ValueError("If it is a slave, should specify the master")
            self.handshake_with_master(master_host, master_port)
        else:
            import random
            import string

            characters = string.ascii_letters + string.digits
            self.master_replid = "".join(random.choice(characters) for _ in range(40))
            self.master_repl_offset = 0

        self.request_handler = RequestHandler(
            role=self.role, master_host=self.master_host, master_port=self.master_port
        )

    def handshake_with_master(
        self, master_host: str, master_port: int, timeout: int = 10
    ) -> None:
        self.clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.clientsocket.connect((master_host, master_port))

        def send_and_wait(data: bytes, stop: Callable[[Any], bool]) -> Any:
            while True:
                self.clientsocket.sendall(data)
                recvd = ""
                start = datetime.now()
                while True:
                    data = self.clientsocket.recv(512)
                    recvd += RespParser.decode(data)[0]
                    if stop(recvd):
                        return recvd
                    if (datetime.now() - start).total_seconds() > timeout:
                        break

        # First, send ping
        send_and_wait(
            RespParser.encode(["PING"], type="bulk").encode(), lambda x: x == "PONG"
        )
        print("[Handshake] Ping completed")

        # Second, REPLCONF messages
        send_and_wait(
            RespParser.encode(
                ["REPLCONF", "listening-port", str(self.port)], type="bulk"
            ).encode(),
            lambda x: x == "OK",
        )
        print("[Handshake] Replconf completed [1]")
        send_and_wait(
            RespParser.encode(["REPLCONF", "capa", "psync2"], type="bulk").encode(),
            lambda x: x == "OK",
        )
        print("[Handshake] Replconf completed [2]")
        resp = send_and_wait(
            RespParser.encode(["PSYNC", "?", "-1"], type="bulk").encode(),
            lambda x: isinstance(x, str) and x.startswith("FULLRESYNC"),
        )

    async def start(self) -> None:
        server = await asyncio.start_server(
            lambda r, w: handle_client(r, w, self.request_handler),
            "localhost",
            self.port,
            reuse_port=True,
        )

        address = server.sockets[0].getsockname()
        print(f"Serving on {address}")

        async with server:
            await server.serve_forever()
