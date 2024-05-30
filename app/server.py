import asyncio
import socket

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
            writer.write(ret.encode())
            await writer.drain()
            data = b""
        except RespParserError as err:
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
        if role == "slave":
            if master_host is None or master_port is None:
                raise ValueError("If it is a slave, should specify the master")
            self.clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.clientsocket.connect((master_host, master_port))
            self.clientsocket.sendall(RespParser.encode(["PING"], type="bulk").encode())
        self.request_handler = RequestHandler(
            role=role, master_host=master_host, master_port=master_port
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
