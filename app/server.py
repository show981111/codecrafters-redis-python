import asyncio
from datetime import datetime
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
        if role == "slave":
            if master_host is None or master_port is None:
                raise ValueError("If it is a slave, should specify the master")
            self.handshake_with_master(master_host, master_port)
        self.request_handler = RequestHandler(
            role=role, master_host=master_host, master_port=master_port
        )

    def handshake_with_master(
        self, master_host: str, master_port: int, timeout: int = 10
    ) -> None:
        self.clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.clientsocket.connect((master_host, master_port))
        # First, send ping
        while True:
            self.clientsocket.sendall(RespParser.encode(["PING"], type="bulk").encode())
            connected = False
            recvd = ""
            start = datetime.now()
            while True:
                data = self.clientsocket.recv(512)
                recvd += RespParser.decode(data)[0]
                if recvd == "PONG":
                    connected = True
                    break
                if (datetime.now() - start).total_seconds() > timeout:
                    break
            if connected:
                break
        print("[Handshake] Ping completed")

        # Second, REPLCONF messages
        self.clientsocket.sendall(
            RespParser.encode(
                ["REPLCONF", "listening-port", str(self.port)], type="bulk"
            ).encode()
        )
        print("send")
        self.clientsocket.sendall(
            RespParser.encode(["REPLCONF", "capa", "psync2"], type="bulk").encode()
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
