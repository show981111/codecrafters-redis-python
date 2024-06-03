import asyncio
from datetime import datetime
from pathlib import Path
import socket
import sys
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

            ret = await request_handler.handle(parsed, address)
            if ret.code == 203:
                request_handler.replicas[writer] = reader
                request_handler.replica_addr_to_writer[address] = writer
            if ret.code == 400:
                continue
            if isinstance(ret.data, list):
                for item in ret.data:
                    writer.write(item)
            else:
                writer.write(ret.data)
            await writer.drain()
            print(f"Sent")
            data = b""
        except RespParserError as err:
            print(f"[WARNING] {err}: {data}")
            if len(data) == 0:
                break
    # comes here only when the connection is closed.
    print(f"Closed connection to {address}")
    writer.close()
    request_handler.discard_wr(writer, address)
    await writer.wait_closed()


class Server:
    def __init__(
        self,
        port: int = 6379,
        role: str = "master",
        master_host: str | None = None,
        master_port: int | None = None,
        dir: Path | None = None,
        rdbfilename: str | None = None,
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
            # self.connect_with_master(master_host, master_port)
        else:
            import random
            import string

            characters = string.ascii_letters + string.digits
            self.master_replid = "".join(random.choice(characters) for _ in range(40))
            self.master_repl_offset = 0

        self.request_handler = RequestHandler(
            role=self.role,
            master_host=self.master_host,
            master_port=self.master_port,
            master_replid=self.master_replid,
            master_repl_offset=self.master_repl_offset,
            dir=dir,
            rdbfilename=rdbfilename,
        )

    async def talk_to_master(self, master_host: str, master_port: int) -> None:
        reader, writer = await asyncio.open_connection(master_host, master_port)
        print(f"Connected to master at {master_host}:{master_port}")

        data = await self.handshake_with_master(reader, writer)
        print(f"[After handshake]: {data}")
        try:
            while True:
                try:
                    while (
                        len(data) > 0
                    ):  # Process multiple commands came as a chunk in data!
                        before_process_length = len(data)
                        parsed, data = RespParser.decode(data)
                        ret = await self.request_handler.handle(
                            parsed, peer_info=writer.get_extra_info("peername")
                        )
                        if ret.code == 201:
                            print(f"Returning: {ret.data}")
                            writer.write(ret.data)
                            await writer.drain()
                        self.request_handler.processed_commands_from_master += (
                            before_process_length - len(data)
                        )
                    data += await reader.read(1024)
                    print("From master", data)
                    if not data:
                        print("Master closed the connection.")
                        break
                except RespParserError as err:
                    print(f"[WARNING] {err}: {data}")
                    pass
        except asyncio.CancelledError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def handshake_with_master(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        timeout: int = 10,
    ) -> bytes:
        async def send_and_wait(
            send_data: bytes, stop: Callable[[bytes], int]
        ) -> bytes:
            while True:
                writer.write(send_data)
                await writer.drain()
                recvd_bytes: bytes = b""
                start = datetime.now()
                while True:
                    recvd_bytes += await reader.read(1024)
                    processed = stop(recvd_bytes)
                    if stop(recvd_bytes) > 0:
                        return recvd_bytes[processed:]
                    if (datetime.now() - start).total_seconds() > timeout:
                        break

        # First, send ping
        await send_and_wait(
            RespParser.encode(["PING"], type="bulk"),
            lambda x: len(x) if RespParser.decode(x)[0] == "PONG" else 0,
        )
        print("[Handshake] Ping completed")

        # Second, REPLCONF messages
        await send_and_wait(
            RespParser.encode(
                ["REPLCONF", "listening-port", str(self.port)], type="bulk"
            ),
            lambda x: len(x) if RespParser.decode(x)[0] == "OK" else 0,
        )
        print("[Handshake] Replconf completed [1]")
        await send_and_wait(
            RespParser.encode(["REPLCONF", "capa", "psync2"], type="bulk"),
            lambda x: len(x) if RespParser.decode(x)[0] == "OK" else 0,
        )
        print("[Handshake] Replconf completed [2]")

        def get_psync_resp(x: bytes) -> int:
            print("[PSYNC]", x)
            length_idx = x.find(b"$")
            length_end_idx = x.find(b"\r\n", length_idx)
            if length_idx != -1 and length_end_idx != -1:
                length = int(x[length_idx + 1 : length_end_idx])
                if RespParser.decode(x[:length_idx])[0].startswith(
                    "FULLRESYNC"
                ) and length <= len(x) - (length_end_idx + 2):
                    return (length_end_idx + 2) + length
            return 0

        resp = await send_and_wait(
            RespParser.encode(["PSYNC", "?", "-1"], type="bulk"),
            get_psync_resp,
        )
        print(f"[Handshake] PSYNC completed")
        return resp

    async def start(self) -> None:
        server = await asyncio.start_server(
            lambda r, w: handle_client(r, w, self.request_handler),
            "localhost",
            self.port,
            reuse_port=True,
            family=socket.AF_INET,
        )
        tasks = [server.serve_forever()]
        if self.role == "slave":
            tasks.append(
                asyncio.create_task(
                    self.talk_to_master(self.master_host, self.master_port)
                )
            )

        address = server.sockets[0].getsockname()
        print(f"Serving on {address}")

        async with server:
            await asyncio.gather(*tasks)
            # await server.serve_forever()
