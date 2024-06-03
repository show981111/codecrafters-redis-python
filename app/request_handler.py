import asyncio

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Tuple
from app.resp_parser import RespParser
from app.container import Container


@dataclass
class Response:
    code: Literal[
        200, 201, 202, 203, 400
    ]  # 200 response to client, 201 response to master, 202 response to replica,203 replica connection establish, 400 error
    data: bytes | list[bytes]


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
            self.processed_commands_from_master = 0  # in bytes
        else:
            self.master_replid = master_replid
            self.master_repl_offset = master_repl_offset
            self.replicas: dict[asyncio.StreamWriter, asyncio.StreamReader] = {}
            self.sent_commands: dict[asyncio.StreamWriter, int] = {}

    async def handle(
        self,
        input: list | int | str,
        bytes_length: int = 0,
        peer_info: Tuple[str, int] | None = None,
    ) -> Response:
        if isinstance(input, list) and isinstance(input[0], str):
            # if (
            #     self.wait
            #     and not (input[0].upper() == "REPLCONF" and input[1] == "ACK")
            # ):
            #     return b""
            # elif self.wait and datetime.now() - self.wait_started > self.timeout: # timeout
            #     ret = len(self.wait_responded)
            #     self.wait_responded.clear()
            #     return ret
            # elif self.wait and self.wait_for >= len(self.wait_responded): # wait condition met
            #     self.wait_responded.clear()
            #     return
            # elif self.wait: # within timeout, wait condition not met yet.

            match input[0].upper():
                case "ECHO":
                    return Response(200, RespParser.encode(input[1]))
                case "PING":
                    return Response(200, RespParser.encode("PONG"))
                case "SET":
                    if len(input) < 3:
                        raise ValueError("Invalid usage of SET")
                    if len(input) == 5 and input[3] == "px":
                        self.container.set(input[1], input[2], expiry=float(input[4]))
                    else:
                        self.container.set(input[1], input[2])
                    await self.propagte_commands(input)
                    return Response(200, RespParser.encode("OK"))
                case "GET":
                    if len(input) != 2:
                        raise ValueError("Invalid usage of GET")
                    return Response(
                        200, RespParser.encode(self.container.get(input[1]))
                    )
                case "INFO":
                    if len(input) != 2:
                        raise ValueError("Invalid usage of GET")
                    if input[1] == "replication":
                        return Response(
                            200,
                            RespParser.encode(
                                self.get_info(),
                                type="bulk",
                            ),
                        )
                case "REPLCONF":
                    print("REPLCONF INPUT", input)
                    if len(input) == 3 and input[1] == "GETACK" and input[2] == "*":
                        # Master asks for Ack
                        if input[2] == "*" and self.role == "slave":
                            if peer_info:
                                print(
                                    "PEER INFO",
                                    peer_info,
                                    "Master host",
                                    self.master_host,
                                    "Master port",
                                    self.master_port,
                                )
                                client_host, client_port = peer_info
                                if (
                                    client_host == self.master_host
                                    and client_port == self.master_port
                                ):
                                    return Response(
                                        201,
                                        RespParser.encode(
                                            [
                                                "REPLCONF",
                                                "ACK",
                                                f"{self.processed_commands_from_master}",
                                            ]
                                        ),
                                    )  # last arg should be #bytes that replica processed
                            else:
                                print("Not the master but sent an ACK request")
                    elif len(input) == 3 and input[1] == "ACK":
                        offset = int(input[2])  # Response from replica for getAck

                    return Response(200, RespParser.encode("OK"))
                case "PSYNC":
                    if self.role != "master":
                        raise ValueError("Role is not a master but got PSYNC ")
                    return Response(
                        203,
                        [
                            RespParser.encode(
                                f"FULLRESYNC {self.master_replid} {self.master_repl_offset}"
                            ),
                            RespParser.encode(RespParser.empty_rdb_hex, type="rdb"),
                        ],
                    )
                case "WAIT":
                    self.wait = True
                    self.wait_started = datetime.now()
                    self.wait_for = int(input[1])
                    self.timeout = int(input[2])

                    ret = await self.handle_wait()
                    return Response(200, RespParser.encode(ret))
        print("Unknown command")
        return Response(400, b"")

    ##### TODO: Unsure about this part. How to listen do multiple stream readers while we are making sure about the timeout
    ##### and exit condition (stop wait if n number of replicas processed the command)?
    async def handle_wait(
        self,
        timeout: int,
        num_replicas: int,
    ) -> int:
        completed = 0
        tasks = []
        for writer, reader in self.replicas.items():

            async def wait_for_ack(
                writer: asyncio.StreamWriter, reader: asyncio.StreamReader
            ) -> asyncio.StreamWriter:
                """Send GETACK to replica and wait for an Ack(infinitely). Loop until we get what we want(enough number of commands processed)"""
                expect = self.sent_commands[writer]
                if expect == 0:
                    return writer
                send_data = RespParser.encode(["REPLCONF", "GETACK", "*"])
                while True:  # Send & recv loop
                    send = False
                    writer.write(send_data)
                    self.sent_commands[writer] += len(send_data)
                    await writer.drain()
                    data = b""
                    while True:  # Recv loop
                        data += await reader.read(512)
                        if data:
                            parsed, _ = RespParser.decode(data)
                            if (
                                len(parsed) == 3
                                and parsed[0].upper() == "REPLCONF"
                                and parsed[1] == "ACK"
                            ):
                                if int(parsed[2]) == expect:
                                    return writer
                                else:
                                    send = True  # Need to resend the commands
                                    data = b""
                                    break
                        else:
                            break
                        if send:
                            break

            tasks.append(  # run this function for max timeout. So that the as_completed can exit within timeout.
                asyncio.wait_for(wait_for_ack(writer, reader), timeout=timeout)
            )

        for coroutine in asyncio.as_completed(tasks):
            try:
                ret = await coroutine
                if ret:
                    completed += 1
                    if completed >= num_replicas:
                        # Note: Due to this, the result will never be greater than num_replicas...
                        # However, we don't know when the next task will be done. So it is an early exit, without waiting for all "timeout"
                        return completed

            except Exception as e:
                pass

        return completed

    def get_info(self) -> str:
        if self.role == "slave":
            return f"role:{self.role}"
        else:
            return f"role:{self.role}\nmaster_replid:{self.master_replid}\nmaster_repl_offset:{self.master_repl_offset}"

    async def propagte_commands(self, input: list | int | str) -> None:
        if self.role == "master":
            for wr in self.replicas.keys():
                d = RespParser.encode(input, type="bulk")
                wr.write(d)
                self.sent_commands[wr] += len(d)
                await wr.drain()
                print("Propagete Done!")

    def discard_wr(self, wr: asyncio.StreamWriter) -> None:
        if self.role == "master":
            self.replicas.pop(wr)
