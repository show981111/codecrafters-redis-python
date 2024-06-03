import asyncio

from collections import defaultdict
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
            self.replica_addr_to_writer: dict[Tuple[str, int], asyncio.StreamWriter] = (
                {}
            )
            self.sent_commands: defaultdict[asyncio.StreamWriter, int] = defaultdict(
                int
            )

    def from_master(self, peer_info: Tuple[str, int] | None = None):
        def is_local_host(address):
            # Check if the address is loopback
            if address == "127.0.0.1" or address == "::1" or address == "localhost":
                return True

        print("FROM MASTER", peer_info)
        return (
            peer_info is not None
            and self.role == "slave"
            and (
                self.master_host == peer_info[0]
                or (is_local_host(self.master_host) and is_local_host(peer_info[0]))
            )
            and self.master_port == peer_info[1]
        )

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
                            if self.from_master(peer_info):
                                return Response(
                                    201,
                                    RespParser.encode(
                                        [
                                            "REPLCONF",
                                            "ACK",
                                            f"{self.processed_commands_from_master}",
                                        ],
                                        type="bulk",
                                    ),
                                )  # last arg should be #bytes that replica processed
                            else:
                                print("Not the master but sent an ACK request")
                    elif len(input) == 3 and input[1] == "ACK":
                        print("ACK FROM REPLICA", input)
                        if self.wait and self.sent_commands[
                            self.replica_addr_to_writer[peer_info]
                        ] == int(input[2]):
                            self.responded_replica += 1
                        return Response(202, b"")
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
                    self.responded_replica = 0
                    send_data = RespParser.encode(
                        ["REPLCONF", "GETACK", "*"], type="bulk"
                    )

                    for wr in self.replicas.keys():
                        if self.sent_commands[wr] == 0:
                            print("Haven't sent anything...")
                            self.responded_replica += 1
                        else:
                            wr.write(send_data)
                            await wr.drain()

                    try:
                        async with asyncio.timeout(int(input[2]) / 1000):
                            while self.responded_replica < int(input[1]):
                                await asyncio.sleep(0)
                                continue
                    except asyncio.TimeoutError:
                        pass

                    for wr in self.replicas.keys():
                        if self.sent_commands[wr] > 0:
                            self.sent_commands[wr] += len(send_data)
                    return Response(200, RespParser.encode(self.responded_replica))
        print("Unknown command")
        return Response(400, b"")

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

    def discard_wr(self, wr: asyncio.StreamWriter, address: Tuple[str, int]) -> None:
        if self.role == "master" and wr in self.replicas.keys():
            self.replicas.pop(wr)
            self.replica_addr_to_writer.pop(address)
            print(f"Replica disconncted: {address}")
