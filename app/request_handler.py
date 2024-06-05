from __future__ import annotations

import asyncio
import bisect
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Tuple

from app.rdb_parser import RdbParser
from app.resp_parser import RespParser
from app.container import Container, StreamEntries, StreamEntry


@dataclass
class Response:
    code: Literal[
        200, 201, 202, 203, 400
    ]  # 200 response to client, 201 response to master, 202 response to replica,203 replica connection establish, 400 error
    data: bytes | list[bytes]


@dataclass
class Xread:
    block: bool
    block_duration: int | None  # sec
    stream_keys: list
    starts: list[StreamEntry]

    @staticmethod
    def parse(input: list) -> Xread:
        block = False
        block_duration = None
        offset = 0
        if input[1] == "block":
            offset = 2
            block = True
            block_duration = int(input[2]) / 1000

        if input[offset + 1] == "streams":
            stream_keys = []
            starts = []
            idx = 2 + offset
            while idx < len(input) and not StreamEntry.validate_input_id_format(
                input[idx]
            ):
                stream_keys.append(input[idx])
                idx += 1
            while idx < len(input):
                starts.append(StreamEntry(id=input[idx], data={}))
                idx += 1

            if len(stream_keys) != len(starts):
                ValueError("Key and start id are not matching")
            return Xread(block, block_duration, stream_keys, starts)
        else:
            ValueError(f"Unknown input {input}")


class RequestHandler:
    def __init__(
        self,
        role: str = "master",
        master_host: str | None = None,
        master_port: int | None = None,
        master_replid: int | None = None,
        master_repl_offset: str | None = None,
        dir: Path | None = None,
        rdbfilename: str | None = None,
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
        self.dir = dir
        self.rdb_filename = rdbfilename
        if self.dir is not None and self.rdb_filename is not None:
            dbfile = self.dir / self.rdb_filename
            rdb_parser = RdbParser(dbfile)
            rdb_parser.parse()
            for k, v in rdb_parser.kv.items():
                if "expiry" in v.keys() and v["expiry"] is not None:
                    self.container.set(
                        k,
                        v["value"],
                        expire_at=datetime.fromtimestamp(v["expiry"] / 1000.0),
                    )
                else:
                    self.container.set(k, v["value"])

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
        peer_info: Tuple[str, int] | None = None,
    ) -> Response:
        if isinstance(input, list) and isinstance(input[0], str):
            match input[0].upper():
                case "ECHO":
                    return Response(200, RespParser.encode(input[1]))
                case "PING":
                    return Response(200, RespParser.encode("PONG"))
                case "SET":
                    if len(input) < 3:
                        raise ValueError("Invalid usage of SET")
                    if len(input) == 5 and input[3] == "px":
                        self.container.set(
                            input[1],
                            input[2],
                            expire_at=datetime.now()
                            + timedelta(milliseconds=int(input[4])),
                        )
                    else:
                        self.container.set(input[1], input[2])
                    await self.propagte_commands(input)
                    return Response(200, RespParser.encode("OK"))
                case "GET":
                    if len(input) != 2:
                        raise ValueError("Invalid usage of GET")
                    return Response(
                        200,
                        RespParser.encode(
                            self.container.get(input[1]),
                            type=(
                                "bulk"
                                if isinstance(self.container.get(input[1]), str)
                                else ""
                            ),
                        ),
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
                            print(
                                "Haven't sent anything..."
                            )  # No previous write operations. So don't have to ask.
                            self.responded_replica += 1
                        else:
                            wr.write(send_data)
                            await wr.drain()
                    # For simplicity, we only send GET ACK ONCE. Rely on TCP's in-order ACK. If we sent A,B,C and got reply for C,
                    # means A and B were transferred successfully.
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
                case "CONFIG":
                    if len(input) >= 3 and input[1] == "GET":
                        if input[2] == "dir":
                            return Response(
                                200,
                                RespParser.encode(["dir", str(self.dir)], type="bulk"),
                            )
                        elif input[2] == "dbfilename":
                            return Response(
                                200,
                                RespParser.encode(
                                    ["dir", self.rdb_filename], type="bulk"
                                ),
                            )
                case "KEYS":
                    if len(input) == 2 and input[1] == "*":
                        return Response(
                            200,
                            RespParser.encode(self.container.keys(), type="bulk"),
                        )
                case "TYPE":
                    if len(input) == 2 and input[1] in self.container.kv:
                        return Response(
                            200,
                            RespParser.encode(self.container.kv[input[1]].type),
                        )
                    else:
                        return Response(
                            200,
                            RespParser.encode("none"),
                        )
                case "XADD":
                    if len(input) < 3:
                        raise ValueError("Invalid input")
                    stream_key = input[1]
                    stream_id = input[2]
                    data = {}
                    for i in range(3, len(input), 2):
                        if i + 1 >= len(input):
                            raise ValueError("Missing value")
                        data[input[i]] = input[i + 1]
                    try:
                        self.container.set(
                            key=stream_key, value=StreamEntry(id=stream_id, data=data)
                        )
                        entries = self.container.get(stream_key).entries
                        return Response(
                            200,
                            RespParser.encode(entries[len(entries) - 1].id),
                        )
                    except ValueError as err:
                        return Response(
                            200,
                            RespParser.encode(
                                str(err),
                                type="err",
                            ),
                        )
                case "XRANGE":
                    stream_key = input[1]
                    start = StreamEntry(id=input[2], data={})
                    end = StreamEntry(id=input[3], data={})
                    if stream_key in self.container.keys():
                        entries = self.container.get(stream_key).entries

                        # keys = [key_func(elem) for elem in entries]
                        start_id = bisect.bisect_left(
                            entries,
                            StreamEntries.key_func(start),
                            key=StreamEntries.key_func,
                        )
                        end_id_excl = bisect.bisect_right(
                            entries,
                            StreamEntries.key_func(end),
                            key=StreamEntries.key_func,
                        )
                        l = entries[start_id:end_id_excl]
                        return Response(200, RespParser.encode(l, type="bulk"))
                    else:
                        pass  # unknown key
                case "XREAD":
                    xread = Xread.parse(input)
                    print("XREAD", xread)
                    if xread.block:
                        if xread.block_duration > 0:
                            # Block this task for this period of time!
                            await asyncio.sleep(xread.block_duration)
                        else:  # block until there is an item added (busy wait)
                            while result := self.container.get_after_excl(
                                xread.stream_keys, xread.starts
                            ):
                                if result[1] > 0:
                                    return Response(
                                        200,
                                        RespParser.encode(result[0], type="bulk"),
                                    )
                                await asyncio.sleep(0.1)

                    res, entry_length = self.container.get_after_excl(
                        xread.stream_keys, xread.starts
                    )
                    print("Result", res)
                    if entry_length == 0:
                        return Response(200, RespParser.encode(None))
                    return Response(
                        200,
                        RespParser.encode(res, type="bulk"),
                    )
        print(f"Unknown command {input}")
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
