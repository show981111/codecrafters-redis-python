# Uncomment this to pass the first stage
import argparse
import asyncio

from app.resp_parser import RespParser, RespParserError
from app.request_handler import RequestHandler


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    address = writer.get_extra_info("peername")
    print(f"Connected to {address}")
    data = b""
    request_handler = RequestHandler()
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


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    parser = argparse.ArgumentParser(
        description="Simple server that uses a specified port."
    )
    parser.add_argument(
        "--port", type=int, required=True, help="Port number to run the server on"
    )

    args = parser.parse_args()

    server = await asyncio.start_server(
        handle_client, "localhost", args.port, reuse_port=True
    )

    address = server.sockets[0].getsockname()
    print(f"Serving on {address}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
