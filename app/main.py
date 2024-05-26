# Uncomment this to pass the first stage
import socket
import asyncio


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    address = writer.get_extra_info("peername")
    print(f"Connected to {address}")

    while True:
        data = await reader.read(1024)  # Read up to 100 bytes
        # message = data.decode()
        if not data:
            break
        print(f"Received {data!r} from {address}")
        writer.write("+PONG\r\n".encode())
        await writer.drain()

    print(f"Closed connection to {address}")
    writer.close()
    await writer.wait_closed()


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server = await asyncio.start_server(
        handle_client, "localhost", 6379, reuse_port=True
    )

    address = server.sockets[0].getsockname()
    print(f"Serving on {address}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
