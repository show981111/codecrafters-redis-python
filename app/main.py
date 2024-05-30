# Uncomment this to pass the first stage
import argparse
import asyncio

from app.server import Server


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    parser = argparse.ArgumentParser(
        description="Simple server that uses a specified port."
    )
    parser.add_argument(
        "--port", type=int, default=6379, help="Port number to run the server on"
    )
    parser.add_argument(
        "--replicaof",
        type=str,
        required=False,
        help='Replica of host. Usage: "<MASTER_HOST> <MASTER_PORT>"',
    )

    args = parser.parse_args()

    role = "master"
    master_host = None
    master_port = None
    if args.replicaof is not None:
        role = "slave"
        master_host = args.replicaof.split(" ")[0]
        master_port = int(args.replicaof.split(" ")[1])
    server = Server(
        port=args.port, role=role, master_host=master_host, master_port=master_port
    )

    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
