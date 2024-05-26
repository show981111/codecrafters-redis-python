# Uncomment this to pass the first stage
import socket


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    print("Listening from port 6379")
    while True:
        conn_sock, client_addr = server_socket.accept()  # wait for client
        print(f"Accepted the request from {client_addr}")
        with conn_sock:
            while True:
                data = conn_sock.recv(10)
                if not data:
                    break
                conn_sock.sendall(
                    "+PONG\r\n".encode()
                )  # Unlike send(), this method continues to send data from bytes until either all data has been sent or an error occurs


if __name__ == "__main__":
    main()
