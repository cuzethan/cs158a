"""
TCP Echo Server
---------------
Waits for a client to connect, echoes back every message it receives,
then waits for the next client.

Run:
    python tcp_server.py

Then start tcp_client.py in a separate terminal.
"""

import threading
import socket
from pathlib import Path

BUFFER_SIZE = 1024  

def load_config(path="config.txt"):
    lines = Path(path).read_text().strip().splitlines()
    server_ip, server_port = lines[0].split(",")
    peer_ip, peer_port = lines[1].split(",")
    return (server_ip.strip(), int(server_port)), (peer_ip.strip(), int(peer_port))
    
((server_ip, server_port), (peer_ip, peer_port)) = load_config()

def run_server(server_ip, server_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        # Allow reusing the port immediately after the server stops.
        # Without this, you'd get "Address already in use" for ~60 seconds.
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        server_sock.bind((server_ip, server_port))
        server_sock.listen(1)
        print(f"[Server] Listening on {server_port}")
        while True:
            conn, addr = server_sock.accept()
            with conn:
                print(f"[Server] Connected by {addr}")
                while True:
                    data = conn.recv(BUFFER_SIZE)
                    if not data:
                        break
                      # repr() gives the developer representation of an object 
                    print(f"[TCP Server] Received: {data.decode()!r}")

                    # sendall() sends all bytes, retrying internally if needed.
                    conn.sendall(data)
                    print(f"[TCP Server] Echoed back.")


server_thread = threading.Thread(target=run_server, args=(server_ip, server_port),  daemon=True)
server_thread.start()

input("press Enter when server is ready.")

#client side runs
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    
    sock.connect((peer_ip, peer_port))
    print(f"[TCP Client] Connected to {peer_ip}:{peer_port}")

    while True:
        message = input("You: ").strip()

        if message.lower() == "quit":
            print("[TCP Client] Closing connection.")
            break

        if not message:
            continue

        # encode() encodes the string to bytes, and sendall() sends it to the server.
        sock.sendall(message.encode())

        # recv() BLOCKS until the server sends data back.
        response = sock.recv(1024)

        print(f"Echo: {response.decode()}\n")