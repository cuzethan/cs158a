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
import uuid
import json
import time
from collections import deque
from pathlib import Path

BUFFER_SIZE = 1024  

class Message:
    def __init__(self, uuid: uuid.UUID, flag: int):
        self.uuid = uuid
        self.flag = flag
    
    #turns message into bytes for sending
    def serialize(self):
        return json.dumps({"uuid": str(self.uuid), "flag": self.flag}).encode()

#loads config file containing info on neighbors
def load_config(path="config.txt"):
    lines = Path(path).read_text().strip().splitlines()
    server_ip, server_port = lines[0].split(",")
    peer_ip, peer_port = lines[1].split(",")
    return (server_ip.strip(), int(server_port)), (peer_ip.strip(), int(peer_port))

def run_server(server_ip, server_port, message_queue):
    #creates socket and binds to port; accept once and keep the connection
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((server_ip, server_port))
    server_sock.listen(1)

    print(f"[Server] Listening on {server_port}")
    conn, addr = server_sock.accept()
    print(f"[Server] Connected by {addr}")
    while True:
        data = conn.recv(BUFFER_SIZE)
        if not data:
            break
        msg_data = json.loads(data.decode())
        print(f"[TCP Server] Received: {msg_data!r}")
        message_queue.append(Message(uuid.UUID(msg_data["uuid"]), msg_data["flag"]))

def main():
    ((server_ip, server_port), (peer_ip, peer_port)) = load_config()

    my_uuid = uuid.uuid4()
    leader_id = None
    state = 0  # 0: electing, 1: leader known
    message_queue = deque[Message]() 

    server_thread = threading.Thread(target=run_server, args=(server_ip, server_port, message_queue),  daemon=True)
    server_thread.start()

    input("press Enter when other server is ready to connect.")

    #client side runs
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        
        sock.connect((peer_ip, peer_port))
        print(f"[TCP Client] Connected to {peer_ip}:{peer_port}")
        #send initial message to server w/o comparison
        sock.sendall(Message(my_uuid, 0).serialize())

        while True:
            #if no message, wait a bit
            if not message_queue:
                time.sleep(0.1)
                continue
            message = message_queue.popleft()

            # leader announcement: learn leader, forward once, then stop sending
            if message.flag == 1:
                if state == 0:
                    leader_id = message.uuid
                    state = 1
                    sock.sendall(message.serialize())
                    print(f"leader is {leader_id}")
                # already know the leader; do not forward again (termination)
            elif state == 1:
                # election finished; ignore further election traffic
                print("IGNORED")
            elif message.uuid > my_uuid:
                sock.sendall(message.serialize())
            elif message.uuid == my_uuid:
                # own uuid returned -> this node is the leader
                leader_id = my_uuid
                state = 1
                sock.sendall(Message(my_uuid, 1).serialize())
                print(f"leader is {leader_id}")
            else:
                print("IGNORED")
            
if __name__ == "__main__":
    main()