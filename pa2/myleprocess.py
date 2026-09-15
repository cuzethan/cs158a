"""
Leader election process for an asynchronous non-anonymous ring.

Each node:
  - listens as a TCP server for one neighbor
  - connects as a TCP client to the other neighbor
  - runs leader election election using UUID comparisons
  - logs Received / Sent / Ignored events to a log file

Config file (two lines):
  <my_ip>,<my_port>          # this node's server bind address
  <peer_ip>,<peer_port>      # neighbor to connect to as client

Example:
  python3 myleprocess.py --config config1.txt --log log1.txt
"""

import argparse
import logging
import threading
import socket
import uuid
import json
import time
from collections import deque
from pathlib import Path

BUFFER_SIZE = 1024

def setup_logger(log_path="log.txt"):
    """Write plain log lines to log_path (overwrites each run)."""
    logger = logging.getLogger("le")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.FileHandler(log_path, mode="w")
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger

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

def run_server(server_ip, server_port, message_queue, ready):
    #creates socket and binds to port; accept once and keep the connection
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((server_ip, server_port))
    server_sock.listen(1)

    print(f"[Server] Listening on {server_port}")
    ready.set()  # let main show the Enter prompt after this line
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", default="log.txt", help="log file path")
    parser.add_argument("--config", default="config.txt", help="config file path")
    args = parser.parse_args()

    logger = setup_logger(args.log)
    ((server_ip, server_port), (peer_ip, peer_port)) = load_config(args.config)

    my_uuid = uuid.uuid4()
    logger.info(f"Process id: {my_uuid}")
    print(f"Process id: {my_uuid}")

    leader_id = None
    state = 0  # 0: electing, 1: leader known
    message_queue = deque[Message]()
    
    #this is just for print statements to be in order
    server_ready = threading.Event()

    server_thread = threading.Thread(
        target=run_server,
        args=(server_ip, server_port, message_queue, server_ready),
        daemon=True,
    )
    server_thread.start()
    server_ready.wait()  # wait until Listening has printed

    input("press Enter when other server is ready to connect.")

    #client side runs
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        
        sock.connect((peer_ip, peer_port))
        print(f"[TCP Client] Connected to {peer_ip}:{peer_port}")
        #send initial message to server w/o comparison
        sock.sendall(Message(my_uuid, 0).serialize())
        logger.info(f"Sent: uuid={my_uuid}, flag=0")

        while True:
            #if no message, wait a bit
            if not message_queue:
                time.sleep(0.1)
                continue
            message = message_queue.popleft()

            # log every received message before deciding what to do
            if message.uuid > my_uuid:
                cmp = "greater"
            elif message.uuid == my_uuid:
                cmp = "same"
            else:
                cmp = "less"
            if state == 1:
                logger.info(
                    f"Received: uuid={message.uuid}, flag={message.flag}, {cmp}, {state}, leader={leader_id}"
                )
            else:
                logger.info(
                    f"Received: uuid={message.uuid}, flag={message.flag}, {cmp}, {state}"
                )

            # if there's a leader announcement
            if message.flag == 1:
                #if not already know the leader, set the leader and state to 1
                if state == 0:
                    leader_id = message.uuid
                    state = 1
                    sock.sendall(message.serialize())
                    logger.info(f"Sent: uuid={message.uuid}, flag=1")
                    print(f"leader is {leader_id}")
                    logger.info(f"Leader is decided to {leader_id}.")
                # if leader known, don't forward again (termination)
            elif state == 1:
                #leader already known, can ignore election traffic
                print(f"Ignored: uuid={message.uuid}, flag={message.flag}")
                logger.info(f"Ignored: uuid={message.uuid}, flag={message.flag}")
            #if message from a higher uuid, forward it to the server
            elif message.uuid > my_uuid:
                sock.sendall(message.serialize())
                logger.info(f"Sent: uuid={message.uuid}, flag={message.flag}")
            #if message from own uuid, set leader and state to 1
            elif message.uuid == my_uuid:
                #own uuid returned -> this node is the leader
                leader_id = my_uuid
                state = 1
                sock.sendall(Message(my_uuid, 1).serialize())
                logger.info(f"Sent: uuid={my_uuid}, flag=1")
                print(f"leader is {leader_id}")
                logger.info(f"Leader is decided to {leader_id}.")
            else:
                print(f"Ignored: uuid={message.uuid}, flag={message.flag}")
                logger.info(f"Ignored: uuid={message.uuid}, flag={message.flag}")
            
if __name__ == "__main__":
    main()