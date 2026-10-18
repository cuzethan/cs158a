"""
P2P file sharing node 

each running node needs its own --dir . it holds that nodes shared/ downloads/ and log.txt
"""

import argparse
import socket
import threading
from pathlib import Path
from catalog import Catalog
from logging_utils import Logger
from network import Network, PeerConnection
from protocol import DATA_REQUEST, FILE_DATA, FILE_LIST, Message, new_node_id
from state import State
from transfers import Transfers

HELP = """commands:
  peers                     list connected peers
  files                     list files offered by peers, and our own shared files
  get <peer_id> <filename>  download a file (a unique prefix of peer_id is enough)
  help                      show this help
  quit                      leave the network and exit"""

SHORT_ID = 8 # chars of a UUID shown in the listings


# best guess at this LAN address No packet sent
def detect_local_ip():
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("10.255.255.255",1))
        return probe.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        probe.close()

# read from the command line
def parse_args():
    parser = argparse.ArgumentParser(description="P2P file-sharing agent")
    parser.add_argument("--port", type=int, required=True,
                        help="TCP port this node listens on (advertised in PEER_ACK)")
    parser.add_argument("--ip", default=None,
                        help="IP other nodes use to reach this node (default: auto-detect)")
    parser.add_argument("--broadcast", default="255.255.255.255",
                        help="UDP broadcast address (default: 255.255.255.255)")
    parser.add_argument("--dir", default=".",
                        help="data directory holding shared/, downloads/, log.txt")
    return parser.parse_args()


# this will be one running node
class Agent:
    # set up the dirs, logs, and every module and network callbacks
    def __init__(self, port: int, ip: str, broadcast: str, data_dir: str):
        self.node_id = new_node_id()
        root = Path(data_dir)
        self.shared_dir = root / "shared"
        self.downloads_dir = root / "downloads"
        self.shared_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir.mkdir(parents=True, exist_ok=True)

        self.stop_event = threading.Event()
        self.logger = Logger(root / "log.txt")
        self.state = State()
        self.network = Network(
            self.node_id, ip, port, broadcast, self.stop_event,
            on_connect=self.on_connect,
            on_message=self.on_message,
            on_disconnect=self.on_disconnect,
            log_message=self.logger.log_message,
        )
        self.catalog = Catalog(
            self.node_id, self.shared_dir, self.network, self.state, self.stop_event
        )
        self.transfers = Transfers(
            self.node_id, self.shared_dir, self.downloads_dir,
            self.network, self.state, notify=self.show,
        )
        self.ip, self.port = ip, port
    
    #open the listeners first so the node is ready for ACKS then it discovers
    def start(self):
        self.network.start()
        self.catalog.start()
        self.network.broadcast_request()
        self.show(f"node {self.node_id[:SHORT_ID]} listening on {self.ip}:{self.port}")

    #close all sockets and join threads
    def shutdown(self):
        self.stop_event.set()
        self.network.stop()
        self.catalog.stop()

    #new peer connected so add to table log it and send our file list
    def on_connect(self, peer_id: str, session: PeerConnection) -> None:
        self.state.add_peer(peer_id, session.address)  # before any FILE_LIST arrives
        self.logger.log_peer_added(peer_id, session.address)
        self.show(f"peer connected {peer_id[:SHORT_ID]} {_fmt(session.address)}")
        try:
            self.catalog.advertise_to(peer_id)
        except OSError:
            pass

    # route each incoming TCP message to the the catalog or transfers by type
    def on_message(self, peer_id: str, message: Message):
        message_type = message["type"]
        if message_type == FILE_LIST:
            self.catalog.apply_remote_list(peer_id, message)
        elif message_type == DATA_REQUEST:
            self.transfers.handle_request(peer_id, message)
        elif message_type == FILE_DATA:
            self.transfers.handle_response(peer_id, message)
    
    # peer left so drop it and its files, log it ma dn cancel reqs waiting on it
    def on_disconnect(self, peer_id: str):
        address = next(
            (p.address for p in self.state.peer_snapshot() if p.peer_id == peer_id), None
        )
        self.state.remove_peer(peer_id)
        self.logger.log_peer_removed(peer_id, address)
        self.transfers.cancel_peer(peer_id)
        self.show(f"peer disconnected {peer_id[:SHORT_ID]}")

    #read user commands until quit
    def command_loop(self):
        print(HELP)
        while not self.stop_event.is_set():
            line = input("> ").strip()
            if not line:
                continue
            command, _, rest = line.partition(" ")
            if command == "quit":
                return
            elif command == "peers":
                self.cmd_peers()
            elif command == "files":
                self.cmd_files()
            elif command == "get":
                self.cmd_get(rest)
            elif command == "help":
                print(HELP)
            else:
                print(f"Unknown command: {command} (type 'help')")

    #print every connected peer with its address and file count
    def cmd_peers(self):
        peers = self.state.peer_snapshot()
        if not peers:
            print("no connected peers")
        for peer in sorted(peers, key=lambda p: p.peer_id):
            print(f" {peer.peer_id} {_fmt(peer.address)} {len(peer.files)} file(s)")
    
    # print files offered by peers, then our own shared files
    def cmd_files(self):
        offers = self.state.file_snapshot()
        if not offers:
            print("no files offered by peers")
        for offer in sorted(offers, key=lambda o: (o.filename, o.peer_id)):
            print(f" {offer.filename:<30} peer {offer.peer_id[:SHORT_ID]} {_fmt(offer.address)}")
        local = self.catalog.list_local_files()
        print(f" ours: { ', '.join(local) if local else '(none)'}")

    #parese get peer id , filename and send the req
    def cmd_get(self, rest: str):
        parts = rest.split(maxsplit=1)
        if len(parts) != 2:
            print("usage: get <peer_id> <filename>")
            return
        prefix, filename = parts
        try:
            peer_id = self.resolve_peer(prefix)
            self.transfers.request(peer_id, filename)
        except ValueError as error:
            print(f"get failed: {error}")
            return
        print(f"requested {filename} from {peer_id[:SHORT_ID]}")

    #expand the ID prefix to the one connected peer match
    def resolve_peer(self, prefix: str):
        matches = [p.peer_id for p in self.state.peer_snapshot()
                   if p.peer_id.startswith(prefix)]
        if not matches:
            raise ValueError(f"no connected peer matches {prefix!r}")
        if len(matches) > 1:
            raise ValueError(f"{prefix!r} matches several peers; type more of the ID")
        return matches[0]
    
    #print events from background threads
    @staticmethod
    def show(text: str):
        print(text, flush=True)

# format an ip port and tuple
def _fmt(address):
    return f"{address[0]}:{address[1]}"

# build the node, start it, run commands, and shut down
def main():
    args = parse_args()
    agent = Agent(args.port, args.ip or detect_local_ip(), args.broadcast, args.dir)
    try:
        agent.start()
        agent.command_loop()
    except (KeyboardInterrupt, EOFError):
        print()
    except OSError as error:
        print(f"network error: {error}")
    finally:
        agent.shutdown()
        print("bye")

if __name__ == "__main__":
    main()