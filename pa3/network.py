
import socket
import threading
from dataclasses import dataclass, field
from typing import Callable, Optional

from protocol import (
    DISCOVERY_BUFFER, FILE_LIST, PEER_ACK, PEER_REQUEST, TCP_TYPES, UDP_PORT,
    UDP_TYPES, Address, Message, decode_message, encode_message, make_message,
    receive_message, send_message as send_protocol_message,
    validate_node_id,
)

LISTENER_TIMEOUT = 0.5
CONNECT_TIMEOUT = 3.0


@dataclass(eq=False)
class PeerConnection:
    """One bidirectional TCP session, with a lock for whole-message sends."""

    socket: socket.socket
    address: Address
    peer_id: Optional[str] = None
    outbound: bool = False
    send_lock: threading.Lock = field(default_factory=threading.Lock)


class Network:
    def __init__(
        self,
        node_id: str,
        local_ip: str,
        tcp_port: int,
        broadcast_ip: str,
        stop_event,
        on_connect,
        on_message,
        on_disconnect,
        log_message,
    ) -> None:
        validate_node_id(node_id)
        self.node_id = node_id
        self.local_ip = local_ip
        self.tcp_port = tcp_port
        self.broadcast_ip = broadcast_ip
        self.stop_event = stop_event
        self.on_connect = on_connect
        self.on_message = on_message
        self.on_disconnect = on_disconnect
        self.log_message = log_message
        self.udp_socket: Optional[socket.socket] = None
        self.tcp_listener: Optional[socket.socket] = None
        self.connections: dict[str, PeerConnection] = {}
        self.connecting: set[str] = set()
        self.sessions: list[PeerConnection] = []
        self.lock = threading.Lock()
        # Serialize peer callbacks with registration/removal so an old worker
        # cannot deliver a file list after its peer has already been removed.
        # RLock permits on_connect to call send_message, including its cleanup.
        self.event_lock = threading.RLock()
        self.threads: list[threading.Thread] = []

    @staticmethod
    def _close_socket(connection: socket.socket) -> None:
        """Unblock readers and close, including sockets not yet connected."""
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        connection.close()

    def _start_worker(self, target: Callable, *args) -> bool:
        """Keep worker handles until stop() joins them."""
        with self.lock:
            if self.stop_event.is_set():
                return False
            worker = threading.Thread(target=target, args=args, daemon=True)
            worker.start()
            self.threads.append(worker)
        return True

    def _track_session(self, session: PeerConnection) -> bool:
        with self.lock:
            if self.stop_event.is_set():
                return False
            self.sessions.append(session)
        return True

    def start(self) -> None:
        """Call from the main thread before broadcasting the startup request."""
        if self.stop_event.is_set():
            raise OSError("Cannot start a stopped Network; create a new instance")
        if self.tcp_listener is not None or self.udp_socket is not None:
            raise OSError("Network is already started")
        try:
            self.tcp_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tcp_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.tcp_listener.bind(("", self.tcp_port))
            self.tcp_listener.listen()
            self.tcp_listener.settimeout(LISTENER_TIMEOUT)

            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            # Allow several nodes on one host to receive discovery broadcasts.
            if hasattr(socket, "SO_REUSEPORT"):
                self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            self.udp_socket.bind(("", UDP_PORT))
            self.udp_socket.settimeout(LISTENER_TIMEOUT)
            self._start_worker(self._udp_loop)
            self._start_worker(self._accept_loop)
        except Exception:
            self.stop()
            raise

    def _send_discovery(self, message: Message) -> None:
        payload = encode_message(message)
        with self.lock:
            listener = self.udp_socket
        if self.stop_event.is_set() or listener is None:
            raise OSError("UDP listener is not running")
        address = (self.broadcast_ip, UDP_PORT)
        listener.sendto(payload, address)
        self.log_message("sent", address, message["type"])

    def broadcast_request(self) -> None:
        self._send_discovery(make_message(PEER_REQUEST, self.node_id))

    def _udp_loop(self) -> None:
        listener = self.udp_socket
        while not self.stop_event.is_set():
            try:
                payload, address = listener.recvfrom(DISCOVERY_BUFFER)
            except socket.timeout:
                continue
            except OSError:
                if self.stop_event.is_set():
                    break
                raise
            try:
                message = decode_message(payload)
            except ValueError:
                continue
            if message["type"] not in UDP_TYPES:
                continue
            self.log_message("received", address, message["type"])
            peer_id = message["sender_id"]
            if peer_id == self.node_id:
                continue
            if message["type"] == PEER_REQUEST:
                ack = make_message(PEER_ACK, self.node_id, recipient_id=peer_id,
                                   tcp_ip=self.local_ip, tcp_port=self.tcp_port)
                try:
                    self._send_discovery(ack)
                except OSError:
                    # UDP replies are best effort; a failed reply must not stop
                    # the lifetime discovery listener.
                    continue
            elif message["recipient_id"] == self.node_id:
                with self.lock:
                    if (self.stop_event.is_set() or peer_id in self.connections
                            or peer_id in self.connecting):
                        continue
                    self.connecting.add(peer_id)
                address = (message["tcp_ip"], message["tcp_port"])
                try:
                    started = self._start_worker(self._connect_to_peer, peer_id, address)
                except Exception:
                    with self.lock:
                        self.connecting.discard(peer_id)
                    raise
                if not started:
                    with self.lock:
                        self.connecting.discard(peer_id)

    def _connect_to_peer(self, peer_id: str, address: Address) -> None:
        session = None
        receiving = False
        try:
            session = PeerConnection(socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                                     address, peer_id=peer_id, outbound=True)
            if not self._track_session(session):
                return
            session.socket.settimeout(CONNECT_TIMEOUT)
            session.socket.connect(address)
            session.socket.setblocking(True)
            if self._register_connection(session):
                receiving = self._start_worker(self._receive_loop, session)
        except OSError:
            # The ACK's node may have left before we could finish connecting.
            pass
        finally:
            with self.lock:
                self.connecting.discard(peer_id)
            if session is not None and not receiving:
                self._drop_connection(session)

    def _accept_loop(self) -> None:
        listener = self.tcp_listener
        while not self.stop_event.is_set():
            try:
                connection, address = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if self.stop_event.is_set():
                    break
                raise
            connection.setblocking(True)
            session = PeerConnection(connection, address)
            try:
                if (not self._track_session(session)
                        or not self._start_worker(self._receive_loop, session)):
                    self._drop_connection(session)
            except Exception:
                self._drop_connection(session)
                raise

    def _identify_peer(self, session: PeerConnection, message: Message) -> str:
        # receive_message() already validated the UUID and required fields.
        peer_id = message["sender_id"]
        if peer_id == self.node_id:
            raise ValueError("Cannot connect to this node itself")
        if session.peer_id is None:
            if message.get("type") != FILE_LIST:
                raise ValueError("An accepted peer must identify itself with FILE_LIST")
        elif peer_id != session.peer_id:
            raise ValueError("Sender ID changed on an established connection")
        return peer_id

    def _register_connection(self, session: PeerConnection) -> bool:
        """Install the session, then notify the agent outside the table lock."""
        displaced = None
        with self.event_lock:
            with self.lock:
                if self.stop_event.is_set() or session.peer_id is None:
                    return False
                existing = self.connections.get(session.peer_id)
                if existing is session:
                    return True
                if existing is not None:
                    # Both ends keep the socket initiated by the smaller UUID
                    # when two nodes discover each other simultaneously.
                    prefer_outbound = self.node_id < session.peer_id
                    if (existing.outbound == prefer_outbound
                            or session.outbound != prefer_outbound):
                        return False
                    displaced = existing
                self.connections[session.peer_id] = session
            if displaced is not None:
                self._drop_connection(displaced)
            # The agent can now find this socket to send its initial FILE_LIST.
            self.on_connect(session.peer_id, session)
        return True

    def _receive_loop(self, session: PeerConnection) -> None:
        try:
            while not self.stop_event.is_set():
                message = receive_message(session.socket)
                self.log_message("received", session.address, message["type"])
                if message["type"] not in TCP_TYPES:
                    raise ValueError("Discovery messages belong on UDP")
                peer_id = self._identify_peer(session, message)
                with self.event_lock:
                    if session.peer_id is None:
                        session.peer_id = peer_id
                        if not self._register_connection(session):
                            return
                    with self.lock:
                        if self.connections.get(peer_id) is not session:
                            return
                    self.on_message(peer_id, message)
        except (EOFError, OSError, ValueError):
            # A malformed message, clean EOF, reset, or failed socket affects
            # only this session. Valid FILE_DATA error replies reach the agent.
            pass
        finally:
            self._drop_connection(session)

    def send_message(self, peer_id: str, message: Message) -> None:
        """Send a complete TCP message under this peer's send lock."""
        # Check routing here; the protocol helper validates the full schema once.
        message_type = message.get("type") if isinstance(message, dict) else None
        if not isinstance(message_type, str) or message_type not in TCP_TYPES:
            raise ValueError("Discovery messages belong on UDP")
        if message.get("sender_id") != self.node_id:
            raise ValueError("Outgoing sender_id must match this node")
        with self.lock:
            session = self.connections.get(peer_id)
        if session is None:
            raise OSError(f"Peer is not connected: {peer_id}")
        try:
            with session.send_lock:
                with self.lock:
                    if self.connections.get(peer_id) is not session:
                        raise OSError(f"Peer connection was replaced: {peer_id}")
                send_protocol_message(session.socket, message)
                self.log_message("sent", session.address, message["type"])
        except OSError:
            # Release the send lock before callbacks, which may themselves send.
            self._drop_connection(session)
            raise

    def _drop_connection(self, session: PeerConnection) -> None:
        self._close_socket(session.socket)
        with self.event_lock:
            removed = False
            with self.lock:
                if session in self.sessions:
                    self.sessions.remove(session)
                if self.connections.get(session.peer_id) is session:
                    del self.connections[session.peer_id]
                    removed = True
            if removed:
                self.on_disconnect(session.peer_id)

    def stop(self) -> None:
        """Call from the main thread to close sockets and join all workers."""
        with self.lock:
            if threading.current_thread() in self.threads:
                raise RuntimeError("Call Network.stop() from the agent's main thread")
            self.stop_event.set()
            listeners = (self.udp_socket, self.tcp_listener)
            self.udp_socket = self.tcp_listener = None
            sessions = list(self.sessions)
            workers = list(self.threads)
        for listener in listeners:
            if listener is not None:
                self._close_socket(listener)
        # Close every socket first so blocked reads/connects can finish, even
        # if a later application callback raises an error.
        for session in sessions:
            self._close_socket(session.socket)
        try:
            for session in sessions:
                self._drop_connection(session)
        finally:
            for worker in workers:
                worker.join()
            with self.lock:
                self.threads.clear()
