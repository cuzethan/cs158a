import socket
import threading
from dataclasses import dataclass, field
from typing import Callable, Optional

from protocol import (
    DISCOVERY_BUFFER, FILE_LIST, PEER_ACK, PEER_REQUEST, TCP_TYPES, UDP_PORT,
    UDP_TYPES, Address, Message, decode_message, encode_message, make_message,
    receive_message, send_message as send_protocol_message,
)

LISTENER_TIMEOUT = 0.5
CONNECT_TIMEOUT = 3.0

@dataclass(eq=False)
class PeerConnection:
    """TCP connection to a peer"""

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
        self.event_lock = threading.RLock()
        self.threads: list[threading.Thread] = []

    @staticmethod
    def _close_socket(connection: socket.socket) -> None:
        """Close the socket and shutdown the connection"""
        try:
            connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        connection.close()

    def _start_worker(self, target: Callable, *args) -> bool:
        """Helper function for starting UDP listener, TCP accept loop, receive loop threads"""
        with self.lock:
            if self.stop_event.is_set():
                return False
            worker = threading.Thread(target=target, args=args, daemon=True)
            worker.start()
            self.threads.append(worker)
        return True

    def _track_session(self, session: PeerConnection) -> bool:
        """Record TCP session so shutdown can find and close the socket"""
        with self.lock:
            if self.stop_event.is_set():
                return False
            self.sessions.append(session)
        return True

    def start(self) -> None:
        """Open the UDP and TCP sockets and start threads for each listener"""
        if self.stop_event.is_set():
            raise OSError("network is already stopped")
        if self.tcp_listener is not None or self.udp_socket is not None:
            raise OSError("Network is already started")
        try:
            # create and bind the TCP listener socket
            self.tcp_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.tcp_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.tcp_listener.bind(("", self.tcp_port))
            self.tcp_listener.listen()
            self.tcp_listener.settimeout(LISTENER_TIMEOUT)

            # create and bind the UDP socket
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

            # Allow several nodes on one host to receive discovery broadcasts
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
        """Broadcast discovery message over UDP and log it"""
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
        """Keep listening for new nodes to discover"""
        listener = self.udp_socket

        # Wait for a message
        while not self.stop_event.is_set():
            try:
                payload, address = listener.recvfrom(DISCOVERY_BUFFER)
            except socket.timeout: # If the timeout is reached, continue the loop
                continue
            except OSError: # if the socket is closed, break the loop
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

            # Ignore messages from yourself
            if peer_id == self.node_id:
                continue
            if message["type"] == PEER_REQUEST:
                ack = make_message(PEER_ACK, self.node_id, recipient_id=peer_id,
                                   tcp_ip=self.local_ip, tcp_port=self.tcp_port)
                try:
                    self._send_discovery(ack)
                except OSError:
                    # If the ACK fails to send, keep listening
                    continue

            # If message is an ACK, connect to the peer
            elif message["recipient_id"] == self.node_id:
                with self.lock:
                    if (self.stop_event.is_set() or peer_id in self.connections
                            or peer_id in self.connecting):
                        continue
                    self.connecting.add(peer_id)

                # Connect over TCP
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
        """Helper function to connect to a peer over TCP"""
        session = None
        receiving = False
        try:
            session = PeerConnection(socket.socket(socket.AF_INET, socket.SOCK_STREAM), address, peer_id=peer_id, outbound=True)
            if not self._track_session(session):
                return
            session.socket.settimeout(CONNECT_TIMEOUT)
            session.socket.connect(address)
            session.socket.setblocking(True)
            if self._register_connection(session):
                receiving = self._start_worker(self._receive_loop, session)
        except OSError:
            pass
        finally:
            with self.lock:
                self.connecting.discard(peer_id)
            if session is not None and not receiving:
                self._drop_connection(session)

    def _accept_loop(self) -> None:
        """TCP accept loop to handle incoming connections"""

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
        """Identify the peer from the message"""

        # receive_message() already validates the UUID and required fields
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
        """Register an identified session once, resolving duplicate connections."""
        displaced = None
        with self.event_lock:
            with self.lock:
                if self.stop_event.is_set():
                    return False
                existing = self.connections.get(session.peer_id)
                # Check for an existing connection
                if existing is not None:

                    # Resolve duplicates
                    prefer_outbound = self.node_id < session.peer_id
                    if (existing.outbound == prefer_outbound
                            or session.outbound != prefer_outbound):
                        return False
                    displaced = existing
                self.connections[session.peer_id] = session
            if displaced is not None:
                self._drop_connection(displaced)

            self.on_connect(session.peer_id, session)
        return True

    def _receive_loop(self, session: PeerConnection) -> None:
        """Receive messages from the peer"""
        try:
            # Keep receiving messages until the network is stopped
            while not self.stop_event.is_set():
                message = receive_message(session.socket)
                self.log_message("received", session.address, message["type"])
                if message["type"] not in TCP_TYPES:
                    raise ValueError("Discovery messages belong on UDP")
                peer_id = self._identify_peer(session, message)

                # Register the peer
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
            pass
        finally:
            self._drop_connection(session)

    def send_message(self, peer_id: str, message: Message) -> None:
        """Send a TCP message to a connected peer"""

        # Validate the message
        message_type = message.get("type") if isinstance(message, dict) else None
        if not isinstance(message_type, str) or message_type not in TCP_TYPES:
            raise ValueError("Discovery messages belong on UDP")
        if message.get("sender_id") != self.node_id:
            raise ValueError("Outgoing sender_id must match this node")
        with self.lock:
            session = self.connections.get(peer_id)
        if session is None:
            raise OSError(f"Peer is not connected: {peer_id}")

        # Send the message if the peer is connected
        try:
            with session.send_lock:
                with self.lock:
                    if self.connections.get(peer_id) is not session:
                        raise OSError(f"Peer connection was replaced: {peer_id}")
                send_protocol_message(session.socket, message)
                self.log_message("sent", session.address, message["type"])
        except OSError:
            self._drop_connection(session)
            raise

    def _drop_connection(self, session: PeerConnection) -> None:
        """Close the socket and remove the connection from the network"""
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
                raise RuntimeError("Call network.stop() from the agent's main thread")
            self.stop_event.set()
            listeners = (self.udp_socket, self.tcp_listener)
            self.udp_socket = self.tcp_listener = None
            sessions = list(self.sessions)
            workers = list(self.threads)
        for listener in listeners:
            if listener is not None:
                self._close_socket(listener)

        # Close every socket first so blocked reads/connects can finish, even if a later application callback raises an error
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
