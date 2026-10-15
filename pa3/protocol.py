"""JSON messages and four-byte TCP framing."""

import ipaddress
import json
import socket
import struct
import uuid
from typing import Any

Message = dict[str, Any]
Address = tuple[str, int]

UDP_PORT = 54321
ADVERTISEMENT_INTERVAL = 10
HEADER = struct.Struct("!I")  # Unsigned four-byte length in network byte order.
DISCOVERY_BUFFER = 4096

PEER_REQUEST = "PEER_REQUEST"
PEER_ACK = "PEER_ACK"
FILE_LIST = "FILE_LIST"
DATA_REQUEST = "DATA_REQUEST"
FILE_DATA = "FILE_DATA"

UDP_TYPES = frozenset({PEER_REQUEST, PEER_ACK})
TCP_TYPES = frozenset({FILE_LIST, DATA_REQUEST, FILE_DATA})
MESSAGE_TYPES = UDP_TYPES | TCP_TYPES


def new_node_id() -> str:
    """Generate one UUIDv4 per running node; agent.py calls this at startup."""
    return str(uuid.uuid4())


def validate_node_id(node_id: str) -> None:
    if not isinstance(node_id, str):
        raise ValueError("Node ID must be a UUIDv4 string")
    try:
        parsed = uuid.UUID(node_id)
    except ValueError as error:
        raise ValueError("Node ID must be a UUIDv4 string") from error
    if parsed.version != 4 or str(parsed) != node_id:
        raise ValueError("Node ID must be a canonical UUIDv4 string")


def make_message(message_type: str, sender_id: str, **fields: Any) -> Message:
    """
    Construct a message with the given message type, sender ID, and additional fields
    depending on the message type as defined in the protocol

    Example: make_message(DATA_REQUEST, node_id, request_id=1, filename="notes.txt").
    """
    if message_type not in MESSAGE_TYPES:
        raise ValueError(f"Unknown message type: {message_type}")
    if "type" in fields:
        raise ValueError("Message type cannot be overridden")
    validate_node_id(sender_id)
    return {"type": message_type, "sender_id": sender_id, **fields}

def validate_message(message: Message) -> None:
    """
    Validate the message based on the message type and the fields in the message
    Raise ValueError if the message is invalid

    """
    if not isinstance(message, dict):
        raise ValueError("Message must be a dictionary")

    def require_field(name: str, expected_type: type) -> Any:
        if name not in message:
            raise ValueError(f"Missing required field: {name}")
        value = message[name]
        valid = (type(value) is int if expected_type is int
                 else isinstance(value, expected_type))
        if not valid:
            raise ValueError(f"{name} must be a {expected_type.__name__}")
        return value

    message_type = require_field("type", str)
    if message_type not in MESSAGE_TYPES:
        raise ValueError(f"Unknown message type: {message_type}")
    validate_node_id(require_field("sender_id", str))

    if message_type == PEER_ACK:
        validate_node_id(require_field("recipient_id", str))
        tcp_ip = require_field("tcp_ip", str)
        try:
            ipaddress.IPv4Address(tcp_ip)
        except ipaddress.AddressValueError as error:
            raise ValueError("tcp_ip must be an IPv4 address string") from error
        tcp_port = require_field("tcp_port", int)
        if not 1 <= tcp_port <= 65535:
            raise ValueError("tcp_port must be between 1 and 65535")

    elif message_type == FILE_LIST:
        files = require_field("files", list)
        if not all(isinstance(filename, str) for filename in files):
            raise ValueError("files must contain only filename strings")

    elif message_type in (DATA_REQUEST, FILE_DATA):
        require_field("request_id", int)
        require_field("filename", str)

        if message_type == FILE_DATA:
            status = require_field("status", str)
            if status == "ok":
                size = require_field("size", int)
                if size < 0:
                    raise ValueError("size must be nonnegative")
                require_field("data", str)
                if "error" in message:
                    raise ValueError("Successful FILE_DATA must omit error")
            elif status == "error":
                require_field("error", str)
                if "size" in message or "data" in message:
                    raise ValueError("Failed FILE_DATA must omit size and data")
            else:
                raise ValueError("FILE_DATA status must be 'ok' or 'error'")


def encode_message(message: Message) -> bytes:
    """
    Validate the message and return the UTF-8 encoded JSON string
    """

    validate_message(message)
    return json.dumps(message).encode("utf-8")

def decode_message(payload: bytes) -> Message:
    """
    Validate the message and return JSON object
    """

    try:
      message = json.loads(payload.decode("utf-8"))
      validate_message(message)
      return message
    except UnicodeDecodeError as error:
      raise ValueError("Invalid UTF-8 encoding") from error
    except json.JSONDecodeError as error:
      raise ValueError("Invalid JSON") from error


def recv_exact(connection: socket.socket, count: int) -> bytes:
    """Read count bytes; raise EOFError if the peer closes before completion."""

    if count < 0:
      raise ValueError("Count of bytes must not be negative")
    if count == 0:
      return b""

    data = b""
    while len(data) < count:
      # Accumulate recv(count - bytes_collected) until count bytes arrive.
      chunk = connection.recv(count - len(data))
      if not chunk:
        raise EOFError("Peer closed the connection before the message was complete")
      data += chunk
    return data


def send_message(connection: socket.socket, message: Message) -> None:
    """Send one TCP message."""

    payload = encode_message(message)
    if len(payload) > 2**32 - 1:
      raise ValueError("Payload size must be less than 2**32 - 1")
    connection.sendall(HEADER.pack(len(payload)) + payload)

def receive_message(connection: socket.socket) -> Message:
    """Read exactly one length-prefixed TCP message, preserving later frames."""

    header = recv_exact(connection, HEADER.size)
    length = HEADER.unpack(header)[0]
    if length == 0:
      raise ValueError("empty JSON body not allowed")
    body = recv_exact(connection, length)
    return decode_message(body)

