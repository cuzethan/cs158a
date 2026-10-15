"""Peer and file table shown by the peer and file commands.

network.py owns the sockets. agent.py reports connection changes here.
"""

import threading
from dataclasses import dataclass

from protocol import Address, validate_node_id


@dataclass(frozen=True)
class PeerInfo:
    """One connected peer, copied out of the table for inspection."""

    peer_id: str
    address: Address
    files: tuple[str, ...]


@dataclass(frozen=True)
class FileOffer:
    """One filename offered by one peer."""

    filename: str
    peer_id: str
    address: Address


class State:
    """Locked table of each peer's ID, address, and available filenames."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._peers: dict[str, PeerInfo] = {}

    def add_peer(self, peer_id: str, address: Address) -> None:
        """Record a peer that just connected. Its file list starts empty."""
        validate_node_id(peer_id)
        stored = PeerInfo(peer_id, _copy_address(address), ())
        with self._lock:
            self._peers[peer_id] = stored

    def remove_peer(self, peer_id: str) -> None:
        """Drop a disconnected peer and every filename it offered."""
        with self._lock:
            self._peers.pop(peer_id, None)

    def replace_files(self, peer_id: str, filenames: list[str]) -> None:
        """Replace this peer's offers. An empty list clears the previous ones."""
        if not isinstance(filenames, list) or not all(
            isinstance(name, str) for name in filenames
        ):
            raise ValueError("filenames must be a list of strings")
        with self._lock:
            current = self._peers.get(peer_id)
            if current is None:
                raise KeyError(f"Peer is not connected: {peer_id}")
            self._peers[peer_id] = PeerInfo(
                current.peer_id, current.address, tuple(filenames)
            )

    def peer_snapshot(self) -> list[PeerInfo]:
        """Copy of every connected peer. Mutating it does not change the table."""
        with self._lock:
            return list(self._peers.values())

    def file_snapshot(self) -> list[FileOffer]:
        """Copy of every current offer. The same filename can appear once per peer."""
        with self._lock:
            return [
                FileOffer(filename, peer.peer_id, peer.address)
                for peer in self._peers.values()
                for filename in peer.files
            ]


def _copy_address(address: Address) -> Address:
    if (
        not isinstance(address, tuple)
        or len(address) != 2
        or not isinstance(address[0], str)
        or type(address[1]) is not int
    ):
        raise ValueError("address must be an (ip, port) pair")
    return (address[0], address[1])
