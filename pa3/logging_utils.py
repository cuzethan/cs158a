"""Per-node log.txt for messages and peer add/remove events."""

import threading
from datetime import datetime, timezone
from pathlib import Path

from protocol import Address

DEFAULT_LOG_PATH = "log.txt"


class Logger:
    """Append timestamped lines to one log.txt for this running node."""

    def __init__(self, path: str | Path = DEFAULT_LOG_PATH) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Fresh file for this process so a demo run is easy to read.
        self.path.write_text("", encoding="utf-8")

    def log_message(self, direction: str, address: Address, message_type: str) -> None:
        """Record a sent or received message. File contents are not logged."""
        if direction not in ("sent", "received"):
            raise ValueError("direction must be 'sent' or 'received'")
        if not isinstance(message_type, str) or not message_type:
            raise ValueError("message_type must be a non-empty string")
        self._write(f"{direction} {_format_address(address)} {message_type}")

    def log_peer_added(self, peer_id: str, address: Address) -> None:
        """Record a peer after a successful connection."""
        self._write(f"peer added {peer_id} {_format_address(address)}")

    def log_peer_removed(self, peer_id: str, address: Address | None = None) -> None:
        """Record a peer after a detected disconnection."""
        if address is None:
            self._write(f"peer removed {peer_id}")
        else:
            self._write(f"peer removed {peer_id} {_format_address(address)}")

    def _write(self, detail: str) -> None:
        line = f"{_timestamp()} {detail}\n"
        with self._lock:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(line)


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _format_address(address: Address) -> str:
    if (
        not isinstance(address, tuple)
        or len(address) != 2
        or not isinstance(address[0], str)
        or type(address[1]) is not int
    ):
        raise ValueError("address must be an (ip, port) pair")
    return f"{address[0]}:{address[1]}"
