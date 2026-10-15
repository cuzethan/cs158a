"""Local file advertisements and handling of peer FILE_LIST messages."""

import threading
from pathlib import Path
from typing import Any

from protocol import ADVERTISEMENT_INTERVAL, FILE_LIST, Message, make_message, validate_node_id
from state import State

# Downloads write here until the transfer finishes, then rename to the final name.
TEMP_SUFFIXES = (".part", ".tmp")


class Catalog:
    """Announce shared/ filenames and apply remote FILE_LIST advertisements."""

    def __init__(
        self,
        node_id: str,
        shared_dir: str | Path,
        network: Any,
        state: State,
        stop_event: threading.Event,
    ) -> None:
        validate_node_id(node_id)
        self.node_id = node_id
        self.shared_dir = Path(shared_dir)
        self.network = network
        self.state = state
        self.stop_event = stop_event
        self._thread: threading.Thread | None = None

    def list_local_files(self) -> list[str]:
        """Scan shared/ for regular files, skipping in-progress downloads."""
        if not self.shared_dir.is_dir():
            return []
        names: list[str] = []
        for entry in self.shared_dir.iterdir():
            try:
                if not entry.is_file():
                    continue
            except OSError:
                continue
            name = entry.name
            if name.startswith("."):
                continue
            if name.endswith(TEMP_SUFFIXES):
                continue
            names.append(name)
        names.sort()
        return names

    def build_file_list(self) -> Message:
        """Build a FILE_LIST for the current contents of shared/."""
        return make_message(FILE_LIST, self.node_id, files=self.list_local_files())

    def advertise_to(self, peer_id: str) -> None:
        """Send the current list immediately after a connection forms."""
        self.network.send_message(peer_id, self.build_file_list())

    def advertise_all(self) -> None:
        """Rescan shared/ and send the list to every connected peer."""
        message = self.build_file_list()
        for peer in self.state.peer_snapshot():
            if self.stop_event.is_set():
                return
            try:
                self.network.send_message(peer.peer_id, message)
            except OSError:
                # The peer may have left between the snapshot and the send.
                continue

    def apply_remote_list(self, peer_id: str, message: Message) -> None:
        """Pass a peer's FILE_LIST into the state table."""
        if not isinstance(message, dict) or message.get("type") != FILE_LIST:
            raise ValueError("Expected a FILE_LIST message")
        files = message.get("files")
        if not isinstance(files, list):
            raise ValueError("FILE_LIST files must be a list")
        try:
            self.state.replace_files(peer_id, list(files))
        except KeyError:
            # The peer disconnected before this advertisement was applied.
            pass

    def start(self) -> None:
        """Start the periodic advertisement thread. Call from the main thread."""
        if self._thread is not None:
            raise OSError("Catalog is already started")
        if self.stop_event.is_set():
            raise OSError("Cannot start a stopped Catalog; create a new instance")
        self._thread = threading.Thread(
            target=self._advertisement_loop, name="catalog-advertise", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Join the advertisement thread after stop_event is set."""
        thread = self._thread
        if thread is None:
            return
        if threading.current_thread() is thread:
            raise RuntimeError("Call Catalog.stop() from the agent's main thread")
        thread.join()
        self._thread = None

    def _advertisement_loop(self) -> None:
        while not self.stop_event.wait(ADVERTISEMENT_INTERVAL):
            self.advertise_all()
