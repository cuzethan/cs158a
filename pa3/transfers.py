"""
handle file requests, serving files, and saving downloaded copies.

request side: request() sends the DATA_REQUEST and records it as pending
handle_response() mathces the FILE_DATA reply, checks and saves it

serving side: handle_request() reads the file from shared/ and then replies with the FILE_DATA or erro if gone
TCP stays open
"""

# get our imports for the transfers
import base64
import binascii
import itertools
import os
import shutil
import threading
from pathlib import Path
from typing import Callable
from catalog import TEMP_SUFFIXES
from protocol import DATA_REQUEST, FILE_DATA, Message, make_message
from state import State


PART_SUFFIX = ".part"
MAX_FILE_SIZE = 100 * 1024 * 1024


# return the name if the file inside the shared or else give ValueError
def check_filename(name: object):
    if not isinstance(name, str) or not name:
        raise ValueError("filename must be non empty string")
    if name in (".", "..") or any(ch in name for ch in ("/", "\\", "\x00")):
        raise ValueError(f"invalid filename: {name!r}")
    if name.startswith("."):
        raise ValueError(f"hidden files are not shared: {name!r}")
    if name.endswith(TEMP_SUFFIXES):
        raise ValueError(f"temp files are not shared: {name!r}")
    if Path(name).name != name:
        raise ValueError(f"invalid filename: {name!r}")
    return name


# pending request table plus the request, serving, and saving logic
class Transfers:
    def __init__(
        self,
        node_id: str,
        shared_dir: str | Path,
        downloads_dir: str | Path,
        network,
        state: State,
        notify: Callable[[str], None] = print,
    ):
        self.node_id = node_id
        self.shared_dir = Path(shared_dir)
        self.downloads_dir = Path(downloads_dir)
        self.network = network
        self.state = state
        self.notify = notify
        self._lock = threading.Lock()
        self._ids = itertools.count(1)
        self._pending: dict[int, tuple[str, str]] = {}

    # requesting side


    # send a DATA_REQUEST raise ValueError witha  user facing reaosn
    
    def request(self, peer_id: str, filename: str):
        check_filename(filename)
        advertised = any(
            offer.peer_id == peer_id and offer.filename == filename
            for offer in self.state.file_snapshot()
        )
        if not advertised:
            raise ValueError(f"that peer does not advertise {filename}")
        if (self.shared_dir / filename).exists():
            #keep our current
            raise ValueError(f"{filename} is already in shared/")
        
        with self._lock:
            if any(name == filename for _, name in self._pending.values()):
                raise ValueError(f"{filename} is already being downloaded")
            request_id = next(self._ids)
            self._pending[request_id] = (peer_id, filename)
        
        message = make_message(
            DATA_REQUEST, self.node_id, request_id=request_id, filename=filename
        )
        try:
            self.network.send_message(peer_id, message)
        except OSError as error:
            self._forget(request_id)
            raise ValueError(f"could not send request: {error}") from error
        return request_id


    # process a FILE_DATA reply to one of our requests
    def handle_response(self, peer_id: str, message: Message):
        request_id = message["request_id"]
        filename = message["filename"]
        with self._lock:
            matched = self._pending.get(request_id) == (peer_id, filename)
        if not matched:
            self.notify(f"get ignored unexpected FILE_DATA for {filename!r}")
            return
        try:
            if message["status"] == "error":
                self.notify(f"get {filename} failed: {message['error']}")
                return
            try:
                data = base64.b64decode(message["data"], validate=True)
            except (binascii.Error, ValueError):
                self.notify(f"get {filename} failed data is not valid base64")
                return
            if len(data) != message["size"]:
                self.notify(
                    f"get {filename} failed expected {message['size']} bytes, "
                    f"got {len(data)}"
                )
                return
            try:
                shared = self._save(filename, data)
            except OSError as error:
                self.notify(f"get {filename} failed to save: {error}")
                return
            where = "downloads/ and shared/" if shared else "downloads/ (kept existing shared copy)"
            self.notify(f"get {filename} saved to {where} ({len(data)} bytes)")
        finally:
            self._forget(request_id)

    
    # drop requests waiting on peer that disconnected
    def cancel_peer(self, peer_id: str):
        with self._lock:
            dropped = [
                (request_id, name)
                for request_id, (owner, name) in self._pending.items()
                if owner == peer_id
            ]
            for request_id, _ in dropped:
                del self._pending[request_id]
        for _, name in dropped:
            self.notify(f"get {name} cancelled: peer disconnected")
    
    #copy for the request that are in flight
    def pending_snapshot(self):
        with self._lock:
            return [(rid, pid, name) for rid, (pid, name) in self._pending.items()]
        
    
    def _forget(self, request_id: int):
        with self._lock:
            self._pending.pop(request_id, None)

    
    # core writing to downloads and then publish to shared
    def _save(self, filename: str, data: bytes):
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        downloaded = self.downloads_dir / filename
        temp = self.downloads_dir / (filename + PART_SUFFIX)
        temp.write_bytes(data)
        os.replace(temp, downloaded)

        final = self.shared_dir / filename
        if final.exists():
            return False
        part = self.shared_dir / (filename + PART_SUFFIX)
        try:
            shutil.copyfile(downloaded, part)
            if final.exists():
                return False
            os.replace(part, final)
            return True
        finally:
            part.unlink(missing_ok=True)



## serving side

    # answer a DATA_REQUEST on a sep thread
    def handle_request(self, peer_id: str, message: Message):
        worker = threading.Thread(
            target = self._serve,
            args = (peer_id, message["request_id"], message["filename"]),
            name = f"serve-{message['request_id']}",
            daemon=True,
        )
        worker.start()

    def _serve(self, peer_id: str, request_id: int, filename: str):
        reply = self._build_reply(request_id, filename)
        try:
            self.network.send_message(peer_id, reply)
        except OSError:
            pass
        if reply["status"] == "ok":
            self.notify(f"serve sent {filename!r} {reply['size']} bytes")
        else:
            self.notify(f"serve could not send {filename!r}: {reply['error']}")


    #return a FILE_DATA success or error message 
    def _build_reply(self, request_id: int, filename: str):
        def error(reason: str):
            return make_message(
                FILE_DATA, self.node_id, request_id=request_id,
                filename=filename, status= "error", error=reason
            )
        
        try:
            check_filename(filename)
        except ValueError:
            return error("invalid filename")
        path = self.shared_dir / filename
        try:
            if not path.is_file():
                return error("file not found")
            if path.stat().st_size > MAX_FILE_SIZE:
                return error("file too large")
            raw = path.read_bytes()
        except FileNotFoundError:
            return error("file not found")
        except OSError:
            return error("could not read file")
        
        return make_message(
            FILE_DATA, self.node_id, request_id=request_id, filename=filename, 
            status="ok", size=len(raw),
            data=base64.b64encode(raw).decode("ascii"),
        )