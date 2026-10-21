# pa3: file-share

### by Ethan Le, Brendan Ly, Vincent Schubert

## Our Message format

Every message is a JSON object with a `type` field and a `sender_id` field. `type` identifies the message kind; `sender_id` identifies the sending node. UDP messages are one datagram each, so the datagram boundary is the message boundary. TCP messages share one byte stream, so each one is framed with a length prefix.

### Node identity (using UUID)

Each running node generates one UUIDv4 at startup with `node_id = new_node_id()` from `protocol.py` (which uses `str(uuid.uuid4())`). Reuse this ID as `sender_id` in every message that node sends, including TCP messages. IDs use the canonical lowercase, hyphenated UUIDv4 string format, such as `8d972042-1f3b-4d75-a5b5-8f5e9a6c2d41`. Each process has its own ID, even when several nodes share an IP address; restarting a node generates a new ID.

- Ignore UDP messages whose `sender_id` equals your own node ID.
- When answering `PEER_REQUEST`, copy its `sender_id` into the ACK's `recipient_id`. The ACK's `sender_id` is the responder's UUID. Only the node matching `recipient_id` connects to the advertised TCP address and port.
- The requester knows the server's UUID from the ACK. Once connected, it immediately sends `FILE_LIST` with its own `sender_id`. The accepting node learns the client's UUID from that first list, registers the peer, and immediately sends its own list.
- Every later TCP message must carry the same sender UUID associated with that connection. Reject self-connections or a changed identity. Use UUIDs as peer-table keys to prevent duplicate connections.

Discovery requests and ACKs are broadcast to UDP port **54321**. Every node listens on that port; ACKs are filtered by `recipient_id`. Only the requester connects to the ACK's advertised TCP endpoint. If simultaneous startup creates two connections between a pair, both nodes keep the connection initiated by the lexicographically smaller UUID. A single connection is kept regardless of which node initiated it.

`Network.start()` opens listeners; call `Network.broadcast_request()` afterward. The agent's `on_connect(peer_id, session)` callback must immediately send its local `FILE_LIST`, and its `on_disconnect(peer_id)` callback must remove that peer's files and pending requests and log the departure. `network.py` logs message sends/receives through `log_message`; the agent logs peer additions/removals. No explicit goodbye message is sent.

Call `Network.start()` and `Network.stop()` from the agent's main thread. During shutdown, `stop()` closes all sockets and joins the workers. Callbacks should return promptly and should not call `stop()` themselves.

For example, construct a request with `make_message(DATA_REQUEST, node_id, request_id=1, filename="notes.txt")`. `request_id` remains an integer identifying a file request; it is separate from the node UUID.

### TCP framing

TCP does not preserve message boundaries. One `recv` can return part of a message, or several messages stuck together. Each TCP message is therefore sent as:

1. A 4-byte big-endian unsigned integer: the length `N` of the JSON payload in bytes.
2. Exactly `N` bytes of UTF-8 JSON.

The length counts only the JSON, not the 4-byte prefix. The receiver reads 4 bytes, interprets `N`, then reads exactly `N` more bytes, looping on short reads until both pieces are complete. The next message, if any, begins at the following byte.

### File content

File bytes travel inside the JSON as base64 text in the `data` field of `FILE_DATA`. The whole message stays one structured text object, so the same JSON parser handles control fields and file contents. Base64 costs about one third extra size compared with a raw byte payload after the length header. That overhead is acceptable here because the format stays simple and one framing rule covers every TCP message.

`size` is the original file length in bytes, before encoding. The receiver base64-decodes `data` and checks that the decoded length equals `size`.

### Message types

All five types require `type` and `sender_id`. Additional fields are listed below.

| Message type | Transport | Additional fields |
| --- | --- | --- |
| `PEER_REQUEST` | UDP | None |
| `PEER_ACK` | UDP | `recipient_id`: requester UUIDv4 string; `tcp_ip`, `tcp_port` |
| `FILE_LIST` | TCP | `files`: list of filename strings |
| `DATA_REQUEST` | TCP | `request_id`, `filename` |
| `FILE_DATA` | TCP | `request_id`, `filename`, `status`; if `ok`: `size`, `data`; if `error`: `error` |

**`PEER_REQUEST`** (UDP). Discovers peers; the requester connects after receiving their ACKs.

```json
{"type": "PEER_REQUEST", "sender_id": "8d972042-1f3b-4d75-a5b5-8f5e9a6c2d41"}
```

**`PEER_ACK`** (UDP). Tells the requester where to connect.

```json
{"type": "PEER_ACK", "sender_id": "cc357388-05c4-480d-b517-ae6a801f357e", "recipient_id": "8d972042-1f3b-4d75-a5b5-8f5e9a6c2d41", "tcp_ip": "127.0.0.1", "tcp_port": 5001}
```

**`FILE_LIST`** (TCP). Names the files this peer is sharing.

```json
{"type": "FILE_LIST", "sender_id": "cc357388-05c4-480d-b517-ae6a801f357e", "files": ["notes.txt", "photo.png"]}
```

**`DATA_REQUEST`** (TCP). Asks for one file. `request_id` ties this request to the later `FILE_DATA`.

```json
{"type": "DATA_REQUEST", "sender_id": "8d972042-1f3b-4d75-a5b5-8f5e9a6c2d41", "request_id": 1, "filename": "notes.txt"}
```

**`FILE_DATA`** (TCP), success. `data` is the file contents as base64. `size` is the decoded byte length.

```json
{"type": "FILE_DATA", "sender_id": "cc357388-05c4-480d-b517-ae6a801f357e", "request_id": 1, "filename": "notes.txt", "status": "ok", "size": 12, "data": "aGVsbG8gd29ybGQK"}
```

**`FILE_DATA`** (TCP), failure. No `size` or `data` field. `error` is a short reason.

```json
{"type": "FILE_DATA", "sender_id": "cc357388-05c4-480d-b517-ae6a801f357e", "request_id": 1, "filename": "notes.txt", "status": "error", "error": "file not found"}
```
## Run instructions

- Prepare setup by creating the folders and sample files
```
cd /Users/your_user/cs158a
mkdir -p pa3/shared/node{1,2,3,4,5}/shared
echo "This is a file in Node1" > pa3/shared/node1/shared/hello.txt
echo "This is a file in Node5" > pa3/shared/node5/shared/leaving.txt
```

- Open five terminal tabs, and each one run:
```
  python3 pa3/agent.py --port 55001 --ip 127.0.0.1 --dir pa3/shared/{node_name}
```
- To show peers or files, run the following commands:
```
peers
files
```
- To download a file from a node (replace node-ID with the UUID prefix)
```
get <node-ID> your_node_name.txt
```


## Division of work
Brendan Ly: worked on protocol.py and network.py parts
- protocol.py: Defines the node's format protocol and how they send + interpret messages
- network.py: Manages the UDP + TCP connections and sockets

Ethan Le: worked on state.py, logging_utils.py, and catalog.py files
- state.py: stores peers and file tables
- logging_utils.py: creates log files
- catalog.py: handles file advertisements

Vincent Schubert: worked on transfers.py and agent.py
- transfers.py: handles downloading and serving files
- agent.py: Program entry point that combines everything together, handles terminal commands that run the demo

## Execution Sample Screenshots


Run 5 nodes in 5 separate terminals
<img width="2544" height="788" alt="Screenshot 2026-10-20 at 1 16 46 PM" src="https://github.com/user-attachments/assets/dab7d95e-6719-4be1-9cf4-bf8e773853ff" />

In node2, demonstrate discovery and file lists by typing commands 'peers' and 'files'
<img width="702" height="451" alt="Screenshot 2026-10-20 at 1 17 02 PM" src="https://github.com/user-attachments/assets/252538cd-69e5-4eba-b39f-06772be06cb3" />

Transfer a file from node1 to node2 by calling node1's UUID prefix
<img width="715" height="97" alt="Screenshot 2026-10-20 at 1 17 33 PM" src="https://github.com/user-attachments/assets/32db0fd8-f935-4b5a-8fda-0ee14c83032e" />

After waiting 10 seconds, transfer a file from node2 to node3
<img width="634" height="449" alt="Screenshot 2026-10-20 at 1 18 31 PM" src="https://github.com/user-attachments/assets/43a8d1fe-3469-42fe-81f3-0484b0019779" />

Disconnect on node5, other nodes can see that node5 disconnected
<img width="984" height="403" alt="Screenshot 2026-10-20 at 1 18 59 PM" src="https://github.com/user-attachments/assets/82fe9981-f406-42e7-a279-f56dddeb9e3d" />

Node 5's file list is dropped

<img width="717" height="455" alt="Screenshot 2026-10-20 at 1 19 48 PM" src="https://github.com/user-attachments/assets/c6ba6d8d-d930-47b3-b361-5a368f46c2a7" />




