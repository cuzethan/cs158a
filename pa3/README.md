# pa3: file-share

### by Ethan Le, Brendan Ly, Vincent Schubert

## Message format

Every message is a JSON object with a `type` field. UDP messages are one datagram each, so the datagram boundary is the message boundary. TCP messages share one byte stream, so each one is framed with a length prefix.

### TCP framing

TCP does not preserve message boundaries. One `recv` can return part of a message, or several messages stuck together. Each TCP message is therefore sent as:

1. A 4-byte big-endian unsigned integer: the length `N` of the JSON payload in bytes.
2. Exactly `N` bytes of UTF-8 JSON.

The length counts only the JSON, not the 4-byte prefix. The receiver reads 4 bytes, interprets `N`, then reads exactly `N` more bytes, looping on short reads until both pieces are complete. The next message, if any, begins at the following byte.

### File content

File bytes travel inside the JSON as base64 text in the `data` field of `FILE_DATA`. The whole message stays one structured text object, so the same JSON parser handles control fields and file contents. Base64 costs about one third extra size compared with a raw byte payload after the length header. That overhead is acceptable here because the format stays simple and one framing rule covers every TCP message.

`size` is the original file length in bytes, before encoding. The receiver base64-decodes `data` and checks that the decoded length equals `size`.

### Message types

| Message type | Transport | Additional fields |
| --- | --- | --- |
| `PEER_REQUEST` | UDP | None |
| `PEER_ACK` | UDP | `recipient_id`, `tcp_ip`, `tcp_port` |
| `FILE_LIST` | TCP | `files`: list of filename strings |
| `DATA_REQUEST` | TCP | `request_id`, `filename` |
| `FILE_DATA` | TCP | `request_id`, `filename`, `status`; if `ok`: `size`, `data`; if `error`: `error` |

**`PEER_REQUEST`** (UDP). Asks a peer to open a TCP connection.

```json
{"type": "PEER_REQUEST"}
```

**`PEER_ACK`** (UDP). Tells the requester where to connect.

```json
{"type": "PEER_ACK", "recipient_id": "peer-a", "tcp_ip": "127.0.0.1", "tcp_port": 5001}
```

**`FILE_LIST`** (TCP). Names the files this peer is sharing.

```json
{"type": "FILE_LIST", "files": ["notes.txt", "photo.png"]}
```

**`DATA_REQUEST`** (TCP). Asks for one file. `request_id` ties this request to the later `FILE_DATA`.

```json
{"type": "DATA_REQUEST", "request_id": 1, "filename": "notes.txt"}
```

**`FILE_DATA`** (TCP), success. `data` is the file contents as base64. `size` is the decoded byte length.

```json
{"type": "FILE_DATA", "request_id": 1, "filename": "notes.txt", "status": "ok", "size": 12, "data": "aGVsbG8gd29ybGQK"}
```

**`FILE_DATA`** (TCP), failure. No `size` or `data` field. `error` is a short reason.

```json
{"type": "FILE_DATA", "request_id": 1, "filename": "notes.txt", "status": "error", "error": "file not found"}
```
