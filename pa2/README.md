# PA2 — Leader Election

Run three copies of `myleprocess.py` in a ring to elect a leader by UUID.

## Setup

1. Open **three terminals**.
2. In each terminal, `cd` into this directory:

```bash
cd pa2
```

3. Create three config files that form a ring (listen on one port, connect to the next):

**config1.txt**
```text
127.0.0.1,5001
127.0.0.1,5002
```

**config2.txt**
```text
127.0.0.1,5002
127.0.0.1,5003
```

**config3.txt**
```text
127.0.0.1,5003
127.0.0.1,5001
```

Line 1 is this process’s server address. Line 2 is the neighbor it connects to as a client.

## Run the demo

Start one process per terminal:

**Terminal 1**
```bash
python3 myleprocess.py --config config1.txt --log log1.txt
```

**Terminal 2**
```bash
python3 myleprocess.py --config config2.txt --log log2.txt
```

**Terminal 3**
```bash
python3 myleprocess.py --config config3.txt --log log3.txt
```

Wait until each terminal prints `[Server] Listening on ...`, then press **Enter** in all three so they connect.

When election finishes, each process prints `leader is <uuid>`, and the same leader ID appears in `log1.txt`, `log2.txt`, and `log3.txt`.

## Example terminal output

From a local 3-node demo. All three processes elect the same leader:
`c7608e43-c7e0-48ce-928a-348e020b32a6`.

**Terminal 1** (`config1.txt` / port 5001)
```text
Process id: 762c1845-12c9-4cdc-b291-20881f6278e9
[Server] Listening on 5001
press Enter when other server is ready to connect.

[TCP Client] Connected to 127.0.0.1:5002
[Server] Connected by ('127.0.0.1', 50074)
[TCP Server] Received: {'uuid': '73f0858f-9d7e-454f-a28b-6ccd7052b248', 'flag': 0}
[TCP Server] Received: {'uuid': 'c7608e43-c7e0-48ce-928a-348e020b32a6', 'flag': 0}
Ignored: uuid=73f0858f-9d7e-454f-a28b-6ccd7052b248, flag=0
[TCP Server] Received: {'uuid': 'c7608e43-c7e0-48ce-928a-348e020b32a6', 'flag': 1}
leader is c7608e43-c7e0-48ce-928a-348e020b32a6
```

**Terminal 2** (`config2.txt` / port 5002)
```text
Process id: c7608e43-c7e0-48ce-928a-348e020b32a6
[Server] Listening on 5002
press Enter when other server is ready to connect.
[Server] Connected by ('127.0.0.1', 50071)
[TCP Server] Received: {'uuid': '762c1845-12c9-4cdc-b291-20881f6278e9', 'flag': 0}

[TCP Client] Connected to 127.0.0.1:5003
Ignored: uuid=762c1845-12c9-4cdc-b291-20881f6278e9, flag=0
[TCP Server] Received: {'uuid': 'c7608e43-c7e0-48ce-928a-348e020b32a6', 'flag': 0}
leader is c7608e43-c7e0-48ce-928a-348e020b32a6
[TCP Server] Received: {'uuid': 'c7608e43-c7e0-48ce-928a-348e020b32a6', 'flag': 1}
```

**Terminal 3** (`config3.txt` / port 5003)
```text
Process id: 73f0858f-9d7e-454f-a28b-6ccd7052b248
[Server] Listening on 5003
press Enter when other server is ready to connect.
[Server] Connected by ('127.0.0.1', 50072)
[TCP Server] Received: {'uuid': 'c7608e43-c7e0-48ce-928a-348e020b32a6', 'flag': 0}

[TCP Client] Connected to 127.0.0.1:5001
[TCP Server] Received: {'uuid': 'c7608e43-c7e0-48ce-928a-348e020b32a6', 'flag': 1}
leader is c7608e43-c7e0-48ce-928a-348e020b32a6
```

Stop a process with `Ctrl+C` when you are done.
