# KVServer

A in-memory key-value store server written in C++, inspired by Redis.
It supports string keys, sorted sets (ZSETs), and time-to-live (TTL) expiration.
The server communicates over a custom binary protocol via TCP, and includes a Python client for sending and testing commands.

---

## Features

- Basic operations: `SET`, `GET`, `DEL`, `KEYS`
- Expiration support: `PEXPIRE`, `PTTL`
- Sorted sets (ZSET): `ZADD`, `ZREM`, `ZSCORE`, `ZQUERY`
- Persistent TCP server using `epoll`
- Idle connection timeout handling
- TTL eviction via min-heap
- Custom binary protocol
- Python client for integration testing

---

## Protocol Format

Binary protocol:
```yaml
[ total_len: uint32 ][ num_args: uint32 ]
[
    [arg_len: uint32][arg_data: bytes]
    [arg_len: uint32][arg_data: bytes]
    ...
]
```

## Project Structure

```console
kvserver/
├── CMakeLists.txt # CMake build script
├── main.cpp # Main server logic
├── hashtable.* # Custom hash map
├── zset.* # AVL-based sorted set
├── heap.* # Min-heap for TTL expiration
├── list.* # Doubly linked list for idle connection tracking
├── common.* # Shared utilities
├── client.py # Python test client
└── README.md # This file
```
---
## Getting Started

### Prerequisites

- Linux
- CMake ≥ 3.10
- GCC / Clang (C++17)
- Python ≥ 3.6 (for client)
---

## Build Instructions

```bash
# Clone the repo
git clone <this-repo-url>
cd kvserver

# Create build folder
mkdir build && cd build

# Generate Makefile
cmake ..

# Build
make
```
---
 #### Run the Server
```bash
./kvserver
```
  #### Output:
```bash
the server is listening
```
 #### For Help section 
```bash
./kvserver help
```
---

## Python Client

A Python client is available to send commands and test the server.

#### Example Usage

Update the <b>HOST</b> variable in client.py to match the IP address of the machine where the server is running:

```bash
python3 client.py
```
---
## Commands Supported

| Command                                             | Description                      |
|-----------------------------------------------------|----------------------------------|
| `set <key> <value>`                                 | Set string value                 |
| `get <key>`                                         | Get value                        |
| `del <key>`                                         | Delete key                       |
| `keys`                                              | List all keys                    |
| `pexpire <key> <ms>`                                | Set TTL in milliseconds          |
| `pttl <key>`                                        | Get remaining TTL                |
| `zadd <zset> <score> <name>`                        | Add to sorted set                |
| `zrem <zset> <name>`                                | Remove from sorted set           |
| `zscore <zset> <name>`                              | Get score of a member            |
| `zquery <zset> <score> <name> <offset> <limit>`     | Range query                      |

###  Sample Commands Tested

```bash
set age 12
get age
pexpire age 30000
pttl age
zadd leaderboard 200 Bob
zquery leaderboard 100 "" 0 5
zrem leaderboard Bob
zscore leaderboard Alice
keys
```
---
 ## Notes

- Server listens on port 8085

- Connections are closed after 5s of inactivity

- TTL eviction is handled periodically using a min-heap

- This is a prototype — no persistence or replication (yet)
