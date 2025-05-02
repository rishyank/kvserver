import socket
import struct
import sys

# === Protocol Constants ===
k_max_msg = 4096 * 12
SER_NIL = 0
SER_ERR = 1
SER_STR = 2
SER_INT = 3
SER_DBL = 4
SER_ARR = 5
SER_KV = 6

# === Helper Functions ===

def msg(message):
    print(message)

def die(message):
    print(f"[ERROR] {message}", file=sys.stderr)
    sys.exit(1)

def read_full(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise IOError("Unexpected EOF")
        data += chunk
    return data

def write_all(sock, data):
    sock.sendall(data)

# === Request Builder ===

def send_req(sock, cmd):
    """
    Encodes a command into the protocol format and sends it.
    Format:
    [len:4][ncmds:4][str_len:4][str_data] ...
    """
    length = 4 + sum(4 + len(s) for s in cmd)
    if length > k_max_msg:
        raise ValueError("Message too long")

    wbuf = struct.pack('<I', length)
    wbuf += struct.pack('<I', len(cmd))
    for s in cmd:
        wbuf += struct.pack('<I', len(s))
        wbuf += s.encode('utf-8')

    write_all(sock, wbuf)

# === Response Parser ===

def on_response(data):
    """ Parses and prints the response based on the type. """
    if not data:
        msg("empty response")
        return -1

    rtype = data[0]
    if rtype == SER_NIL:
        print("(nil)")
        return 1
    elif rtype == SER_ERR:
        code, strlen = struct.unpack('<Ii', data[1:9])
        errmsg = data[9:9 + strlen].decode()
        print(f"(err) {code}: {errmsg}")
        return 9 + strlen
    elif rtype == SER_STR:
        strlen = struct.unpack('<I', data[1:5])[0]
        string = data[5:5 + strlen].decode()
        print(f"(str) {string}")
        return 5 + strlen
    elif rtype == SER_INT:
        val = struct.unpack('<q', data[1:9])[0]
        print(f"(int) {val}")
        return 9
    elif rtype == SER_DBL:
        val = struct.unpack('<d', data[1:9])[0]
        print(f"(dbl) {val}")
        return 9
    elif rtype == SER_ARR:
        arrlen = struct.unpack('<I', data[1:5])[0]
        print(f"(arr) len={arrlen}")
        offset = 5
        for _ in range(arrlen):
            rv = on_response(data[offset:])
            if rv < 0: return rv
            offset += rv
        print("(arr) end")
        return offset
    elif rtype == SER_KV:
        total_len = struct.unpack('<I', data[1:5])[0]
        key_len = struct.unpack('<I', data[5:9])[0]
        key = data[9:9 + key_len].decode()
        val_offset = 9 + key_len
        val_len = struct.unpack('<I', data[val_offset:val_offset + 4])[0]
        value = data[val_offset + 4:val_offset + 4 + val_len].decode()
        print(f"(kv) key: {key}, value: {value}")
        return 5 + total_len
    else:
        msg("unknown response type")
        return -1

def read_res(sock):
    """ Reads and handles a full response from the server. """
    header = read_full(sock, 4)
    length = struct.unpack('<I', header)[0]
    if length > k_max_msg:
        raise ValueError("Response too long")
    body = read_full(sock, length)
    rv = on_response(body)
    if rv != length:
        msg("incomplete response")
        return -1
    return rv

# === Client Entry ===
def main():
    HOST = "your.server.ip.address"  # Replace with the actual server IP
    PORT = 8085

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((HOST, PORT))
        except Exception as e:
            die(f"Could not connect to server: {e}")

        # === Test Cases ===

        try:
            print("== SET key `age` to 12 ==")
            send_req(sock, ["set", "age", "12"])
            read_res(sock)

            print("== GET key `age` ==")
            send_req(sock, ["get", "age"])
            read_res(sock)
           
            print("== Set expire time (30s) for `age` ==")
            send_req(sock, ["pexpire", "age", "30000"])
            read_res(sock)
            
            print("== TTL for `age` ==")
            send_req(sock, ["pttl", "age"])
            read_res(sock)
            
            print("== Add scores to leaderboard ==")
            send_req(sock, ["zadd", "leaderboard", "100", "Alice"])
            read_res(sock)
            send_req(sock, ["zadd", "leaderboard", "200", "Bob"])
            read_res(sock)
            send_req(sock, ["zadd", "leaderboard", "150", "Charlie"])
            read_res(sock)

            print("== Query leaderboard ==")
            send_req(sock, ["zquery", "leaderboard", "100", "", "0", "5"])
            read_res(sock)

            print("== Remove Alice from leaderboard ==")
            send_req(sock, ["zrem", "leaderboard", "Alice"])
            read_res(sock)

            print("== Score of Bob ==")
            send_req(sock, ["zscore", "leaderboard", "Bob"])
            read_res(sock)
            
            print("== List all keys & Values ==")
            send_req(sock, ["keys"])
            read_res(sock)
            
        except Exception as e:
            die(f"Runtime error: {e}")

if __name__ == "__main__":
    main()
