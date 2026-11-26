#!/usr/bin/env python3
"""Measure chat latency (RTT) and throughput.

Usage: python measure.py <host> <port> <msg_size_bytes> [count]
Example: python measure.py 127.0.0.1 5000 50 100

1) Latency: send one message, wait until the server echoes it back
   (group messages are also delivered to the sender), repeat `count` times.
2) Throughput: send `count*10` messages back-to-back, time until all echoes return.
"""
import json
import socket
import statistics
import secrets
import sys
import time
import uuid

host = sys.argv[1]
port = int(sys.argv[2])
size = int(sys.argv[3])
count = int(sys.argv[4]) if len(sys.argv) > 4 else 100
size = min(size, 900)                       # server limit is 1000 chars of text
name = f"meas{uuid.uuid4().hex[:12]}"

sock = socket.create_connection((host, port))
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
buf = b""


def next_msg():
    """Read one framed JSON message (uses the same newline framing as the app)."""
    global buf
    while b"\n" not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            raise ConnectionError("server closed")
        buf += chunk
    line, _, buf = buf.partition(b"\n")
    return json.loads(line)


def send(obj):
    sock.sendall((json.dumps(obj) + "\n").encode())


password = secrets.token_urlsafe(16)
send({"type": "register", "username": name, "password": password})
while next_msg()["type"] != "registered":
    pass
send({"type": "login", "username": name, "password": password})
while next_msg()["type"] != "login_ok":
    pass
del password

pad = "x" * max(0, size - 8)

# ---------- 1) latency ----------
rtts = []
for i in range(count):
    text = f"{i:06d}|{pad}"[:size]
    t0 = time.perf_counter()
    send({"type": "msg", "text": text})
    while True:
        m = next_msg()
        if m.get("type") == "msg" and m.get("from") == name and m["text"] == text:
            break
    rtts.append((time.perf_counter() - t0) * 1000)

print(f"--- Latency ({count} msgs, {size} bytes) ---")
print(f"RTT avg {statistics.mean(rtts):.3f} ms | min {min(rtts):.3f} | "
      f"max {max(rtts):.3f} | stdev {statistics.pstdev(rtts):.3f}")

# ---------- 2) throughput ----------
n = count * 10
t0 = time.perf_counter()
for i in range(n):
    send({"type": "msg", "text": f"{i:06d}|{pad}"[:size]})
received = 0
while received < n:
    m = next_msg()
    if m.get("type") == "msg" and m.get("from") == name:
        received += 1
dt = time.perf_counter() - t0
total_bytes = n * size
print(f"--- Throughput ({n} msgs, {size} bytes each) ---")
print(f"{n} msgs in {dt:.3f} s = {n/dt:.0f} msgs/s, {total_bytes*8/dt/1e6:.2f} Mbit/s (payload)")
send({"type": "quit"})
sock.shutdown(socket.SHUT_WR)