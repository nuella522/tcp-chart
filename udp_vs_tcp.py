#!/usr/bin/env python3
"""TCP vs UDP comparison on localhost. No downloads or admin rights needed.

Idea: a fast sender sends N numbered messages to a SLOW receiver that has a
tiny receive buffer. The operating system has to drop what doesn't fit.
  - UDP: extra datagrams are silently dropped; the app is never told.
  - TCP: flow control makes the sender wait, so nothing is lost.

Usage: python udp_vs_tcp.py [N] [slow_ms]
       defaults: N = 5000, slow_ms = 0.3   (receiver delay per message, in ms)
       Example: python udp_vs_tcp.py 3000 2
"""
import socket
import sys
import threading
import time

N = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
PAYLOAD = "x" * 150
SLOW = (float(sys.argv[2]) if len(sys.argv) > 2 else 0.3) / 1000   # receiver delay per message
HOST = "127.0.0.1"


def work():
    """Pretend to process a message. Busy-wait: time.sleep() is too coarse on Windows."""
    end = time.perf_counter() + SLOW
    while time.perf_counter() < end:
        pass


def udp_test():
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)   # tiny buffer
    rx.bind((HOST, 0))
    port = rx.getsockname()[1]
    rx.settimeout(1.5)
    got = []

    def receiver():
        try:
            while True:
                data, _ = rx.recvfrom(2048)
                got.append(int(data.split(b"|", 1)[0]))
                work()
        except socket.timeout:
            pass

    t = threading.Thread(target=receiver)
    t.start()
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    t0 = time.perf_counter()
    for i in range(N):
        tx.sendto(f"{i}|{PAYLOAD}".encode(), (HOST, port))
    t.join()
    dt = time.perf_counter() - t0 - 1.5
    out_of_order = sum(1 for a, b in zip(got, got[1:]) if b < a)
    return len(got), out_of_order, dt


def tcp_test():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind((HOST, 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    got = []

    def receiver():
        conn, _ = srv.accept()
        buf = b""
        while True:
            chunk = conn.recv(512)
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:                       # newline framing
                line, _, buf = buf.partition(b"\n")
                got.append(int(line.split(b"|", 1)[0]))
                work()
        conn.close()

    t = threading.Thread(target=receiver)
    t.start()
    tx = socket.create_connection((HOST, port))
    t0 = time.perf_counter()
    for i in range(N):
        tx.sendall(f"{i}|{PAYLOAD}\n".encode())
    tx.shutdown(socket.SHUT_WR)
    t.join()
    dt = time.perf_counter() - t0
    out_of_order = sum(1 for a, b in zip(got, got[1:]) if b < a)
    return len(got), out_of_order, dt


print(f"Sending {N} numbered messages to a slow receiver (tiny buffer)...\n")
r, o, dt = udp_test()
print(f"UDP: received {r}/{N}  lost {N - r} ({(N - r) / N * 100:.1f}%)  "
      f"out-of-order {o}  sender was never told")
r, o, dt = tcp_test()
print(f"TCP: received {r}/{N}  lost {N - r} ({(N - r) / N * 100:.1f}%)  "
      f"out-of-order {o}  took {dt:.2f} s (sender was slowed down by flow control)")