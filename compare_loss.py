#!/usr/bin/env python3
"""TCP vs UDP comparison: 100 numbered messages each, paced, on localhost.

Run it twice and keep both outputs:
  1) baseline (no packet loss injected)
  2) with Clumsy dropping packets on UDP port 6001 and TCP port 6002

Usage: python compare_loss.py [N] [interval_ms]     (defaults: 100, 5)

Records: messages received, order, latency, and recovery (late messages).
Latency is measured inside one process, so one clock is used for send and receive.
"""
import socket
import sys
import threading
import time

N = int(sys.argv[1]) if len(sys.argv) > 1 else 100
INTERVAL = (float(sys.argv[2]) if len(sys.argv) > 2 else 5) / 1000
HOST = "127.0.0.1"
UDP_PORT = 6001
TCP_PORT = 6002
PAYLOAD = "x" * 100
LATE_MS = 50          # a message slower than this counts as "delayed" (recovery)


def report(name, arrivals, sent_at):
    """arrivals: list of (seq, recv_time) in arrival order."""
    got = [s for s, _ in arrivals]
    unique = sorted(set(got))
    lost = N - len(unique)
    out_of_order = sum(1 for a, b in zip(got, got[1:]) if b < a)
    dupes = len(got) - len(unique)
    lat = [(t - sent_at[s]) * 1000 for s, t in arrivals]
    print(f"{name}")
    print(f"  received      : {len(unique)}/{N}   lost: {lost} ({lost / N * 100:.1f}%)")
    print(f"  out of order  : {out_of_order}   duplicates: {dupes}")
    if lat:
        late = sum(1 for x in lat if x > LATE_MS)
        print(f"  latency (ms)  : avg {sum(lat) / len(lat):.2f}  max {max(lat):.2f}")
        print(f"  delayed >{LATE_MS} ms: {late} message(s)  (recovered by retransmission)" if name.startswith("TCP")
              else f"  delayed >{LATE_MS} ms: {late} message(s)")


def udp_test():
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx.bind((HOST, UDP_PORT))
    rx.settimeout(2.0)
    sent_at, arrivals = {}, []

    def receiver():
        try:
            while True:
                data, _ = rx.recvfrom(2048)
                arrivals.append((int(data.split(b"|", 1)[0]), time.perf_counter()))
        except socket.timeout:
            pass

    t = threading.Thread(target=receiver)
    t.start()
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for i in range(N):
        sent_at[i] = time.perf_counter()
        tx.sendto(f"{i}|{PAYLOAD}".encode(), (HOST, UDP_PORT))
        time.sleep(INTERVAL)
    t.join()
    rx.close()
    tx.close()
    report("UDP", arrivals, sent_at)


def tcp_test():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, TCP_PORT))
    srv.listen(1)
    sent_at, arrivals = {}, []

    def receiver():
        conn, _ = srv.accept()
        conn.settimeout(30)
        buf = b""
        try:
            while len(arrivals) < N:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, _, buf = buf.partition(b"\n")
                    arrivals.append((int(line.split(b"|", 1)[0]), time.perf_counter()))
        except socket.timeout:
            pass
        conn.close()

    t = threading.Thread(target=receiver)
    t.start()
    tx = socket.create_connection((HOST, TCP_PORT))
    tx.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    for i in range(N):
        sent_at[i] = time.perf_counter()
        tx.sendall(f"{i}|{PAYLOAD}\n".encode())
        time.sleep(INTERVAL)
    t.join()
    tx.close()
    srv.close()
    report("TCP", arrivals, sent_at)


print(f"Sending {N} numbered messages per protocol, one every {INTERVAL * 1000:.0f} ms\n")
udp_test()
print()
tcp_test()
