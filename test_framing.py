"""Automated tests: split/combined messages, malformed input, 3 clients, abrupt close + rejoin.
Run: python test_framing.py   (starts its own server on port 5055)
"""
import json, os, socket, struct, subprocess, sys, tempfile, time

PORT = 5055
temp_dir = tempfile.TemporaryDirectory()
env = dict(os.environ, CHAT_PORT=str(PORT), CHAT_DB_PATH=os.path.join(temp_dir.name, "chat.sqlite3"))
srv = subprocess.Popen([sys.executable, "server.py"], env=env,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(1)


class C:
    def __init__(self, name, register=True):
        self.s = socket.create_connection(("127.0.0.1", PORT)); self.s.settimeout(3)
        self.buf = b""
        self.name = name
        if register:
            self.send({"type": "register", "username": name, "password": "test-password-123"})
            assert self.wait(lambda m: m["type"] == "registered")
        self.login()

    def send(self, obj):
        self.s.sendall(json.dumps(obj).encode() + b"\n")

    def login(self):
        self.send({"type": "login", "username": self.name, "password": "test-password-123"})
        assert self.wait(lambda m: m["type"] == "login_ok")

    def wait(self, pred):
        """Read framed messages until one satisfies pred."""
        while True:
            while b"\n" in self.buf:
                line, _, self.buf = self.buf.partition(b"\n")
                m = json.loads(line)
                if pred(m): return m
            self.buf += self.s.recv(4096)

try:
    a, b, c = C("alice"), C("bob"), C("carol")
    print("PASS 3 authenticated clients connected")

    payload = json.dumps({"type": "msg", "from": "mallory", "text": "split-hello"}).encode() + b"\n"
    for i in range(0, len(payload), 5):
        a.s.sendall(payload[i:i+5]); time.sleep(0.05)
    delivered = b.wait(lambda m: m["type"] == "msg")
    assert delivered["text"] == "split-hello" and delivered["from"] == "alice"
    print("PASS split message reassembled and sender identity is server-controlled")

    a.s.sendall(b"".join(json.dumps({"type": "msg", "text": t}).encode() + b"\n" for t in ("one", "two")))
    assert c.wait(lambda m: m.get("text") == "one") and c.wait(lambda m: m.get("text") == "two")
    print("PASS combined messages separated")

    a.s.sendall(b"this is not json\n")
    a.wait(lambda m: m["type"] == "error")
    a.send({"type": "msg", "text": "still alive"})
    assert b.wait(lambda m: m.get("text") == "still alive")
    print("PASS malformed input handled, server still running")

    oversized = C("dave")
    oversized.s.sendall(json.dumps({"type": "msg", "text": "x" * 5000}).encode() + b"\n")
    assert oversized.wait(lambda m: m["type"] == "error")["reason"] == "message too long"
    oversized.s.close()
    print("PASS oversized framed message rejected")

    a.send({"type": "register", "username": "alice", "password": "test-password-123"})
    assert a.wait(lambda m: m["type"] == "error")
    print("PASS duplicate account rejected")

    b.s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    b.s.close()   # abrupt close -> RST
    c.wait(lambda m: m.get("text") == "bob left")
    b2 = C("bob", register=False)
    print("PASS abrupt disconnect cleaned up, bob logged in again")
    print("ALL TESTS PASSED")
finally:
    srv.terminate()
    srv.wait(timeout=5)
    temp_dir.cleanup()
