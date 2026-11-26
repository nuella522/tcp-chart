"""Tests the browser (WebSocket) side and its bridge to raw TCP clients.
Run: python test_websocket.py   (starts its own server on TCP 5091 / web 8091)
"""
import base64, json, os, socket, struct, subprocess, sys, tempfile, time, urllib.request

TCP_PORT, WEB_PORT = 5091, 8091
temp_dir = tempfile.TemporaryDirectory()
env = dict(os.environ, CHAT_PORT=str(TCP_PORT), WEB_PORT=str(WEB_PORT),
           CHAT_DB_PATH=os.path.join(temp_dir.name, "chat.sqlite3"))
srv = subprocess.Popen([sys.executable, "server.py"], env=env,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(1)


class TcpClient:
    def __init__(self, name):
        self.s = socket.create_connection(("127.0.0.1", TCP_PORT)); self.s.settimeout(3); self.buf = b""
        self.send({"type": "register", "username": name, "password": "test-password-123"})
        assert self.wait(lambda m: m["type"] == "registered")
        self.send({"type": "login", "username": name, "password": "test-password-123"})
        assert self.wait(lambda m: m["type"] == "login_ok")
    def send(self, obj): self.s.sendall(json.dumps(obj).encode() + b"\n")
    def wait(self, pred):
        while True:
            while b"\n" in self.buf:
                line, _, self.buf = self.buf.partition(b"\n")
                m = json.loads(line)
                if pred(m): return m
            self.buf += self.s.recv(4096)


class WsClient:
    """Tiny WebSocket client (what a browser does for us)."""
    def __init__(self, origin=None):
        self.s = socket.create_connection(("127.0.0.1", WEB_PORT)); self.s.settimeout(3)
        key = base64.b64encode(os.urandom(16)).decode()
        req = (f"GET /ws HTTP/1.1\r\nHost: 127.0.0.1:{WEB_PORT}\r\nUpgrade: websocket\r\n"
               f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n")
        if origin: req += f"Origin: {origin}\r\n"
        self.s.sendall((req + "\r\n").encode())
        self.head = b""
        while b"\r\n\r\n" not in self.head:
            self.head += self.s.recv(1024)
        self.key = key
    def send(self, obj, masked=True):
        payload = json.dumps(obj).encode()
        n = len(payload)
        head = bytes([0x81, (0x80 if masked else 0) | (n if n < 126 else 126)]) + (struct.pack("!H", n) if n >= 126 else b"")
        if masked:
            mask = os.urandom(4)
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            self.s.sendall(head + mask + payload)
        else:
            self.s.sendall(head + payload)
    def recv_msg(self):
        b1, b2 = self.exact(2); n = b2 & 0x7F
        if n == 126: n = struct.unpack("!H", self.exact(2))[0]
        return json.loads(self.exact(n))
    def exact(self, n):
        out = b""
        while len(out) < n:
            c = self.s.recv(n - len(out))
            if not c: raise ConnectionError
            out += c
        return out
    def wait(self, pred):
        while True:
            m = self.recv_msg()
            if pred(m): return m


try:
    page = urllib.request.urlopen(f"http://127.0.0.1:{WEB_PORT}/").read().decode()
    assert "TCP Chat" in page and "WebSocket" in page and "Log in" in page
    assert all(marker in page for marker in (
        'id="auth-screen"', 'id="app-screen"', 'id="online-list"',
        'id="composer"', 'id="header-logout"', 'max-width:760px',
    ))
    print("PASS web page served over HTTP")
    assert urllib.request.urlopen(f"http://127.0.0.1:{WEB_PORT}/health").read() == b"ok"
    print("PASS health check")

    w = WsClient()
    assert b"101 Switching Protocols" in w.head
    expected = base64.b64encode(__import__("hashlib").sha1((w.key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest())
    assert expected in w.head
    print("PASS WebSocket handshake (101, correct Sec-WebSocket-Accept)")

    w.send({"type": "register", "username": "webuser", "password": "test-password-123"})
    assert w.wait(lambda m: m["type"] == "registered")["username"] == "webuser"
    w.send({"type": "login", "username": "webuser", "password": "test-password-123"})
    assert w.wait(lambda m: m["type"] == "login_ok")["username"] == "webuser"
    t = TcpClient("tcpuser")
    w.wait(lambda m: m.get("text") == "tcpuser joined")
    print("PASS browser and TCP clients share one room")

    w.send({"type": "msg", "text": "hello from the browser"})
    assert t.wait(lambda m: m["type"] == "msg")["text"] == "hello from the browser"
    t.send({"type": "msg", "text": "hello from raw TCP"})
    assert w.wait(lambda m: m["type"] == "msg" and m["from"] == "tcpuser")["text"] == "hello from raw TCP"
    print("PASS messages flow both ways between WebSocket and TCP")

    w.send({"type": "msg", "text": "x" * 300})          # frame longer than 125 bytes (extended length)
    assert len(t.wait(lambda m: m["type"] == "msg" and m["from"] == "webuser")["text"]) == 300
    print("PASS long WebSocket frame (extended length field)")

    bad = WsClient(origin="http://evil.example")
    assert b"403" in bad.head
    print("PASS foreign Origin rejected")

    u = WsClient(); u.send({"type": "msg", "text": "no join"}, masked=False)
    reply = u.recv_msg()
    assert reply["type"] == "error" and "masked" in reply["reason"], reply
    print("PASS unmasked client frame refused with an error")

    w.s.close()
    t.wait(lambda m: m.get("text") == "webuser left")
    print("PASS browser disconnect cleaned up")
    print("ALL TESTS PASSED")
finally:
    srv.terminate()
    srv.wait(timeout=5)
    temp_dir.cleanup()
