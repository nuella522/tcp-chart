"""Account/authentication integration checks. Run: python test_accounts.py"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=3)
        self.sock.settimeout(3)
        self.buffer = b""

    def send(self, message):
        self.sock.sendall(json.dumps(message).encode("utf-8") + b"\n")

    def wait(self, predicate):
        while True:
            while b"\n" in self.buffer:
                line, _, self.buffer = self.buffer.partition(b"\n")
                message = json.loads(line.decode("utf-8"))
                if predicate(message):
                    return message
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("server closed the connection")
            self.buffer += chunk

    def register(self, username, password="correct-password-123"):
        self.send({"type": "register", "username": username, "password": password})
        return self.wait(lambda m: m.get("type") in ("registered", "error"))

    def login(self, username, password):
        self.send({"type": "login", "username": username, "password": password})
        return self.wait(lambda m: m.get("type") in ("login_ok", "error"))

    def close(self):
        self.sock.close()


temp_dir = tempfile.TemporaryDirectory()
port = free_port()
env = dict(os.environ, CHAT_PORT=str(port), WEB_PORT="0",
           CHAT_DB_PATH=os.path.join(temp_dir.name, "accounts.sqlite3"))
server = subprocess.Popen([sys.executable, "server.py"], env=env,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
clients = []


def connect():
    deadline = time.time() + 8
    while True:
        try:
            client = Client(port)
            clients.append(client)
            return client
        except OSError:
            if time.time() >= deadline:
                raise
            time.sleep(0.1)


try:
    anonymous = connect()
    anonymous.send({"type": "msg", "text": "not authenticated"})
    assert anonymous.wait(lambda m: m.get("type") == "error")["reason"] == "authentication required"
    print("PASS sending without login is rejected")

    account = connect()
    assert account.register("Alice")["type"] == "registered"
    duplicate = connect()
    assert duplicate.register("alice")["reason"] == "username already exists"
    print("PASS case-insensitive duplicate registration is rejected")

    wrong = connect()
    response = wrong.login("Alice", "incorrect-password")
    assert response["reason"] == "invalid username or password"
    assert wrong.login("Alice", "correct-password-123")["type"] == "login_ok"
    print("PASS wrong password fails and correct password succeeds")

    limited = connect()
    assert limited.register("ratelimit")["type"] == "registered"
    for _ in range(5):
        result = limited.login("ratelimit", "incorrect-password")
        assert result["reason"] == "invalid username or password"
    result = limited.login("ratelimit", "correct-password-123")
    assert "too many failed logins" in result["reason"]
    print("PASS repeated failed logins trigger temporary rate limit")
    print("ALL ACCOUNT TESTS PASSED")
finally:
    for client in clients:
        client.close()
    server.terminate()
    server.wait(timeout=5)
    temp_dir.cleanup()
