"""Private messaging, offline delivery, history, and conversation tests."""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time


PASSWORD = "message-test-password"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=4)
        self.sock.settimeout(4)
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
            data = self.sock.recv(4096)
            if not data:
                raise ConnectionError("server closed the connection")
            self.buffer += data

    def register_and_login(self, username):
        self.send({"type": "register", "username": username, "password": PASSWORD})
        assert self.wait(lambda m: m.get("type") == "registered")
        self.send({"type": "login", "username": username, "password": PASSWORD})
        assert self.wait(lambda m: m.get("type") == "login_ok")

    def login(self, username):
        self.send({"type": "login", "username": username, "password": PASSWORD})
        assert self.wait(lambda m: m.get("type") == "login_ok")

    def assert_no_type(self, message_type, timeout=0.25):
        deadline = time.time() + timeout
        try:
            while True:
                while b"\n" in self.buffer:
                    line, _, self.buffer = self.buffer.partition(b"\n")
                    message = json.loads(line.decode("utf-8"))
                    assert message.get("type") != message_type, message
                remaining = deadline - time.time()
                if remaining <= 0:
                    return
                self.sock.settimeout(remaining)
                try:
                    self.buffer += self.sock.recv(4096)
                except socket.timeout:
                    return
        finally:
            self.sock.settimeout(4)

    def close(self):
        self.sock.close()


temp_dir = tempfile.TemporaryDirectory()
port = free_port()
env = dict(
    os.environ,
    CHAT_PORT=str(port),
    WEB_PORT="0",
    CHAT_DB_PATH=os.path.join(temp_dir.name, "messages.sqlite3"),
)
server = subprocess.Popen(
    [sys.executable, "server.py"],
    env=env,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
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
    alice = connect()
    alice.register_and_login("alice")
    bob = connect()
    bob.register_and_login("bob")

    bob.send({"type": "logout"})
    assert bob.wait(lambda m: m.get("type") == "logged_out")
    alice.send({"type": "dm", "to": "bob", "text": "Stored while you were away"})
    sent = alice.wait(lambda m: m.get("type") == "dm_sent")
    assert sent["status"] == "pending"
    print("PASS private message to offline recipient is stored")

    alice.send({"type": "history", "with": "bob"})
    history = alice.wait(lambda m: m.get("type") == "history_result")
    assert len(history["messages"]) == 1
    assert history["messages"][0]["text"] == "Stored while you were away"
    assert history["messages"][0]["from"] == "alice"

    alice.send({"type": "conversations"})
    conversations = alice.wait(lambda m: m.get("type") == "conversations_result")
    assert len(conversations["conversations"]) == 1
    assert conversations["conversations"][0]["with"] == "bob"
    print("PASS history and conversation list return stored DM")

    bob.login("bob")
    delivered = bob.wait(lambda m: m.get("type") == "dm")
    assert delivered["text"] == "Stored while you were away"
    assert delivered["from"] == "alice" and delivered["status"] == "delivered"
    status_update = alice.wait(lambda m: m.get("type") == "delivery_status")
    assert status_update["message_id"] == sent["message_id"]
    bob.send({"type": "history", "with": "alice"})
    bob_history = bob.wait(lambda m: m.get("type") == "history_result")
    assert bob_history["messages"][0]["status"] == "delivered"
    print("PASS pending message is delivered at next login")

    stranger = connect()
    stranger.register_and_login("carol")
    bob.send({"type": "dm", "to": "alice", "text": "You are back"})
    reply = alice.wait(lambda m: m.get("type") == "dm")
    ack = bob.wait(lambda m: m.get("type") == "dm_sent")
    assert reply["text"] == "You are back" and ack["status"] == "delivered"
    stranger.assert_no_type("dm")
    print("PASS online private message reaches only its recipient")

    stranger.send({"type": "history", "with": "bob"})
    stranger_history = stranger.wait(lambda m: m.get("type") == "history_result")
    assert stranger_history["messages"] == []
    assert "conversation_id" not in stranger_history or stranger_history["conversation_id"] is None
    print("PASS unrelated user cannot read another pair's history")
    print("ALL MESSAGING TESTS PASSED")
finally:
    for client in clients:
        client.close()
    server.terminate()
    server.wait(timeout=5)
    temp_dir.cleanup()
