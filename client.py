#!/usr/bin/env python3
"""Terminal chat client with a guided registration and login menu."""
import getpass
import json
import queue
import socket
import sys
import threading

host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
port = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
auth_responses = queue.Queue()
auth_waiting = threading.Event()


def send(sock, obj):
    sock.sendall((json.dumps(obj) + "\n").encode("utf-8"))


def receiver(sock):
    buf = b""
    try:
        while True:
            chunk = sock.recv(1024)
            if not chunk:
                print("\n[server closed the connection]")
                break
            buf += chunk
            while b"\n" in buf:
                line, _, buf = buf.partition(b"\n")
                try:
                    m = json.loads(line.decode("utf-8"))
                except ValueError:
                    continue
                t = m.get("type")
                if auth_waiting.is_set() and (
                    t in ("registered", "login_ok", "logged_out") or t == "error"
                ):
                    auth_responses.put(m)
                elif t == "msg":
                    print(f"[{m['from']}] {m['text']}")
                elif t == "dm":
                    print(f"[DM {m['from']} -> {m['to']}] {m['text']}")
                elif t == "dm_sent":
                    print(f"[You -> {m['to']}] {m['text']} ({m['status']})")
                elif t == "history_result":
                    print(f"* Conversation with {m['with']}:")
                    for item in m["messages"]:
                        print(f"  [{item['from']}] {item['text']}")
                elif t == "conversations_result":
                    print("* Your conversations:")
                    for item in m["conversations"]:
                        preview = item["last_message"] or "(no messages)"
                        print(f"  {item['with']}: {preview}")
                elif t == "system":
                    print(f"* {m['text']}")
                elif t == "login_ok":
                    print(f"* Logged in as {m['username']}")
                elif t == "logged_out":
                    print("* Logged out")
                elif t == "users":
                    print(f"* Online: {', '.join(m['users'])}")
                elif t == "error":
                    print(f"! {m['reason']}")
    except OSError:
        print("\n[connection lost]")
    finally:
        try:
            sock.close()
        except OSError:
            pass


def auth_request(sock, message):
    auth_waiting.set()
    try:
        send(sock, message)
        return auth_responses.get(timeout=15)
    finally:
        auth_waiting.clear()


def chat_loop(sock, username):
    print(f"\nSigned in as {username}. Use /dm <user> <text> for a private message.")
    print("Commands: /dm, /history <user>, /chats, /users, /logout, /quit, /help")
    while True:
        line = input("chat> ").strip()
        if not line:
            continue
        if line == "/quit":
            send(sock, {"type": "quit"})
            return False
        if line == "/logout":
            response = auth_request(sock, {"type": "logout"})
            if response["type"] == "logged_out":
                print("You are signed out.")
                return True
            print(f"Sign out failed: {response.get('reason', 'unexpected response')}")
            continue
        if line == "/users":
            send(sock, {"type": "users"})
        elif line == "/chats":
            send(sock, {"type": "conversations"})
        elif line.startswith("/dm "):
            parts = line.split(" ", 2)
            if len(parts) != 3 or not parts[2].strip():
                print("Usage: /dm <username> <message>")
                continue
            send(sock, {"type": "dm", "to": parts[1], "text": parts[2]})
        elif line.startswith("/history "):
            parts = line.split()
            if len(parts) != 2:
                print("Usage: /history <username>")
                continue
            send(sock, {"type": "history", "with": parts[1]})
        elif line == "/help":
            print("Plain text is public. Use /dm <user> <text> for private chat.")
            print("Commands: /history <user>, /chats, /users, /logout, /quit, /help")
        else:
            send(sock, {"type": "msg", "text": line})


def main():
    sock = socket.create_connection((host, port), timeout=10)
    sock.settimeout(None)
    print(f"Connected to chat server at {sock.getpeername()[0]}:{sock.getpeername()[1]}")
    rt = threading.Thread(target=receiver, args=(sock,), daemon=True)
    rt.start()
    try:
        while True:
            print("\n=== TCP Chat ===")
            print("1) Create account")
            print("2) Log in")
            print("3) Exit")
            choice = input("Choose 1, 2, or 3: ").strip()
            if choice == "3":
                send(sock, {"type": "quit"})
                break
            if choice not in ("1", "2"):
                print("Please enter 1, 2, or 3.")
                continue

            username = input("Username (3-16 letters, numbers, or _): ").strip()
            password_prompt = "Create password (10-128 characters): " if choice == "1" else "Password: "
            password = getpass.getpass(password_prompt)
            request_type = "register" if choice == "1" else "login"
            try:
                response = auth_request(
                    sock,
                    {"type": request_type, "username": username, "password": password},
                )
            except queue.Empty:
                print("No response from server. Check the connection and try again.")
                break
            finally:
                del password

            if response["type"] == "error":
                print(f"Could not {('create account' if choice == '1' else 'log in')}: {response['reason']}")
                continue
            if choice == "1":
                print(f"Account created for {response['username']}. Choose option 2 to log in.")
                continue

            if not chat_loop(sock, response["username"]):
                break
    except (EOFError, KeyboardInterrupt, OSError):
        pass
    finally:
        sock.close()


if __name__ == "__main__":
    main()