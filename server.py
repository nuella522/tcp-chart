#!/usr/bin/env python3
"""Multi-user TCP messenger.

Two front doors, one message service:
  - raw TCP clients (client.py) on CHAT_PORT
  - browser clients over WebSocket (HTTP upgrade, still TCP underneath) on WEB_PORT

Framing: newline-delimited JSON (one JSON object per line, UTF-8).
Concurrency: one thread per client; shared state guarded by a lock.
Config via environment variables: CHAT_HOST (default 0.0.0.0), CHAT_PORT (default 5000).
"""
import base64
import hashlib
import json
import logging
import os
import re
import socket
import struct
import threading
import time

from auth_store import AuthStore, DuplicateUsername, InvalidCredentials, LoginRateLimited
from message_store import ConversationNotFound, MessageStore, UnknownUser

HOST = os.environ.get("CHAT_HOST", "0.0.0.0")
PORT = int(os.environ.get("CHAT_PORT", "5000"))
WEB_PORT = int(os.environ.get("WEB_PORT", "8080"))   # browser page + WebSocket (0 disables)
DB_PATH = os.environ.get("CHAT_DB_PATH", "chat.sqlite3")
SESSION_TTL = int(os.environ.get("SESSION_TTL_SECONDS", "86400"))
MAX_LINE = 4096          # max bytes per framed message (protects against memory abuse)
HEARTBEAT = 25           # seconds between keep-alive pings sent to every client
MAX_TEXT = 1000          # max chat text length
USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,16}$")
PASSWORD_MIN = 10
PASSWORD_MAX = 128

if SESSION_TTL <= 0:
    raise ValueError("SESSION_TTL_SECONDS must be greater than zero")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("chat")

accounts = AuthStore(DB_PATH)
message_store = MessageStore(DB_PATH)
clients = {}                 # normalized username -> set of authenticated sockets
client_expiries = {}          # socket -> session expiry timestamp
clients_lock = threading.Lock()


def send(sock, obj):
    """Frame and send one message. Returns False if the socket is broken."""
    try:
        sock.sendall((json.dumps(obj) + "\n").encode("utf-8"))
        return True
    except OSError:
        return False


def prune_expired_clients():
    with clients_lock:
        targets = [(username, sock, client_expiries.get(sock, 0))
                   for username, sockets in clients.items() for sock in sockets]
    now = time.time()
    for username, target, expires_at in targets:
        if expires_at <= now:
            untrack_client(username, target)
            send(target, {"type": "error", "reason": "session expired; log in again"})


def broadcast(obj, exclude=None):
    prune_expired_clients()
    with clients_lock:
        targets = [sock for username, sockets in clients.items()
                   if username != exclude for sock in sockets]
    for target in targets:
        send(target, obj)


def track_client(username_key, sock, expires_at):
    with clients_lock:
        clients.setdefault(username_key, set()).add(sock)
        client_expiries[sock] = expires_at


def untrack_client(username_key, sock):
    with clients_lock:
        client_expiries.pop(sock, None)
        if username_key is not None:
            sockets = clients.get(username_key)
            if sockets:
                sockets.discard(sock)
                if not sockets:
                    clients.pop(username_key, None)


def deliver_pending(username_key, sock):
    for item in message_store.pending_for(username_key):
        event = {
            "type": "dm",
            "message_id": item["message_id"],
            "conversation_id": item["conversation_id"],
            "from": item["from"],
            "to": item["to"],
            "text": item["text"],
            "ts": item["ts"],
            "status": "delivered",
        }
        if send(sock, event):
            message_store.mark_delivered(item["message_id"], username_key)
        else:
            break


def valid_username(value):
    return isinstance(value, str) and USERNAME_RE.fullmatch(value) is not None


def valid_password(value):
    return isinstance(value, str) and PASSWORD_MIN <= len(value) <= PASSWORD_MAX


def valid_text(value):
    return isinstance(value, str) and bool(value.strip()) and len(value) <= MAX_TEXT


def read_messages(sock, buf):
    """Generator: yields complete lines, buffering partial data across recv() calls.

    Handles both split messages (one message over many reads) and
    combined messages (many messages in one read).
    """
    while True:
        while b"\n" in buf:
            line, _, buf = buf.partition(b"\n")
            if len(line) > MAX_LINE:
                raise ValueError("message too long")
            yield line
        if len(buf) > MAX_LINE:
            raise ValueError("message too long")
        chunk = sock.recv(1024)
        if not chunk:            # b"" => peer closed (FIN received)
            return
        buf += chunk


def handle_client(sock, addr):
    username = None
    username_key = None
    session_hash = None
    log.info("CONNECT %s:%s", *addr)
    sock.settimeout(300)   # drop idle/dead clients after 5 minutes
    try:
        for raw in read_messages(sock, b""):
            try:
                msg = json.loads(raw.decode("utf-8"))
                if not isinstance(msg, dict):
                    raise ValueError("not an object")
                mtype = msg.get("type")
            except (ValueError, UnicodeDecodeError):
                log.warning("MALFORMED from %s:%s", *addr)
                send(sock, {"type": "error", "reason": "malformed message"})
                continue

            if mtype == "register":
                name = msg.get("username")
                password = msg.get("password")
                if not valid_username(name):
                    send(sock, {"type": "error",
                                "reason": "username must be 3-16 letters, digits, or _"})
                    continue
                if not valid_password(password):
                    send(sock, {"type": "error",
                                "reason": f"password must be {PASSWORD_MIN}-{PASSWORD_MAX} characters"})
                    continue
                key = name.casefold()
                try:
                    accounts.register(name, key, password)
                except DuplicateUsername:
                    send(sock, {"type": "error", "reason": "username already exists"})
                    continue
                send(sock, {"type": "registered", "username": name})
                log.info("REGISTER username=%s from %s:%s", key, *addr)
                continue

            if mtype == "login":
                name = msg.get("username")
                password = msg.get("password")
                if not valid_username(name) or not valid_password(password):
                    send(sock, {"type": "error", "reason": "invalid username or password format"})
                    continue
                if session_hash is not None:
                    if accounts.resolve_session(session_hash) is not None:
                        send(sock, {"type": "error", "reason": "log out before changing accounts"})
                        continue
                    untrack_client(username_key, sock)
                    username = username_key = session_hash = None
                try:
                    username, username_key, token, session_hash, expires_at = accounts.login(
                        name.casefold(), password, str(addr[0]), SESSION_TTL)
                except LoginRateLimited:
                    log.warning("LOGIN_RATE_LIMIT username=%s from %s:%s", name.casefold(), *addr)
                    send(sock, {"type": "error", "reason": "too many failed logins; try again in 5 minutes"})
                    continue
                except InvalidCredentials:
                    log.warning("LOGIN_FAILURE username=%s from %s:%s", name.casefold(), *addr)
                    send(sock, {"type": "error", "reason": "invalid username or password"})
                    continue
                track_client(username_key, sock, expires_at)
                send(sock, {"type": "login_ok", "username": username,
                            "token": token, "expires_at": int(expires_at)})
                deliver_pending(username_key, sock)
                broadcast({"type": "system", "text": f"{username} joined"}, exclude=username_key)
                log.info("LOGIN_SUCCESS username=%s from %s:%s", username_key, *addr)
                continue

            if mtype == "logout":
                if session_hash is None:
                    send(sock, {"type": "error", "reason": "authentication required"})
                    continue
                if accounts.resolve_session(session_hash) is None:
                    untrack_client(username_key, sock)
                    username = username_key = session_hash = None
                    send(sock, {"type": "error", "reason": "session expired; log in again"})
                    continue
                accounts.revoke_session(session_hash)
                untrack_client(username_key, sock)
                broadcast({"type": "system", "text": f"{username} left"}, exclude=username_key)
                log.info("LOGOUT username=%s from %s:%s", username_key, *addr)
                username = username_key = session_hash = None
                send(sock, {"type": "logged_out"})
                continue

            if mtype == "quit":
                break

            if session_hash is None:
                send(sock, {"type": "error", "reason": "authentication required"})
                continue

            identity = accounts.resolve_session(session_hash)
            if identity is None:
                untrack_client(username_key, sock)
                username = username_key = session_hash = None
                send(sock, {"type": "error", "reason": "session expired; log in again"})
                continue
            username, username_key = identity

            if mtype == "msg":
                text = msg.get("text")
                if not valid_text(text):
                    send(sock, {"type": "error", "reason": "invalid text"})
                    continue
                log.info("MSG username=%s chars=%d", username_key, len(text))
                broadcast({"type": "msg", "from": username, "text": text, "ts": time.time()})
            elif mtype == "dm":
                recipient = msg.get("to")
                text = msg.get("text")
                if not valid_username(recipient):
                    send(sock, {"type": "error", "reason": "invalid recipient username"})
                    continue
                if not valid_text(text):
                    send(sock, {"type": "error", "reason": "invalid text"})
                    continue
                try:
                    saved = message_store.send_direct(username_key, recipient.casefold(), text)
                except UnknownUser:
                    send(sock, {"type": "error", "reason": "recipient not found"})
                    continue
                except ValueError as exc:
                    send(sock, {"type": "error", "reason": str(exc)})
                    continue

                prune_expired_clients()
                with clients_lock:
                    targets = list(clients.get(saved["to_key"], ()))
                delivered = False
                event = {
                    "type": "dm",
                    "message_id": saved["message_id"],
                    "conversation_id": saved["conversation_id"],
                    "from": saved["from"],
                    "to": saved["to"],
                    "text": saved["text"],
                    "ts": saved["ts"],
                    "status": "delivered",
                }
                for target in targets:
                    if send(target, event):
                        delivered = True
                status = "delivered" if delivered else "pending"
                if delivered:
                    message_store.mark_delivered(saved["message_id"], saved["to_key"])
                send(sock, {
                    "type": "dm_sent",
                    "message_id": saved["message_id"],
                    "conversation_id": saved["conversation_id"],
                    "to": saved["to"],
                    "text": saved["text"],
                    "ts": saved["ts"],
                    "status": status,
                })
                log.info("DM from=%s to=%s chars=%d status=%s",
                         username_key, saved["to_key"], len(text), status)
            elif mtype == "history":
                peer = msg.get("with")
                if not valid_username(peer):
                    send(sock, {"type": "error", "reason": "invalid conversation username"})
                    continue
                limit = msg.get("limit", 50)
                if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
                    send(sock, {"type": "error", "reason": "history limit must be 1-100"})
                    continue
                try:
                    result = message_store.history(username_key, peer.casefold(), limit)
                except UnknownUser:
                    send(sock, {"type": "error", "reason": "user not found"})
                    continue
                except (ConversationNotFound, ValueError) as exc:
                    send(sock, {"type": "error", "reason": str(exc) or "conversation not found"})
                    continue
                send(sock, {"type": "history_result", **result})
            elif mtype == "conversations":
                send(sock, {"type": "conversations_result",
                            "conversations": message_store.conversations(username_key)})
            elif mtype == "users":
                prune_expired_clients()
                with clients_lock:
                    users = sorted(clients)
                send(sock, {"type": "users", "users": users})
            else:
                send(sock, {"type": "error", "reason": "unknown type"})
    except ValueError as e:
        log.warning("PROTOCOL ERROR %s:%s user=%s: %s", *addr, username, e)
        send(sock, {"type": "error", "reason": str(e)})
    except socket.timeout:
        log.info("TIMEOUT %s:%s", *addr)
    except (ConnectionResetError, BrokenPipeError):
        log.info("ABRUPT DISCONNECT (RST) %s:%s user=%s", *addr, username)
    except Exception:
        log.exception("UNEXPECTED error for %s:%s", *addr)
    finally:
        if session_hash is not None:
            accounts.revoke_session(session_hash)
            untrack_client(username_key, sock)
            broadcast({"type": "system", "text": f"{username} left"}, exclude=username_key)
        try:
            sock.close()
        except OSError:
            pass
        log.info("DISCONNECT %s:%s user=%s", *addr, username)


# ---------------------------------------------------------------------------
# Browser support: a minimal HTTP + WebSocket (RFC 6455) layer, standard library only.
# A WebSocket is an ordinary TCP connection that starts with an HTTP "Upgrade"
# request and afterwards carries framed messages. WSConn makes such a connection
# look like a plain socket to handle_client(), so both kinds of client share the
# same room, validation, logging and cleanup code.
# ---------------------------------------------------------------------------
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#f1f5f2">
<title>TCP Chat</title>
<style>
*{box-sizing:border-box}
:root{font-family:Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;color:#1d3029;background:#edf3ef;font-synthesis:none}
body{margin:0;min-height:100vh}
button,input,textarea{font:inherit}
button{cursor:pointer}
.hidden{display:none!important}
.auth-screen{min-height:100vh;display:grid;place-items:center;padding:32px;background:radial-gradient(ellipse at 12% 10%,#dcebe1 0,transparent 36%),#f2f6f3}
.auth-card{width:min(920px,100%);min-height:570px;display:grid;grid-template-columns:1fr 1fr;overflow:hidden;background:#fff;border:1px solid #e1ebe4;border-radius:28px;box-shadow:0 24px 70px #183d2b18}
.auth-brand{position:relative;display:flex;flex-direction:column;justify-content:space-between;padding:42px;color:#f5fff8;background:linear-gradient(145deg,#174c37,#197451 62%,#39a16e);overflow:hidden}
.auth-brand:after{content:"";position:absolute;width:380px;height:380px;right:-175px;bottom:-180px;border:1px solid #ffffff24;border-radius:50%;box-shadow:0 0 0 32px #ffffff0d,0 0 0 72px #ffffff0a}
.brand-mark{display:flex;align-items:center;gap:12px;font-weight:750;letter-spacing:.02em}
.brand-icon{width:42px;height:42px;display:grid;place-items:center;border:1px solid #ffffff55;border-radius:15px;background:#ffffff1a;font-size:20px}
.brand-copy{position:relative;z-index:1;max-width:340px;margin:auto 0}
.brand-copy h1{margin:0 0 14px;font-size:clamp(34px,5vw,48px);line-height:1.08;letter-spacing:-.045em}
.brand-copy p,.brand-foot{color:#d7eee1;line-height:1.65}
.brand-foot{position:relative;z-index:1;margin:30px 0 0;font-size:12px}
.auth-panel{display:flex;flex-direction:column;justify-content:center;padding:52px}
.eyebrow{margin:0 0 8px;color:#438365;font-size:12px;font-weight:750;letter-spacing:.13em;text-transform:uppercase}
.auth-panel h2{margin:0;font-size:30px;letter-spacing:-.035em}
.subcopy{margin:10px 0 25px;color:#73837b;font-size:14px;line-height:1.55}
.tabs{display:grid;grid-template-columns:1fr 1fr;gap:5px;padding:5px;margin-bottom:22px;border-radius:12px;background:#f1f5f2}
.tab{border:0;border-radius:9px;padding:10px;color:#718078;background:transparent;font-weight:650}
.tab.active{color:#225e42;background:white;box-shadow:0 2px 8px #183d2b12}
.field{display:block;margin:0 0 16px}
.field span{display:block;margin:0 0 7px;color:#42564b;font-size:13px;font-weight:650}
.field input{width:100%;height:48px;padding:0 14px;border:1px solid #dce6df;border-radius:11px;outline:none;color:#20382c;background:#fff;transition:border-color .15s,box-shadow .15s}
.field input:focus{border-color:#3b986a;box-shadow:0 0 0 3px #3b986a1c}
.hint{margin:-7px 0 16px;color:#84928a;font-size:12px}
.primary{width:100%;height:48px;border:0;border-radius:11px;color:#fff;background:#218255;font-weight:700;box-shadow:0 7px 16px #21825528;transition:background .15s,transform .15s}
.primary:hover{background:#176c46;transform:translateY(-1px)}
.primary:disabled{cursor:wait;opacity:.65;transform:none}
.auth-status{min-height:24px;margin:13px 0 0;color:#a33e39;font-size:13px;line-height:1.5}
.auth-status.success{color:#28794f}
.app-shell{position:relative;width:min(1180px,calc(100vw - 48px));height:min(790px,calc(100vh - 48px));min-height:540px;display:grid;grid-template-columns:300px minmax(0,1fr);margin:24px auto;overflow:hidden;border:1px solid #dfe9e2;border-radius:22px;background:#fff;box-shadow:0 20px 60px #193c2a16}
.sidebar{display:flex;flex-direction:column;min-width:0;border-right:1px solid #e8eee9;background:#fff}
.side-head{height:76px;display:flex;align-items:center;justify-content:space-between;padding:0 20px;border-bottom:1px solid #edf1ee}
.side-brand{display:flex;align-items:center;gap:10px;font-size:16px;font-weight:750}
.mini-mark{width:34px;height:34px;display:grid;place-items:center;border-radius:12px;color:#fff;background:#238457;font-weight:800}
.icon-button{width:36px;height:36px;border:0;border-radius:10px;color:#668075;background:#f2f6f3}
.side-label{padding:21px 20px 9px;color:#7e8f85;font-size:11px;font-weight:750;letter-spacing:.12em;text-transform:uppercase}
.new-chat{display:flex;gap:7px;padding:0 12px 10px}
.new-chat input{width:100%;min-width:0;height:40px;padding:0 11px;border:1px solid #e0e9e2;border-radius:10px;outline:0;background:#f8faf8;font-size:12px}
.new-chat input:focus{border-color:#65a782;box-shadow:0 0 0 3px #3b986a12}
.new-chat button{width:40px;flex:0 0 auto;border:0;border-radius:10px;color:#fff;background:#218255;font-size:20px}
.avatar{flex:0 0 auto;width:42px;height:42px;display:grid;place-items:center;border-radius:50%;color:#fff;background:#44966c;font-weight:750}
.conversation-list{flex:1;overflow:auto;padding:3px 9px}
.conversation-item{width:100%;display:flex;align-items:center;gap:10px;padding:11px 9px;border:0;border-radius:12px;text-align:left;background:transparent}
.conversation-item:hover{background:#f5f8f5}
.conversation-item.selected{background:#edf6ef}
.conversation-avatar{width:40px;height:40px;color:#397455;background:#e4f0e8;font-size:13px}
.conversation-copy{flex:1;min-width:0}
.conversation-name{display:block;overflow:hidden;color:#344a3c;font-size:13px;font-weight:700;text-overflow:ellipsis;white-space:nowrap}
.conversation-preview{display:block;overflow:hidden;margin-top:4px;color:#829087;font-size:11px;text-overflow:ellipsis;white-space:nowrap}
.conversation-meta{display:flex;flex-direction:column;align-items:flex-end;gap:5px}
.conversation-time{color:#8b9990;font-size:10px}
.pending-badge{min-width:18px;padding:2px 5px;border-radius:10px;color:#fff;background:#278457;font-size:10px;text-align:center}
.empty-conversations{padding:14px 10px;color:#84928a;font-size:12px;line-height:1.55}
.profile{display:flex;align-items:center;gap:10px;padding:15px 16px;border-top:1px solid #edf1ee}
.profile .avatar{width:36px;height:36px;color:#28734e;background:#e3f1e8;font-size:13px}
.profile-name{flex:1;overflow:hidden;font-size:13px;font-weight:700;text-overflow:ellipsis;white-space:nowrap}
.logout{border:0;color:#6b7d72;background:transparent;font-size:12px}
.logout:hover{color:#b34c43}
.chat-panel{display:flex;flex-direction:column;min-width:0;background:#f8faf8}
.chat-head{height:76px;display:flex;align-items:center;gap:12px;padding:0 22px;border-bottom:1px solid #e8eee9;background:#fff}
.chat-head .avatar{width:42px;height:42px;color:#397455;background:#e4f0e8}
.chat-heading{min-width:0}
.chat-heading strong{display:block;font-size:15px}
.chat-heading span{display:block;margin-top:4px;color:#7d8c83;font-size:12px}
.connection{display:flex;align-items:center;gap:7px;margin-left:auto;color:#718078;font-size:12px}
.connection-dot{width:8px;height:8px;border-radius:50%;background:#42a66c}
.header-logout{display:none;width:auto;padding:0 11px;color:#52675b;font-size:12px}
.header-menu{display:none;width:auto;padding:0 10px;color:#52675b;font-size:12px}
.app-shell.show-sidebar::before{content:"";position:absolute;inset:0;z-index:2;background:#142b1c55}
.app-shell.show-sidebar .sidebar{position:absolute;inset:0 auto 0 0;z-index:3;width:min(320px,86vw);display:flex;box-shadow:12px 0 30px #102d1e26}
.messages{flex:1;overflow-y:auto;padding:26px clamp(16px,4vw,48px);background-color:#f5f8f5;background-image:radial-gradient(#dce8df 0.65px,transparent 0.65px);background-size:18px 18px}
.empty-chat{max-width:330px;margin:auto;padding:24px;color:#73837a;text-align:center;font-size:14px;line-height:1.7}
.empty-chat strong{display:block;margin-bottom:6px;color:#42574a;font-size:16px}
.message-row{display:flex;margin:8px 0}
.message-row.mine{justify-content:flex-end}
.bubble{max-width:min(76%,540px);padding:10px 13px 8px;border:1px solid #e7ece8;border-radius:5px 15px 15px 15px;background:#fff;box-shadow:0 2px 5px #1d302908;overflow-wrap:anywhere}
.mine .bubble{border-color:#d6ecdc;border-radius:15px 5px 15px 15px;background:#e5f4e8}
.sender{margin-bottom:4px;color:#31845b;font-size:12px;font-weight:750}
.message-text{color:#293a30;font-size:14px;line-height:1.5;white-space:pre-wrap}
.message-time{margin-top:4px;color:#8b9990;font-size:10px;text-align:right}
.system-message{width:fit-content;margin:11px auto;padding:6px 10px;border-radius:9px;color:#75847b;background:#ffffffd9;font-size:11px}
.composer-wrap{padding:14px 20px 18px;border-top:1px solid #e8eee9;background:#fff}
.composer{display:flex;align-items:flex-end;gap:10px;padding:8px 9px 8px 15px;border:1px solid #e0e9e2;border-radius:16px;background:#f8faf8}
.composer:focus-within{border-color:#65a782;box-shadow:0 0 0 3px #3b986a12}
.composer textarea{flex:1;max-height:120px;min-height:38px;resize:vertical;padding:9px 0;border:0;outline:0;color:#26382d;background:transparent;font-size:14px;line-height:1.45}
.composer textarea::placeholder{color:#9aa69e}
.send-button{width:40px;height:40px;flex:0 0 auto;border:0;border-radius:12px;color:#fff;background:#218255;font-size:17px}
.send-button:hover{background:#176c46}
.composer-help{margin:7px 4px 0;color:#9aa69e;font-size:10px}
.toast{position:fixed;right:22px;bottom:22px;z-index:5;max-width:min(380px,calc(100vw - 32px));padding:12px 16px;border:1px solid #f0d7d2;border-radius:12px;color:#8c3832;background:#fff;box-shadow:0 10px 32px #162c201c;font-size:13px}
@media(max-width:760px){
  .auth-screen{padding:16px}.auth-card{max-width:470px;min-height:0;grid-template-columns:1fr;border-radius:22px}
  .auth-brand{min-height:190px;padding:24px}.brand-copy{margin:25px 0 0}.brand-copy h1{font-size:30px}.brand-copy p{margin:8px 0 0;font-size:13px}.brand-foot{display:none}
  .auth-panel{padding:28px 24px 24px}.auth-panel h2{font-size:26px}.subcopy{margin-bottom:19px}
  .app-shell{width:100vw;height:100dvh;min-height:0;margin:0;border:0;border-radius:0;grid-template-columns:1fr}
  .sidebar{display:none}.chat-head{height:68px;padding:0 10px}.messages{padding:20px 13px}.bubble{max-width:88%}
  .composer-wrap{padding:10px 12px 12px}.connection{font-size:0}.connection-dot{width:9px;height:9px}.header-logout,.header-menu{display:block}
}
@media(prefers-reduced-motion:reduce){*,*::before,*::after{scroll-behavior:auto!important;transition:none!important}}
</style></head><body>
<section id="auth-screen" class="auth-screen">
  <div class="auth-card">
    <aside class="auth-brand">
      <div class="brand-mark"><span class="brand-icon" aria-hidden="true">T</span><span>TCP Chat</span></div>
      <div class="brand-copy"><h1>Good conversations start here.</h1><p>A simple, secure place to connect. Sign in to join the community chat.</p></div>
      <p class="brand-foot">One account works in the browser and the terminal client.</p>
    </aside>
    <main class="auth-panel">
      <p class="eyebrow">Welcome</p>
      <h2 id="auth-title">Sign in to your account</h2>
      <p class="subcopy">Enter your details below. New here? Create an account in a moment.</p>
      <div class="tabs" role="tablist" aria-label="Account action">
        <button id="login-tab" class="tab active" type="button" role="tab" aria-selected="true">Log in</button>
        <button id="register-tab" class="tab" type="button" role="tab" aria-selected="false">Create account</button>
      </div>
      <form id="auth-form">
        <label class="field"><span>Username</span><input id="username" name="username" autocomplete="username" minlength="3" maxlength="16" pattern="[A-Za-z0-9_]+" placeholder="e.g. alex_01" required></label>
        <label class="field"><span>Password</span><input id="password" name="password" type="password" autocomplete="current-password" minlength="10" maxlength="128" placeholder="At least 10 characters" required></label>
        <p id="password-hint" class="hint hidden">Use 10-128 characters. Your password is never shown on screen.</p>
        <button id="auth-submit" class="primary" type="submit">Log in</button>
        <p id="auth-status" class="auth-status" role="status" aria-live="polite"></p>
      </form>
    </main>
  </div>
</section>
<main id="app-screen" class="app-shell hidden">
  <aside class="sidebar">
    <header class="side-head"><div class="side-brand"><span class="mini-mark">T</span><span>TCP Chat</span></div><button id="refresh-conversations" class="icon-button" type="button" aria-label="Refresh conversations" title="Refresh conversations">↻</button></header>
    <div class="side-label">Conversations</div>
    <form id="new-chat-form" class="new-chat"><input id="new-peer" placeholder="Start chat by username" maxlength="16" aria-label="Username to start a chat"><button type="submit" aria-label="Start chat">+</button></form>
    <div id="conversation-list" class="conversation-list"><div class="empty-conversations">Your private conversations will appear here.</div></div>
    <div class="profile"><span id="profile-avatar" class="avatar">?</span><span id="profile-name" class="profile-name"></span><button id="logout-button" class="logout" type="button">Log out</button></div>
  </aside>
  <section class="chat-panel" aria-label="Private conversation">
    <header class="chat-head"><button id="mobile-conversations" class="icon-button header-menu" type="button" aria-label="Open conversations" aria-expanded="false">Chats</button><span id="peer-avatar" class="avatar">?</span><div class="chat-heading"><strong id="peer-name">Choose a conversation</strong><span id="peer-status">Start a private chat using a username</span></div><div class="connection"><span class="connection-dot"></span><span>Connected</span></div><button id="header-logout" class="icon-button header-logout" type="button">Log out</button></header>
    <div id="messages" class="messages" aria-live="polite"><div id="empty-chat" class="empty-chat"><strong>Your messages, one-to-one.</strong>Choose a conversation or enter a username on the left to start one.</div></div>
    <div class="composer-wrap">
      <form id="composer" class="composer"><textarea id="message-input" rows="1" maxlength="1000" placeholder="Select a conversation first" aria-label="Write a message" disabled></textarea><button class="send-button" type="submit" aria-label="Send message" title="Send message" disabled>➤</button></form>
      <div class="composer-help">Enter to send · Shift + Enter for a new line</div>
    </div>
  </section>
</main>
<div id="toast" class="toast hidden" role="alert"></div>
<script>
let ws = null, authMode = 'login', currentUser = '', activePeer = '';
let conversationItems = [];
let authPending = false, toastTimer = null;
const authScreen = document.getElementById('auth-screen'), appScreen = document.getElementById('app-screen');
const authForm = document.getElementById('auth-form'), usernameInput = document.getElementById('username');
const passwordInput = document.getElementById('password'), authStatus = document.getElementById('auth-status');
const authTitle = document.getElementById('auth-title'), authSubmit = document.getElementById('auth-submit');
const passwordHint = document.getElementById('password-hint'), loginTab = document.getElementById('login-tab');
const registerTab = document.getElementById('register-tab'), messages = document.getElementById('messages');
const messageInput = document.getElementById('message-input');
const conversationList = document.getElementById('conversation-list');
const newPeerInput = document.getElementById('new-peer');
const toast = document.getElementById('toast');

function showToast(text){
  toast.textContent = text;
  toast.classList.remove('hidden');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => toast.classList.add('hidden'), 3500);
}
function setAuthStatus(text, success){
  authStatus.textContent = text;
  authStatus.classList.toggle('success', Boolean(success));
}
function setAuthMode(mode){
  authMode = mode;
  const registering = mode === 'register';
  loginTab.classList.toggle('active', !registering);
  registerTab.classList.toggle('active', registering);
  loginTab.setAttribute('aria-selected', String(!registering));
  registerTab.setAttribute('aria-selected', String(registering));
  authTitle.textContent = registering ? 'Create your account' : 'Sign in to your account';
  authSubmit.textContent = registering ? 'Create account' : 'Log in';
  passwordInput.autocomplete = registering ? 'new-password' : 'current-password';
  passwordHint.classList.toggle('hidden', !registering);
  setAuthStatus('', false);
}
function addSystemMessage(text){
  const node = document.createElement('div');
  node.className = 'system-message';
  node.textContent = text;
  messages.appendChild(node);
  messages.scrollTop = messages.scrollHeight;
}
function showEmptyChat(title, description){
  messages.replaceChildren();
  const empty = document.createElement('div');
  empty.className = 'empty-chat';
  const heading = document.createElement('strong');
  heading.textContent = title;
  const copy = document.createTextNode(description);
  empty.append(heading, copy);
  messages.appendChild(empty);
}
function addChatMessage(message){
  const row = document.createElement('div');
  const mine = message.from.toLowerCase() === currentUser.toLowerCase();
  row.className = 'message-row' + (mine ? ' mine' : '');
  const bubble = document.createElement('div');
  bubble.className = 'bubble';
  if(!mine){
    const sender = document.createElement('div');
    sender.className = 'sender';
    sender.textContent = message.from;
    bubble.appendChild(sender);
  }
  const text = document.createElement('div');
  text.className = 'message-text';
  text.textContent = message.text;
  const time = document.createElement('div');
  time.className = 'message-time';
  const sentTime = new Date((message.ts || Date.now() / 1000) * 1000)
    .toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
  time.textContent = mine && message.status
    ? sentTime + ' · ' + (message.status === 'pending' ? 'Pending delivery' : 'Delivered')
    : sentTime;
  bubble.append(text, time);
  row.appendChild(bubble);
  messages.appendChild(row);
  messages.scrollTop = messages.scrollHeight;
}
function requestConversations(){
  if(ws && ws.readyState === WebSocket.OPEN)
    ws.send(JSON.stringify({type:'conversations'}));
}
function setActivePeer(peer){
  activePeer = peer || '';
  document.getElementById('peer-name').textContent = activePeer || 'Choose a conversation';
  document.getElementById('peer-avatar').textContent = activePeer
    ? activePeer.slice(0, 1).toUpperCase() : '?';
  document.getElementById('peer-status').textContent = activePeer
    ? 'One-to-one conversation' : 'Start a private chat using a username';
  messageInput.disabled = !activePeer;
  document.querySelector('#composer button[type="submit"]').disabled = !activePeer;
  messageInput.placeholder = activePeer ? 'Write a message...' : 'Select a conversation first';
}
function renderConversations(items){
  conversationList.replaceChildren();
  if(!items.length){
    const empty = document.createElement('div');
    empty.className = 'empty-conversations';
    empty.textContent = 'No conversations yet. Enter a username above to start one.';
    conversationList.appendChild(empty);
    return;
  }
  items.forEach(item => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'conversation-item' +
      (item.with.toLowerCase() === activePeer.toLowerCase() ? ' selected' : '');
    button.setAttribute('aria-label', 'Open conversation with ' + item.with);
    const avatar = document.createElement('span');
    avatar.className = 'avatar conversation-avatar';
    avatar.textContent = item.with.slice(0, 1).toUpperCase();
    const copy = document.createElement('span');
    copy.className = 'conversation-copy';
    const name = document.createElement('span');
    name.className = 'conversation-name';
    name.textContent = item.with;
    const preview = document.createElement('span');
    preview.className = 'conversation-preview';
    const prefix = item.last_sender && item.last_sender.toLowerCase() === currentUser.toLowerCase()
      ? 'You: ' : '';
    preview.textContent = prefix + (item.last_message || 'No messages yet');
    const meta = document.createElement('span');
    meta.className = 'conversation-meta';
    if(item.updated_at){
      const time = document.createElement('span');
      time.className = 'conversation-time';
      time.textContent = new Date(item.updated_at * 1000)
        .toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
      meta.appendChild(time);
    }
    if(item.pending_count){
      const badge = document.createElement('span');
      badge.className = 'pending-badge';
      badge.textContent = String(item.pending_count);
      meta.appendChild(badge);
    }
    copy.append(name, preview);
    button.append(avatar, copy, meta);
    button.addEventListener('click', () => openConversation(item.with));
    conversationList.appendChild(button);
  });
}
function openConversation(peer){
  appScreen.classList.remove('show-sidebar');
  document.getElementById('mobile-conversations').setAttribute('aria-expanded', 'false');
  activePeer = peer;
  setActivePeer(peer);
  renderConversations(conversationItems);
  messages.replaceChildren();
  addSystemMessage('Loading conversation...');
  ws.send(JSON.stringify({type:'history', with:peer, limit:50}));
}
function showChat(username){
  currentUser = username;
  document.getElementById('profile-name').textContent = username;
  document.getElementById('profile-avatar').textContent = username.slice(0, 1).toUpperCase();
  authScreen.classList.add('hidden');
  appScreen.classList.remove('hidden');
  appScreen.classList.remove('show-sidebar');
  setActivePeer('');
  showEmptyChat('Your messages, one-to-one.',
    'Choose a conversation or enter a username on the left to start one.');
  conversationItems = [];
  requestConversations();
}
function showLogin(message){
  currentUser = '';
  activePeer = '';
  setActivePeer('');
  appScreen.classList.add('hidden');
  appScreen.classList.remove('show-sidebar');
  authScreen.classList.remove('hidden');
  setAuthMode('login');
  if(message) setAuthStatus(message, false);
}
function handleMessage(message){
  if(message.type === 'registered'){
    authPending = false;
    authSubmit.disabled = false;
    setAuthMode('login');
    passwordInput.value = '';
    setAuthStatus('Account created. You can now log in.', true);
  }else if(message.type === 'login_ok'){
    authPending = false;
    authSubmit.disabled = false;
    passwordInput.value = '';
    showChat(message.username);
  }else if(message.type === 'logged_out'){
    showLogin('You have been logged out.');
  }else if(message.type === 'conversations_result'){
    conversationItems = message.conversations || [];
    renderConversations(conversationItems);
  }else if(message.type === 'history_result'){
    if(message.with.toLowerCase() !== activePeer.toLowerCase()) return;
    activePeer = message.with;
    setActivePeer(activePeer);
    renderConversations(conversationItems);
    messages.replaceChildren();
    if(!message.messages.length){
      addSystemMessage('No messages yet. Send the first message to start this conversation.');
      return;
    }
    message.messages.forEach(addChatMessage);
  }else if(message.type === 'dm'){
    const peer = message.from.toLowerCase() === currentUser.toLowerCase()
      ? message.to : message.from;
    if(peer.toLowerCase() === activePeer.toLowerCase()) addChatMessage(message);
    else showToast('New message from ' + message.from);
    requestConversations();
  }else if(message.type === 'dm_sent'){
    if(message.to.toLowerCase() === activePeer.toLowerCase())
      addChatMessage({...message, from:currentUser});
    requestConversations();
  }else if(message.type === 'error'){
    if(authPending){
      authPending = false;
      authSubmit.disabled = false;
      setAuthStatus(message.reason, false);
    }else if(message.reason.startsWith('session expired')){
      showLogin('Your session expired. Please log in again.');
    }else{
      if(message.reason === 'user not found' || message.reason === 'invalid conversation username'){
        setActivePeer('');
        showEmptyChat('Conversation not found.', 'Check the username and try again.');
      }
      showToast(message.reason);
    }
  }
}
function connectAndSend(request){
  if(ws && ws.readyState === WebSocket.OPEN){
    ws.send(JSON.stringify(request));
    return;
  }
  ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws');
  ws.onopen = () => ws.send(JSON.stringify(request));
  ws.onmessage = event => {
    try{ handleMessage(JSON.parse(event.data)); }
    catch(error){ showToast('Received an unreadable server response.'); }
  };
  ws.onerror = () => showToast('Could not connect. Check that the chat server is running.');
  ws.onclose = () => {
    authPending = false;
    authSubmit.disabled = false;
    if(currentUser) showLogin('Connection lost. Please log in again.');
    else showToast('Disconnected from the chat server.');
  };
}
authForm.addEventListener('submit', event => {
  event.preventDefault();
  if(authPending) return;
  const username = usernameInput.value.trim(), password = passwordInput.value;
  if(!username || !password) return;
  authPending = true;
  authSubmit.disabled = true;
  setAuthStatus(authMode === 'register' ? 'Creating your account...' : 'Signing you in...', true);
  connectAndSend({type:authMode, username, password});
  if(authMode === 'register') passwordInput.value = '';
});
loginTab.addEventListener('click', () => setAuthMode('login'));
registerTab.addEventListener('click', () => setAuthMode('register'));
document.getElementById('logout-button').addEventListener('click', () => {
  if(ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({type:'logout'}));
});
document.getElementById('header-logout').addEventListener('click', () => {
  if(ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({type:'logout'}));
});
document.getElementById('refresh-conversations').addEventListener('click', () => {
  requestConversations();
});
document.getElementById('mobile-conversations').addEventListener('click', event => {
  const isOpen = appScreen.classList.toggle('show-sidebar');
  event.currentTarget.setAttribute('aria-expanded', String(isOpen));
});
appScreen.addEventListener('click', event => {
  if(event.target === appScreen && appScreen.classList.contains('show-sidebar')){
    appScreen.classList.remove('show-sidebar');
    document.getElementById('mobile-conversations').setAttribute('aria-expanded', 'false');
  }
});
document.getElementById('new-chat-form').addEventListener('submit', event => {
  event.preventDefault();
  const peer = newPeerInput.value.trim();
  if(!/^[A-Za-z0-9_]{3,16}$/.test(peer)){
    showToast('Enter a username with 3-16 letters, numbers, or underscores.');
    return;
  }
  newPeerInput.value = '';
  openConversation(peer);
});
document.getElementById('composer').addEventListener('submit', event => {
  event.preventDefault();
  const text = messageInput.value.trim();
  if(!text || !activePeer || !ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify({type:'dm', to:activePeer, text}));
  messageInput.value = '';
  messageInput.focus();
});
messageInput.addEventListener('keydown', event => {
  if(event.key === 'Enter' && !event.shiftKey){
    event.preventDefault();
    document.getElementById('composer').requestSubmit();
  }
});
</script></main></body></html>"""


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("connection closed")
        buf += chunk
    return buf


def ws_frame(payload, opcode=0x1):
    """Encode one unmasked server-to-client WebSocket frame."""
    n = len(payload)
    if n < 126:
        head = struct.pack("!BB", 0x80 | opcode, n)
    elif n < 65536:
        head = struct.pack("!BBH", 0x80 | opcode, 126, n)
    else:
        head = struct.pack("!BBQ", 0x80 | opcode, 127, n)
    return head + payload


class WSConn:
    """Wraps an upgraded socket so handle_client() can treat it like a TCP socket.

    recv() returns one decoded WebSocket text message plus a newline, so the
    existing newline framing code works unchanged. sendall() turns the server's
    newline-terminated JSON line back into a WebSocket text frame.
    """

    def __init__(self, sock):
        self.sock = sock
        self.lock = threading.Lock()       # frames from different threads must not interleave

    def settimeout(self, t):
        self.sock.settimeout(t)

    def sendall(self, data):
        with self.lock:
            self.sock.sendall(ws_frame(data.rstrip(b"\n")))

    def _read_frame(self):
        b1, b2 = recv_exact(self.sock, 2)
        fin, opcode = b1 & 0x80, b1 & 0x0F
        length = b2 & 0x7F
        if length == 126:
            length = struct.unpack("!H", recv_exact(self.sock, 2))[0]
        elif length == 127:
            length = struct.unpack("!Q", recv_exact(self.sock, 8))[0]
        if length > MAX_LINE:
            raise ValueError("message too long")
        if not b2 & 0x80:
            if length:
                recv_exact(self.sock, length)
            raise ValueError("client frames must be masked")
        mask = recv_exact(self.sock, 4)
        data = bytearray(recv_exact(self.sock, length)) if length else bytearray()
        for i in range(len(data)):
            data[i] ^= mask[i % 4]
        return fin, opcode, bytes(data)

    def recv(self, n):
        message = b""
        while True:
            try:
                fin, opcode, data = self._read_frame()
            except ConnectionError as e:
                log.info("WS ended without a close frame (%s)", e)
                return b""                         # treated as end of stream
            if opcode == 0x8:                      # close
                code = struct.unpack("!H", data[:2])[0] if len(data) >= 2 else None
                log.info("WS close frame received from client (code=%s)", code)
                return b""
            if opcode == 0x9:                      # ping -> pong
                with self.lock:
                    self.sock.sendall(ws_frame(data, 0xA))
                continue
            if opcode == 0xA:                      # pong
                continue
            if opcode in (0x0, 0x1):               # continuation / text
                message += data
                if len(message) > MAX_LINE:
                    raise ValueError("message too long")
                if fin:
                    return message + b"\n"
                continue
            raise ValueError("unsupported frame type")

    def close(self):
        try:
            with self.lock:
                self.sock.sendall(ws_frame(b"", 0x8))
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


def http_reply(conn, code, body, ctype="text/plain; charset=utf-8"):
    reasons = {200: "OK", 400: "Bad Request", 403: "Forbidden", 404: "Not Found", 431: "Header Too Large"}
    data = body.encode("utf-8") if isinstance(body, str) else body
    head = (f"HTTP/1.1 {code} {reasons.get(code, 'Error')}\r\nContent-Type: {ctype}\r\n"
            f"Content-Length: {len(data)}\r\nConnection: close\r\n\r\n").encode()
    try:
        conn.sendall(head + data)
    except OSError:
        pass
    conn.close()


def handle_web(conn, addr):
    """Serve the chat page, a health check, or upgrade /ws to a WebSocket."""
    try:
        conn.settimeout(10)
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = conn.recv(2048)
            if not chunk:
                conn.close()
                return
            head += chunk
            if len(head) > 8192:
                http_reply(conn, 431, "request too large")
                return
        lines = head.split(b"\r\n\r\n", 1)[0].decode("latin-1").split("\r\n")
        method, path, _ = lines[0].split(" ", 2)
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        path = path.split("?", 1)[0]
        if path == "/ws" and headers.get("upgrade", "").lower() == "websocket":
            origin = headers.get("origin")
            if origin and origin.split("://", 1)[-1] != headers.get("host"):
                log.warning("WEBSOCKET rejected (origin %s) from %s:%s", origin, *addr)
                http_reply(conn, 403, "origin not allowed")
                return
            key = headers.get("sec-websocket-key")
            if not key or headers.get("sec-websocket-version") != "13":
                http_reply(conn, 400, "bad websocket handshake")
                return
            accept = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
            conn.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                          f"Connection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").encode())
            log.info("WEBSOCKET upgrade from %s:%s", *addr)
            handle_client(WSConn(conn), addr)
            return
        if method == "GET" and path in ("/", "/index.html"):
            http_reply(conn, 200, INDEX_HTML, "text/html; charset=utf-8")
        elif path == "/health":
            http_reply(conn, 200, "ok")
        else:
            http_reply(conn, 404, "not found")
    except Exception:
        log.exception("WEB error for %s:%s", *addr)
        try:
            conn.close()
        except OSError:
            pass


def serve_web():
    try:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((HOST, WEB_PORT))
        srv.listen(50)
    except OSError as e:
        log.error("Web interface disabled (port %s): %s", WEB_PORT, e)
        return
    log.info("Web interface (HTTP + WebSocket) listening on %s:%s", HOST, WEB_PORT)
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_web, args=(conn, addr), daemon=True).start()


def heartbeat():
    """Send a small {"type": "ping"} to every client at a fixed interval.

    Cloud proxies close connections that carry no data for a while (idle timeout).
    A steady trickle of traffic keeps them open. Clients simply ignore the ping.
    """
    while True:
        time.sleep(HEARTBEAT)
        broadcast({"type": "ping"})


def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(50)
    log.info("Listening on %s:%s", HOST, PORT)
    threading.Thread(target=heartbeat, daemon=True).start()
    if WEB_PORT:
        threading.Thread(target=serve_web, daemon=True).start()
    try:
        while True:
            sock, addr = srv.accept()
            threading.Thread(target=handle_client, args=(sock, addr), daemon=True).start()
    except KeyboardInterrupt:
        log.info("Shutting down")
    finally:
        srv.close()


if __name__ == "__main__":
    main()