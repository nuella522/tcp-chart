# TCP Multi-User Chat - Stage B: Private Conversations

This project extends the existing threaded TCP/WebSocket chat with accounts,
private one-to-one conversations, message history, and delivery to offline
recipients. It uses only Python's standard library. Group conversations are a
later stage.

## Run locally

```text
python server.py
python client.py 127.0.0.1 5000
```

The browser UI is served at `http://localhost:8080/`. Both clients use the same
newline-delimited JSON request handler. The browser's WebSocket connection is
also TCP; it is only a different framing layer.

Environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `CHAT_HOST` | `0.0.0.0` | Bind address |
| `CHAT_PORT` | `5000` | Raw TCP listener |
| `WEB_PORT` | `8080` | Browser and WebSocket listener; set to `0` to disable |
| `CHAT_DB_PATH` | `chat.sqlite3` | SQLite database file |
| `SESSION_TTL_SECONDS` | `86400` | Session lifetime |

The terminal client starts with a numbered account menu. Choose "Create
account" or "Log in"; password input is hidden. Once signed in, `/dm <user>
<text>` sends a private message, `/history <user>` reads that conversation,
and `/chats` lists your conversations. Plain text remains the legacy shared
room; use `/dm` for private messages. `/logout`, `/quit`, and `/help` are also
available.

The browser opens on a register/login screen, then shows a responsive
conversation list. Enter a username to start a conversation. Both clients use
the same server protocol. A message sent while the recipient is offline is
stored and delivered when they next log in.

## Stage A protocol

Every TCP request is one JSON object followed by `\n`. Browser WebSocket text
frames carry the same JSON objects.

| Type | Direction | Fields | Example | Error cases |
| --- | --- | --- | --- | --- |
| `register` | Client -> server | `username`, `password` | `{"type":"register","username":"alice","password":"..."}` | Invalid username/password; username already exists |
| `registered` | Server -> client | `username` | `{"type":"registered","username":"alice"}` | None |
| `login` | Client -> server | `username`, `password` | `{"type":"login","username":"alice","password":"..."}` | Invalid credentials, malformed fields, rate limited |
| `login_ok` | Server -> client | `username`, `token`, `expires_at` | `{"type":"login_ok","username":"alice","token":"...","expires_at":1900000000}` | None |
| `logout` | Client -> server | None | `{"type":"logout"}` | Authentication required |
| `logged_out` | Server -> client | None | `{"type":"logged_out"}` | None |
| `msg` | Client/server | Client: `text`; server adds `from`, `ts` | `{"type":"msg","text":"hello"}` | Authentication required; invalid/oversized text |
| `users` | Client -> server; server responds | None; response has `users` | `{"type":"users"}` | Authentication required |
| `quit` | Client -> server | None | `{"type":"quit"}` | Closes connection |
| `ping` | Server -> client | None | `{"type":"ping"}` | Keepalive; clients may ignore |
| `error` | Server -> client | `reason` | `{"type":"error","reason":"authentication required"}` | Returned for invalid or unauthorized requests |

Usernames are 3–16 ASCII letters, digits, or underscores and are unique
case-insensitively. Passwords are 10–128 characters. The server derives a
32-byte password hash with scrypt (`N=16384`, `r=8`, `p=1`) and a random salt
per account. Failed logins are limited to five per username-and-source-address
pair within five minutes.

Login returns a cryptographically random session token with an expiry, but the
current protocol binds authentication to that TCP/WebSocket connection; later
requests do not send the token again. Only a hash of the token is stored in
SQLite, and disconnect/logout revokes the session. Identity on messages comes
from the server-side session, not a client-supplied `from` field.

## Security limitation

This stage does **not** provide TLS. Passwords, session tokens, and chat contents
cross the network unencrypted, so a person able to observe the connection can
read them. Production deployment must terminate TLS for both HTTPS/WSS and the
raw TCP protocol (for example, with a TLS-enabled proxy or TLS sockets). Hashing
passwords protects stored credentials; it does not encrypt network traffic.

## Tests

Run the account, TCP framing, and WebSocket integration tests together:

```text
python run_tests.py
```

The tests use temporary SQLite databases and local ports.
