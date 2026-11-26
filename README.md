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
| `CHAT_HEARTBEAT_SECONDS` | `25` | Interval between keep-alive pings |
| `CLIENT_IDLE_TIMEOUT_SECONDS` | `300` | Disconnect clients that do not answer heartbeats |

The terminal client starts with a numbered account menu. Choose "Create
account" or "Log in"; password input is hidden. Once signed in, `/dm <user> <text>`
sends a private message, `/history <user>` reads that conversation, and
`/chats` lists your conversations. Plain text remains the legacy shared
room; use `/dm` for private messages. `/logout`, `/quit`, and `/help` are also
available.

The browser opens on a register/login screen, then shows a responsive
conversation list. Enter a username to start a conversation. Both clients use
the same server protocol. A message sent while the recipient is offline is
stored and delivered when they next log in.

## Protocol

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
| `dm` | Client -> server; server -> recipient | Client: `to`, `text`; server supplies `from`, IDs, time and status | `{"type":"dm","to":"bob","text":"hello"}` | Invalid/unknown recipient; invalid text |
| `dm_sent` | Server -> sender | `message_id`, `conversation_id`, `to`, `text`, `ts`, `status` | `{"type":"dm_sent","to":"bob","text":"hello","status":"pending"}` | Returned as `error` if rejected |
| `delivery_status` | Server -> sender | `message_id`, `status` | `{"type":"delivery_status","message_id":12,"status":"delivered"}` | Sent when a pending DM is delivered |
| `history` | Client -> server | `with`, optional `limit` (1-100) | `{"type":"history","with":"bob"}` | Invalid username/limit; conversation not found |
| `history_result` | Server -> client | `conversation_id`, `with`, `messages` | `{"type":"history_result","with":"bob","messages":[]}` | None |
| `conversations` | Client -> server | None | `{"type":"conversations"}` | Authentication required |
| `conversations_result` | Server -> client | `conversations` | `{"type":"conversations_result","conversations":[]}` | None |
| `users` | Client -> server; server responds | None; response has `users` | `{"type":"users"}` | Authentication required |
| `quit` | Client -> server | None | `{"type":"quit"}` | Closes connection |
| `ping` | Server -> client | None | `{"type":"ping"}` | Keepalive; clients should respond with `pong` |
| `pong` | Client -> server | None | `{"type":"pong"}` | Keepalive response |
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

Messages are inserted with `pending` status. The server marks a message
`delivered` after successfully sending it to the recipient, either while
online or at their next login. History returns the stored status. This is a
delivery status only; read receipts are not implemented.

## Security limitation

This stage does **not** provide TLS. Passwords, session tokens, and chat contents
cross the network unencrypted, so a person able to observe the connection can
read them. Production deployment must terminate TLS for both HTTPS/WSS and the
raw TCP protocol (for example, with a TLS-enabled proxy or TLS sockets). Hashing
passwords protects stored credentials; it does not encrypt network traffic.

## Tests

Run the account, private-message, TCP framing, and WebSocket integration tests together:

```text
python run_tests.py
```

The tests use temporary SQLite databases and local ports.

## Deploy to Railway

1. Put the updated project files in the GitHub repository connected to your
   Railway service. The Docker build needs `Dockerfile`, `server.py`,
   `auth_store.py`, and `message_store.py`.
2. In Railway, select the service, choose the Dockerfile builder if needed, and
   deploy the latest commit.
3. Configure `CHAT_HOST=0.0.0.0`, `CHAT_PORT=5000`, `WEB_PORT=8080`, and
   `CHAT_DB_PATH=/data/chat.sqlite3`. Confirm the Railway public web port targets
   the same port as `WEB_PORT`.
4. Attach a Railway volume mounted at `/data`. Without a persistent volume, the
   SQLite accounts and messages on the container filesystem can disappear on
   redeploy. The volume must be mounted before relying on stored chat data.
5. Open the public domain and `/health`; it should return `ok`. The browser
   client uses WebSocket over the same web port. Raw TCP terminal clients need a
   separately configured public TCP proxy for `CHAT_PORT`.

Railway's HTTPS domain protects browser traffic with TLS at the public edge.
The raw TCP listener in this project is still unencrypted; do not send
credentials over an exposed raw TCP connection until TLS is added.
