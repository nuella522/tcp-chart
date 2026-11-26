"""SQLite persistence for direct conversations and messages."""

import sqlite3
import time
from contextlib import contextmanager


class UnknownUser(Exception):
    pass


class ConversationNotFound(Exception):
    pass


class MessageStore:
    def __init__(self, path):
        self.path = path
        with self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY,
                    kind TEXT NOT NULL CHECK (kind IN ('direct', 'group')),
                    direct_key TEXT UNIQUE,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS conversation_members (
                    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    role TEXT NOT NULL DEFAULT 'member',
                    joined_at REAL NOT NULL,
                    PRIMARY KEY (conversation_id, user_id)
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY,
                    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
                    sender_id INTEGER NOT NULL REFERENCES users(id),
                    body TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending', 'delivered'))
                );
                CREATE INDEX IF NOT EXISTS idx_messages_conversation_id
                    ON messages(conversation_id, id);
                CREATE INDEX IF NOT EXISTS idx_messages_pending_status
                    ON messages(status, conversation_id);
                """
            )

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.execute("PRAGMA foreign_keys = ON")
        db.execute("PRAGMA busy_timeout = 10000")
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _direct_key(first, second):
        return "direct:" + ":".join(sorted((first, second)))

    def send_direct(self, sender_key, recipient_key, text):
        now = time.time()
        with self._connect() as db:
            sender = db.execute(
                "SELECT id, username FROM users WHERE username_key = ?", (sender_key,)
            ).fetchone()
            recipient = db.execute(
                "SELECT id, username FROM users WHERE username_key = ?", (recipient_key,)
            ).fetchone()
            if sender is None or recipient is None:
                raise UnknownUser
            if sender_key == recipient_key:
                raise ValueError("cannot send a direct message to yourself")

            direct_key = self._direct_key(sender_key, recipient_key)
            db.execute(
                """INSERT OR IGNORE INTO conversations (kind, direct_key, created_at)
                   VALUES ('direct', ?, ?)""",
                (direct_key, now),
            )
            conversation_id = db.execute(
                "SELECT id FROM conversations WHERE direct_key = ?", (direct_key,)
            ).fetchone()[0]
            db.executemany(
                """INSERT OR IGNORE INTO conversation_members
                   (conversation_id, user_id, role, joined_at) VALUES (?, ?, 'member', ?)""",
                ((conversation_id, sender[0], now), (conversation_id, recipient[0], now)),
            )
            cursor = db.execute(
                """INSERT INTO messages
                   (conversation_id, sender_id, body, created_at, status)
                   VALUES (?, ?, ?, ?, 'pending')""",
                (conversation_id, sender[0], text, now),
            )
            return {
                "message_id": cursor.lastrowid,
                "conversation_id": conversation_id,
                "from": sender[1],
                "from_key": sender_key,
                "to": recipient[1],
                "to_key": recipient_key,
                "text": text,
                "ts": now,
            }

    def history(self, username_key, peer_key, limit=50):
        limit = max(1, min(int(limit), 100))
        with self._connect() as db:
            viewer = db.execute(
                "SELECT id FROM users WHERE username_key = ?", (username_key,)
            ).fetchone()
            peer = db.execute(
                "SELECT id, username FROM users WHERE username_key = ?", (peer_key,)
            ).fetchone()
            if viewer is None or peer is None:
                raise UnknownUser
            if username_key == peer_key:
                raise ValueError("cannot open a direct conversation with yourself")

            direct_key = self._direct_key(username_key, peer_key)
            conversation = db.execute(
                "SELECT id FROM conversations WHERE direct_key = ?", (direct_key,)
            ).fetchone()
            if conversation is None:
                return {"conversation_id": None, "with": peer[1], "messages": []}

            conversation_id = conversation[0]
            is_member = db.execute(
                """SELECT 1 FROM conversation_members
                   WHERE conversation_id = ? AND user_id = ?""",
                (conversation_id, viewer[0]),
            ).fetchone()
            if is_member is None:
                raise ConversationNotFound

            rows = db.execute(
                """SELECT messages.id, sender.username, recipient.username,
                          messages.body, messages.created_at, messages.status
                   FROM messages
                   JOIN users AS sender ON sender.id = messages.sender_id
                   JOIN conversation_members AS other_member
                     ON other_member.conversation_id = messages.conversation_id
                    AND other_member.user_id != messages.sender_id
                   JOIN users AS recipient ON recipient.id = other_member.user_id
                   WHERE messages.conversation_id = ?
                   ORDER BY messages.id DESC LIMIT ?""",
                (conversation_id, limit),
            ).fetchall()
            messages = [
                {"id": row[0], "from": row[1], "to": row[2], "text": row[3],
                 "ts": row[4], "status": row[5]}
                for row in reversed(rows)
            ]
            return {
                "conversation_id": conversation_id,
                "with": peer[1],
                "messages": messages,
            }

    def conversations(self, username_key):
        with self._connect() as db:
            rows = db.execute(
                """SELECT conversations.id, peer.username, last_message.body,
                          last_message.created_at, last_sender.username,
                          (SELECT COUNT(*) FROM messages AS pending
                           WHERE pending.conversation_id = conversations.id
                             AND pending.sender_id != me.id
                             AND pending.status = 'pending')
                   FROM users AS me
                   JOIN conversation_members AS my_member ON my_member.user_id = me.id
                   JOIN conversations ON conversations.id = my_member.conversation_id
                   JOIN conversation_members AS peer_member
                     ON peer_member.conversation_id = conversations.id
                    AND peer_member.user_id != me.id
                   JOIN users AS peer ON peer.id = peer_member.user_id
                   LEFT JOIN messages AS last_message
                     ON last_message.id = (
                         SELECT MAX(id) FROM messages
                         WHERE conversation_id = conversations.id
                     )
                   LEFT JOIN users AS last_sender ON last_sender.id = last_message.sender_id
                   WHERE me.username_key = ? AND conversations.kind = 'direct'
                   ORDER BY COALESCE(last_message.created_at, conversations.created_at) DESC,
                            conversations.id DESC""",
                (username_key,),
            ).fetchall()
            return [
                {
                    "conversation_id": row[0],
                    "with": row[1],
                    "last_message": row[2] or "",
                    "updated_at": row[3],
                    "last_sender": row[4],
                    "pending_count": row[5],
                }
                for row in rows
            ]

    def pending_for(self, username_key):
        with self._connect() as db:
            user = db.execute(
                "SELECT id FROM users WHERE username_key = ?", (username_key,)
            ).fetchone()
            if user is None:
                raise UnknownUser
            rows = db.execute(
                """SELECT messages.id, messages.conversation_id, sender.username,
                          recipient.username, messages.body, messages.created_at
                   FROM messages
                   JOIN conversation_members AS recipient_member
                     ON recipient_member.conversation_id = messages.conversation_id
                    AND recipient_member.user_id = ?
                   JOIN users AS recipient ON recipient.id = recipient_member.user_id
                   JOIN users AS sender ON sender.id = messages.sender_id
                   WHERE messages.status = 'pending' AND messages.sender_id != ?
                   ORDER BY messages.id""",
                (user[0], user[0]),
            ).fetchall()
            return [
                {"message_id": row[0], "conversation_id": row[1], "from": row[2],
                 "to": row[3], "text": row[4], "ts": row[5]}
                for row in rows
            ]

    def mark_delivered(self, message_id, username_key):
        with self._connect() as db:
            db.execute(
                """UPDATE messages SET status = 'delivered'
                   WHERE id = ? AND status = 'pending'
                     AND sender_id != (
                         SELECT users.id FROM users WHERE users.username_key = ?
                     )
                     AND conversation_id IN (
                         SELECT conversation_members.conversation_id
                         FROM conversation_members
                         JOIN users ON users.id = conversation_members.user_id
                         WHERE users.username_key = ?
                     )""",
                (message_id, username_key, username_key),
            )
