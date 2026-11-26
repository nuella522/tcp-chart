"""SQLite-backed account and connection-session storage."""

import hashlib
import hmac
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager


SCRYPT_N = 1 << 14
SCRYPT_R = 8
SCRYPT_P = 1
SCRYPT_LENGTH = 32
MAX_LOGIN_FAILURES = 5
LOGIN_WINDOW_SECONDS = 300
_DUMMY_SALT = b"tcp-chat-invalid-user"
_DUMMY_HASH = hashlib.scrypt(
    b"not-a-real-password",
    salt=_DUMMY_SALT,
    n=SCRYPT_N,
    r=SCRYPT_R,
    p=SCRYPT_P,
    dklen=SCRYPT_LENGTH,
    maxmem=64 * 1024 * 1024,
)


class DuplicateUsername(Exception):
    pass


class InvalidCredentials(Exception):
    pass


class LoginRateLimited(Exception):
    pass


class AuthStore:
    def __init__(self, path):
        self.path = path
        self._login_lock = threading.Lock()
        with self._connect() as db:
            db.execute("PRAGMA journal_mode = WAL")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    username TEXT NOT NULL,
                    username_key TEXT NOT NULL UNIQUE,
                    password_salt BLOB NOT NULL,
                    password_hash BLOB NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    token_hash BLOB PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    expires_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS login_failures (
                    attempt_key TEXT PRIMARY KEY,
                    failures INTEGER NOT NULL,
                    window_started REAL NOT NULL
                );
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
    def _derive(password, salt):
        return hashlib.scrypt(
            password.encode("utf-8"),
            salt=salt,
            n=SCRYPT_N,
            r=SCRYPT_R,
            p=SCRYPT_P,
            dklen=SCRYPT_LENGTH,
            maxmem=64 * 1024 * 1024,
        )

    def register(self, username, username_key, password):
        salt = secrets.token_bytes(16)
        password_hash = self._derive(password, salt)
        try:
            with self._connect() as db:
                db.execute(
                    """INSERT INTO users
                       (username, username_key, password_salt, password_hash, created_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (username, username_key, salt, password_hash, time.time()),
                )
        except sqlite3.IntegrityError as exc:
            raise DuplicateUsername from exc

    def login(self, username_key, password, remote_key, ttl_seconds):
        attempt_key = hashlib.sha256(
            (username_key + "\0" + remote_key).encode("utf-8")
        ).hexdigest()
        now = time.time()
        outcome = None
        result = None
        with self._login_lock:
            with self._connect() as db:
                failure = db.execute(
                    "SELECT failures, window_started FROM login_failures WHERE attempt_key = ?",
                    (attempt_key,),
                ).fetchone()
                if failure and now - failure[1] < LOGIN_WINDOW_SECONDS:
                    if failure[0] >= MAX_LOGIN_FAILURES:
                        outcome = "limited"
                else:
                    db.execute("DELETE FROM login_failures WHERE attempt_key = ?", (attempt_key,))
                    failure = None

                if outcome is None:
                    user = db.execute(
                        "SELECT id, username, password_salt, password_hash "
                        "FROM users WHERE username_key = ?",
                        (username_key,),
                    ).fetchone()
                    if user:
                        expected = self._derive(password, user[2])
                        valid = hmac.compare_digest(expected, user[3])
                    else:
                        candidate = self._derive(password, _DUMMY_SALT)
                        hmac.compare_digest(candidate, _DUMMY_HASH)
                        valid = False

                    if not valid:
                        failures = failure[0] + 1 if failure else 1
                        window_started = failure[1] if failure else now
                        db.execute(
                            """INSERT OR REPLACE INTO login_failures
                               (attempt_key, failures, window_started) VALUES (?, ?, ?)""",
                            (attempt_key, failures, window_started),
                        )
                        outcome = "invalid"
                    else:
                        db.execute("DELETE FROM login_failures WHERE attempt_key = ?", (attempt_key,))
                        token = secrets.token_urlsafe(32)
                        token_hash = hashlib.sha256(token.encode("ascii")).digest()
                        expires_at = now + ttl_seconds
                        db.execute(
                            "INSERT INTO sessions (token_hash, user_id, expires_at) VALUES (?, ?, ?)",
                            (token_hash, user[0], expires_at),
                        )
                        result = user[1], username_key, token, token_hash, expires_at

        if outcome == "limited":
            raise LoginRateLimited
        if outcome == "invalid":
            raise InvalidCredentials
        return result

    def resolve_session(self, token_hash):
        now = time.time()
        with self._connect() as db:
            row = db.execute(
                """SELECT users.username, users.username_key, sessions.expires_at
                   FROM sessions JOIN users ON users.id = sessions.user_id
                   WHERE sessions.token_hash = ?""",
                (token_hash,),
            ).fetchone()
            if row is None:
                return None
            if row[2] <= now:
                db.execute("DELETE FROM sessions WHERE token_hash = ?", (token_hash,))
                return None
            return row[0], row[1]

    def revoke_session(self, token_hash):
        if token_hash is None:
            return
        with self._connect() as db:
            db.execute("DELETE FROM sessions WHERE token_hash = ?", (token_hash,))
