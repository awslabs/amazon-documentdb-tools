"""Local application-login store (encrypted SQLite).

Stores Prism UI login accounts in a local SQLite file with optional
transparent 256-bit AES encryption (via SQLCipher). Credentials are verified
with salted PBKDF2-HMAC-SHA256 — the plaintext password is never persisted.

This is *application* authentication only (who may open the Prism UI). It is
unrelated to AWS credentials or the DocumentDB database username/password.

Encryption:
    If `sqlcipher3-binary` is installed, the DB file is AES-256 encrypted at rest.
    The encryption key is sourced from (priority order):
        1. PRISM_DB_KEY environment variable
        2. .prism_db.key file (auto-generated on first run, chmod 600)
    If sqlcipher3 is not installed, falls back to plain sqlite3 with a warning.

Environment overrides:
    PRISM_AUTH_DB       Path to the SQLite file (default: .prism_auth.db)
    PRISM_DB_KEY        Hex encryption key (64 chars). If unset, auto-managed via key file.
    PRISM_SECRET_KEY    Flask session secret key (bypasses DB storage entirely).
"""
import os
import logging
import sqlite3 as _stdlib_sqlite3
import hashlib
import secrets
import threading
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ── Encryption layer ──────────────────────────────────────────────────────────

try:
    import sqlcipher3.dbapi2 as _sqlite3
    _ENCRYPTED = True
except ImportError:
    _sqlite3 = _stdlib_sqlite3
    _ENCRYPTED = False
    logger.warning(
        "sqlcipher3 not installed — auth DB will NOT be encrypted at rest. "
        "Install sqlcipher3 for AES-256 encryption: pip install sqlcipher3"
    )

# ── PBKDF2 parameters ────────────────────────────────────────────────────────

_ALGO = "sha256"
_ITERATIONS = 200_000
_SALT_BYTES = 16

# ── Paths ─────────────────────────────────────────────────────────────────────

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DB = os.path.join(_APP_DIR, ".prism_auth.db")
_LEGACY_DB = os.path.join(_APP_DIR, ".doculens_auth.db")
_KEY_FILE = os.path.join(_APP_DIR, ".prism_db.key")

_db_lock = threading.Lock()
_migrated = False

# Auto-migrate legacy filename on first access
if not os.path.exists(_DEFAULT_DB) and os.path.exists(_LEGACY_DB):
    os.rename(_LEGACY_DB, _DEFAULT_DB)


# ── Key management ────────────────────────────────────────────────────────────

import re as _re

_HEX_KEY_RE = _re.compile(r"^[0-9a-fA-F]{64}$")


def _get_db_key():
    """Return the DB encryption key (64-char hex string).

    Priority: PRISM_DB_KEY env var > .prism_db.key file > auto-generate.
    Validates the key is exactly 64 hex characters to prevent malformed PRAGMA.
    """
    env_key = os.environ.get("PRISM_DB_KEY")
    if env_key:
        if not _HEX_KEY_RE.match(env_key):
            raise ValueError(
                "PRISM_DB_KEY must be exactly 64 hex characters. "
                "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
            )
        return env_key

    if os.path.exists(_KEY_FILE):
        with open(_KEY_FILE) as f:
            key = f.read().strip()
        if not _HEX_KEY_RE.match(key):
            raise ValueError(f"Corrupt key file {_KEY_FILE} — must be 64 hex chars. Delete and restart.")
        return key

    # Generate and persist with restrictive permissions
    key = secrets.token_hex(32)
    try:
        fd = os.open(_KEY_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.write(fd, key.encode())
        os.close(fd)
    except FileExistsError:
        # Another process beat us — read theirs
        with open(_KEY_FILE) as f:
            return f.read().strip()
    except OSError:
        # Windows or permission issue — write normally (best effort)
        with open(_KEY_FILE, "w") as f:
            f.write(key)

    return key


# ── Connection ────────────────────────────────────────────────────────────────

def _db_path():
    return os.environ.get("PRISM_AUTH_DB", _DEFAULT_DB)


def _set_pragma_key(conn, hex_key):
    """Set SQLCipher encryption key. Key is pre-validated as 64 hex chars by _get_db_key()."""
    stmt = 'PRAGMA key = "x\'' + hex_key + '\'"'
    conn.execute(stmt)


def _connect():
    """Open a connection to the auth DB (encrypted if sqlcipher3 available)."""
    conn = _sqlite3.connect(_db_path())
    if _ENCRYPTED:
        _set_pragma_key(conn, _get_db_key())
    conn.row_factory = _sqlite3.Row
    return conn


# ── Migration (plaintext → encrypted) ────────────────────────────────────────

def _ensure_migrated():
    """One-time migration: if DB exists as plaintext and encryption is available,
    re-create it in encrypted format. Idempotent and atomic.
    Also handles the reverse: encrypted DB but sqlcipher3 not available — deletes
    the unreadable file so a fresh plaintext one can be created."""
    global _migrated
    if _migrated:
        return

    db = _db_path()
    if not os.path.exists(db):
        _migrated = True
        return  # Will be created fresh by init_db

    if not _ENCRYPTED:
        # sqlcipher3 not available — verify stdlib can open the file
        try:
            test = _stdlib_sqlite3.connect(db)
            test.execute("SELECT count(*) FROM sqlite_master")
            test.close()
        except Exception:
            # File is encrypted but we can't decrypt — remove and start fresh
            logger.warning(
                "Existing auth DB is encrypted but sqlcipher3 is not installed. "
                "Removing encrypted DB — create users again with 'python create_user.py add <user>'. "
                "Install sqlcipher3 to preserve encrypted databases."
            )
            os.remove(db)
        _migrated = True
        return

    # _ENCRYPTED is True — check if file needs plaintext→encrypted migration
    # Try opening with encryption key — if it works, already encrypted
    try:
        conn = _sqlite3.connect(db)
        _set_pragma_key(conn, _get_db_key())
        conn.execute("SELECT count(*) FROM sqlite_master")
        conn.close()
        _migrated = True
        return
    except Exception:
        pass  # Not encrypted — need to migrate

    # Read data from plaintext DB using stdlib sqlite3
    try:
        old = _stdlib_sqlite3.connect(db)
        old.row_factory = _stdlib_sqlite3.Row
        users = old.execute("SELECT * FROM users").fetchall()
        meta = old.execute("SELECT * FROM meta").fetchall()
        old.close()
    except Exception as e:
        logger.error("Cannot read existing auth DB for migration: %s", e)
        _migrated = True
        return

    # Write to new encrypted DB (temp file → atomic rename)
    tmp = db + ".enc_tmp"
    try:
        new = _sqlite3.connect(tmp)
        _set_pragma_key(new, _get_db_key())
        new.execute(
            "CREATE TABLE users (username TEXT PRIMARY KEY, password_hash TEXT NOT NULL, "
            "salt TEXT NOT NULL, iterations INTEGER NOT NULL, created_at TEXT NOT NULL)"
        )
        new.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        for u in users:
            new.execute("INSERT INTO users VALUES (?,?,?,?,?)", tuple(u))
        for m in meta:
            new.execute("INSERT INTO meta VALUES (?,?)", tuple(m))
        new.commit()
        new.close()
        os.replace(tmp, db)  # Atomic on POSIX
        logger.info("Auth DB migrated to encrypted format (AES-256)")
    except Exception as e:
        logger.error("Auth DB migration failed: %s", e)
        if os.path.exists(tmp):
            os.remove(tmp)

    _migrated = True


# ── Public API ────────────────────────────────────────────────────────────────

def init_db():
    """Create the schema if it does not exist. Safe to call repeatedly."""
    with _db_lock:
        _ensure_migrated()
    with _db_lock, _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                username      TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                salt          TEXT NOT NULL,
                iterations    INTEGER NOT NULL,
                created_at    TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
    # Restrict file permissions (defense-in-depth; best-effort on non-POSIX)
    try:
        os.chmod(_db_path(), 0o600)
    except OSError:
        pass


def _hash_password(password, salt_hex, iterations):
    dk = hashlib.pbkdf2_hmac(
        _ALGO, password.encode("utf-8"), bytes.fromhex(salt_hex), iterations
    )
    return dk.hex()


def create_user(username, password):
    """Create a new user. Raises ValueError if the username already exists."""
    username = (username or "").strip()
    if not username:
        raise ValueError("Username must not be empty.")
    if not password:
        raise ValueError("Password must not be empty.")
    if len(password) < 12:
        raise ValueError("Password must be at least 12 characters.")

    init_db()
    salt_hex = secrets.token_hex(_SALT_BYTES)
    pw_hash = _hash_password(password, salt_hex, _ITERATIONS)
    now = datetime.now(timezone.utc).isoformat()

    with _db_lock, _connect() as conn:
        exists = conn.execute(
            "SELECT 1 FROM users WHERE username = ?", (username,)
        ).fetchone()
        if exists:
            raise ValueError(f"User '{username}' already exists.")
        conn.execute(
            "INSERT INTO users (username, password_hash, salt, iterations, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (username, pw_hash, salt_hex, _ITERATIONS, now),
        )


def set_password(username, password):
    """Reset an existing user's password. Raises ValueError if not found."""
    username = (username or "").strip()
    if not password:
        raise ValueError("Password must not be empty.")
    init_db()
    salt_hex = secrets.token_hex(_SALT_BYTES)
    pw_hash = _hash_password(password, salt_hex, _ITERATIONS)
    with _db_lock, _connect() as conn:
        cur = conn.execute(
            "UPDATE users SET password_hash = ?, salt = ?, iterations = ? WHERE username = ?",
            (pw_hash, salt_hex, _ITERATIONS, username),
        )
        if cur.rowcount == 0:
            raise ValueError(f"User '{username}' does not exist.")


# ── Rate limiting (in-memory, per-username exponential backoff) ────────────────

_failed_attempts: dict = {}  # {username: {"count": int, "last_attempt": float}}
_RATE_LIMIT_BASE_SECONDS = 2     # First lockout: 2s, then 4s, 8s, 16s...
_RATE_LIMIT_MAX_SECONDS = 300    # Cap at 5 minutes
_RATE_LIMIT_THRESHOLD = 3        # Start locking out after 3 failures


def _check_rate_limit(username):
    """Return a lockout reason string if rate-limited, else None."""
    import time
    info = _failed_attempts.get(username)
    if not info or info["count"] < _RATE_LIMIT_THRESHOLD:
        return None
    # Exponential backoff: 2^(failures - threshold) * base, capped
    wait = min(
        _RATE_LIMIT_BASE_SECONDS * (2 ** (info["count"] - _RATE_LIMIT_THRESHOLD)),
        _RATE_LIMIT_MAX_SECONDS
    )
    elapsed = time.time() - info["last_attempt"]
    if elapsed < wait:
        return f"Too many failed attempts. Try again in {int(wait - elapsed)}s."
    return None


def _record_failed_attempt(username):
    """Record a failed login attempt for rate limiting."""
    import time
    info = _failed_attempts.get(username, {"count": 0, "last_attempt": 0})
    info["count"] += 1
    info["last_attempt"] = time.time()
    _failed_attempts[username] = info


def _clear_failed_attempts(username):
    """Clear failed attempt counter on successful login."""
    _failed_attempts.pop(username, None)


def verify_user(username, password):
    """Return True iff the username exists and the password matches.

    Uses a constant-time comparison to avoid leaking match info via timing.
    Enforces per-username rate limiting with exponential backoff.
    """
    username = (username or "").strip()
    if not username or not password:
        return False

    # Rate limiting: check if this username is locked out
    lockout_reason = _check_rate_limit(username)
    if lockout_reason:
        logger.warning("Login rate-limited for '%s': %s", username, lockout_reason)
        return False

    init_db()
    with _db_lock, _connect() as conn:
        row = conn.execute(
            "SELECT password_hash, salt, iterations FROM users WHERE username = ?",
            (username,),
        ).fetchone()
    if not row:
        _hash_password(password, secrets.token_hex(_SALT_BYTES), _ITERATIONS)
        _record_failed_attempt(username)
        return False
    candidate = _hash_password(password, row["salt"], row["iterations"])
    if secrets.compare_digest(candidate, row["password_hash"]):
        _clear_failed_attempts(username)
        return True
    _record_failed_attempt(username)
    return False


def user_count():
    """Number of registered users (0 means the app needs seeding)."""
    init_db()
    with _db_lock, _connect() as conn:
        return conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]


def list_users():
    """Return a list of (username, created_at) tuples."""
    init_db()
    with _db_lock, _connect() as conn:
        rows = conn.execute(
            "SELECT username, created_at FROM users ORDER BY username"
        ).fetchall()
    return [(r["username"], r["created_at"]) for r in rows]


def delete_user(username):
    """Delete a user. Raises ValueError if not found."""
    init_db()
    with _db_lock, _connect() as conn:
        cur = conn.execute("DELETE FROM users WHERE username = ?", ((username or "").strip(),))
        if cur.rowcount == 0:
            raise ValueError(f"User '{username}' does not exist.")


def get_or_create_secret_key():
    """Return a stable Flask secret key, generating and persisting one if absent.

    A stable key keeps login sessions valid across server restarts. An explicit
    PRISM_SECRET_KEY environment variable always takes precedence.

    Production warning: if binding to a non-localhost address without PRISM_SECRET_KEY,
    a security warning is logged (the DB-stored key is less secure than env/Secrets Manager).
    """
    env_key = os.environ.get("PRISM_SECRET_KEY")
    if env_key:
        return env_key

    # Warn if non-localhost binding without explicit secret key
    host = os.environ.get("PRISM_HOST", "127.0.0.1")
    if host not in ("127.0.0.1", "localhost", "::1"):
        logger.warning(
            "SECURITY: PRISM_SECRET_KEY not set while binding to %s. "
            "Session secret is stored in the auth DB. Set PRISM_SECRET_KEY env var "
            "or use AWS Secrets Manager for production deployments.", host
        )

    init_db()
    with _db_lock, _connect() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key = 'secret_key'").fetchone()
        if row:
            return row["value"]
        key = secrets.token_hex(32)
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('secret_key', ?)", (key,)
        )
        return key
