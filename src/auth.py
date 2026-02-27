from __future__ import annotations

import hashlib
import hmac
import os
import unicodedata


PBKDF2_ALGO = "sha256"
PBKDF2_ITERATIONS = 180_000
SALT_SIZE = 16


def hash_password(password: str) -> str:
    salt = os.urandom(SALT_SIZE)
    digest = hashlib.pbkdf2_hmac(
        PBKDF2_ALGO, password.encode("utf-8"), salt, PBKDF2_ITERATIONS
    )
    return f"pbkdf2_{PBKDF2_ALGO}${PBKDF2_ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        method, iterations_str, salt_hex, digest_hex = encoded.split("$")
        algo = method.replace("pbkdf2_", "", 1)
        iterations = int(iterations_str)
    except ValueError:
        return False

    candidate = hashlib.pbkdf2_hmac(
        algo, password.encode("utf-8"), bytes.fromhex(salt_hex), iterations
    )
    return hmac.compare_digest(candidate.hex(), digest_hex)


def slugify_username(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    tokens = [token.lower() for token in ascii_name.replace("-", " ").split() if token]
    if not tokens:
        return "consultor"
    return ".".join(tokens)

