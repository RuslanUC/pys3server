import hmac
from base64 import urlsafe_b64decode, urlsafe_b64encode
from hashlib import sha256

from msgpack import packb, unpackb


def _b64encode(data: str | bytes | dict) -> str:
    if isinstance(data, dict):
        data = packb(data)
    elif isinstance(data, str):
        data = data.encode("utf8")

    return urlsafe_b64encode(data).decode("utf8").strip("=")


def _b64decode(data: str | bytes) -> bytes:
    if isinstance(data, str):
        data = data.encode("utf8")

    if len(data) % 4 != 0:
        data += b"=" * (-len(data) % 4)

    return urlsafe_b64decode(data)


class JWTMpNoTs:
    @staticmethod
    def decode(token: str, secret: bytes) -> dict | None:
        try:
            header, payload, signature = token.split(".")
            header_dict = unpackb(_b64decode(header))
            assert header_dict.get("alg") == "HS256"
            assert header_dict.get("typ") == "JWT"
            signature = _b64decode(signature)
        except (IndexError, AssertionError, ValueError):
            return

        sig = f"{header}.{payload}".encode("utf8")
        sig = hmac.new(secret, sig, sha256).digest()
        if sig == signature:
            return unpackb(_b64decode(payload))

    @staticmethod
    def encode(payload: dict, secret: bytes) -> str:
        header = {
            "alg": "HS256",
            "typ": "JWT",
        }
        header = _b64encode(header)
        payload = _b64encode(payload)

        signature = f"{header}.{payload}".encode("utf8")
        signature = hmac.new(secret, signature, sha256).digest()
        signature = _b64encode(signature)

        return f"{header}.{payload}.{signature}"
