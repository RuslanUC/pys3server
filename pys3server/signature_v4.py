from __future__ import annotations

import hmac
from hashlib import sha256
from typing import TypedDict
from blacksheep import Request


class AuthDict(TypedDict):
    Credential: bytes
    SignedHeaders: bytes
    Signature: bytes


class SignatureV4:
    __slots__ = ("key_id", "signature", "datestamp", "region", "signed_headers", "amzdate", "request",)

    _AUTH_DICT_KEYS = {"Credential", "SignedHeaders", "Signature"}

    def __init__(
            self, key_id: bytes, signature: bytes, datestamp: bytes, region: bytes, signed_headers: bytes,
            amzdate: bytes, request: Request
    ):
        self.key_id = key_id
        self.signature = bytes.fromhex(signature.decode())
        self.datestamp = datestamp
        self.region = region
        self.signed_headers = signed_headers
        self.amzdate = amzdate
        self.request = request

    def verify(self, access_key: str) -> bool:
        canonical_request = b"\n".join([
            self.request.method.encode("utf8"),
            self.request.path.encode("utf8"),
            self.request._raw_query,
            b"\n".join([name+b":"+self.request.headers.get_first(name) for name in self.signed_headers.split(b";")]),
            b"",
            self.signed_headers,
            self.request.headers.get_first(b"x-amz-content-sha256"),
        ])
        to_sign = b"\n".join([
            b"AWS4-HMAC-SHA256",
            self.amzdate,
            b"/".join([self.datestamp, self.region, b"s3", b"aws4_request"]),
            sha256(canonical_request).hexdigest().encode("utf8"),
        ])

        access_key = access_key.encode("utf8")
        key = self._sign(b"AWS"+access_key, self.datestamp)
        key = self._sign(key, self.region)
        key = self._sign(key, b"s3")
        key = self._sign(key, b"aws4_request")

        return self.signature == self._sign(key, to_sign)

    @staticmethod
    def _sign(key: bytes, msg: bytes) -> bytes:
        return hmac.new(key, msg, sha256).digest()

    @classmethod
    def parse(cls, request: Request) -> SignatureV4 | None:
        auth_header = request.headers.get_first(b"authorization")
        if not auth_header:
            return
        auth = auth_header.replace(b",", b"").split(b" ")
        if auth[0] != b"AWS4-HMAC-SHA256":
            return
        auth = auth[1:]
        auth_dict: AuthDict = {key.decode("utf8"): value for key, value in [kv.split(b"=", 1) for kv in auth]}
        if cls._AUTH_DICT_KEYS.intersection(auth_dict.keys()) != cls._AUTH_DICT_KEYS:
            return

        try:
            key_id, datestamp, region, *_ = auth_dict["Credential"].split(b"/")
        except ValueError:
            return

        signed_headers = auth_dict["SignedHeaders"]
        amzdate = request.headers.get_first(b"x-amz-date")

        return SignatureV4(key_id, auth_dict["Signature"], datestamp, region, signed_headers, amzdate, request)
