from __future__ import annotations

import hmac
from hashlib import sha256
from typing import TypedDict
from blacksheep import Request

from pys3server import parse_query, InvalidSignature, InvalidRequest


class AuthDict(TypedDict):
    Credential: bytes
    SignedHeaders: bytes
    Signature: bytes


class SignatureV4:
    __slots__ = ("key_id", "signature", "datestamp", "region", "signed_headers", "amzdate", "request",)

    _AUTH_QUERY_KEYS = {
        b"X-Amz-Algorithm", b"X-Amz-Credential", b"X-Amz-Date", b"X-Amz-SignedHeaders", b"X-Amz-Signature"
    }
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
            self.request.headers.get_first(b"x-amz-content-sha256") or b"UNSIGNED_PAYLOAD",
        ])
        to_sign = b"\n".join([
            b"AWS4-HMAC-SHA256",
            self.amzdate,
            b"/".join([self.datestamp, self.region, b"s3", b"aws4_request"]),
            sha256(canonical_request).hexdigest().encode("utf8"),
        ])

        access_key = access_key.encode("utf8")
        key = self._sign(b"AWS4"+access_key, self.datestamp)
        key = self._sign(key, self.region)
        key = self._sign(key, b"s3")
        key = self._sign(key, b"aws4_request")

        return self.signature == self._sign(key, to_sign)

    @staticmethod
    def _sign(key: bytes, msg: bytes) -> bytes:
        return hmac.new(key, msg, sha256).digest()

    @classmethod
    def parse(cls, request: Request) -> SignatureV4 | None:
        query: dict[bytes, bytes] = parse_query(request, decode=False)

        auth_header = request.headers.get_first(b"authorization")
        if auth_header is not None:
            auth = auth_header.replace(b",", b"").split(b" ")
            if auth[0] != b"AWS4-HMAC-SHA256":
                raise InvalidSignature()
            auth = auth[1:]
            auth_dict: AuthDict = {key.decode("utf8"): value for key, value in [kv.split(b"=", 1) for kv in auth]}

            if cls._AUTH_DICT_KEYS.intersection(auth_dict.keys()) != cls._AUTH_DICT_KEYS:
                return

            amzdate = request.headers.get_first(b"x-amz-date")
        else:
            if cls._AUTH_QUERY_KEYS.intersection(query.keys()) != cls._AUTH_QUERY_KEYS:
                return
            if query.get(b"X-Amz-Algorithm") != b"AWS4-HMAC-SHA256":
                raise InvalidSignature()

            auth_dict: AuthDict = {
                "Credential": query[b"X-Amz-Credential"],
                "SignedHeaders": query[b"X-Amz-SignedHeaders"],
                "Signature": query[b"X-Amz-Signature"],
            }

            amzdate = query[b"X-Amz-Date"]

        try:
            key_id, datestamp, region, *_ = auth_dict["Credential"].split(b"/")
        except ValueError:
            raise InvalidRequest()

        return SignatureV4(
            key_id, auth_dict["Signature"], datestamp, region, auth_dict["SignedHeaders"], amzdate, request
        )
